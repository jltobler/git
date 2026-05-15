#ifndef MVCC_H
#define MVCC_H

#include "git-compat-util.h"
#include "hash.h"
#include "string-list.h"

struct strbuf;

/*
 * The manifest tracks the list of currently active packs and reftables.
 * Manifests are content-addressable: each manifest body is named by the
 * hex SHA-256 of its contents and stored under "manifests/<sha256>".
 *
 * The mutable "manifest" pointer file holds the SHA-256 of the current
 * manifest body and is rewritten atomically whenever a new manifest is
 * activated.
 */
struct mvcc_manifest {
	struct string_list packs;
	struct string_list reftables;
};

#define MVCC_MANIFEST_INIT { \
	.packs = STRING_LIST_INIT_DUP, \
	.reftables = STRING_LIST_INIT_DUP, \
}

/* Release memory associated with the given manifest. */
void mvcc_manifest_release(struct mvcc_manifest *manifest);

/* Copy the given manifest. */
void mvcc_manifest_copy(const struct mvcc_manifest *from,
			struct mvcc_manifest *to);

struct mvcc_storage {
	unsigned refcount;

	/*
	 * Whether the manifest pointer is pinned by GIT_MVCC_MANIFEST. A
	 * pinned storage is read-only: any attempt to write a manifest body
	 * or activate a new manifest dies. The pin is captured once at
	 * storage construction time.
	 */
	bool pinned;

	/*
	 * Local directory used as the on-disk store of artifacts that make
	 * up the repository. Layout:
	 *
	 *   <cache_dir>/manifest               mutable pointer file holding
	 *                                      the SHA-256 of the current
	 *                                      manifest body
	 *   <cache_dir>/manifests/<sha256>     immutable manifest bodies
	 *   <cache_dir>/pack/                  packs (.pack/.idx/.rev)
	 *   <cache_dir>/reftables/<sha256>.ref reftables (one per active table)
	 *
	 * All artifacts (packs, reftables, manifest bodies) are written
	 * straight into this directory. They are content-addressed, so
	 * concurrent writers producing the same artifact cannot collide,
	 * and unreferenced files left behind by aborted writes are
	 * reclaimed by the orchestrator's garbage collection rather than
	 * by Git itself.
	 */
	char *cache_dir;

	/*
	 * Optional path to an external manifest pointer file. When set
	 * (typically via the GIT_MVCC_MANIFEST_PATH environment variable),
	 * this file -- rather than <cache_dir>/manifest -- is read and
	 * written by manifest resolution and activation.
	 *
	 * The orchestrator is responsible for creating this file before
	 * invoking Git (seeded with the hex SHA-256 of the base manifest)
	 * and for inspecting it after the RPC to discover the new manifest
	 * pointer that resulted from the write.
	 *
	 * NULL when no external manifest pointer has been requested; in
	 * that case <cache_dir>/manifest is used directly.
	 */
	char *manifest_path;

	/* The currently resolved manifest. */
	struct mvcc_manifest manifest;
	bool manifest_initialized;

	/*
	 * The hex SHA-256 of the manifest body that this storage handle
	 * believes is active. Used as the expected-old value for the
	 * compare-and-swap performed inside mvcc_storage_activate_manifest:
	 * if the on-disk pointer no longer matches this value at activation
	 * time, another writer has advanced the pointer underneath us and
	 * we must reject the activation rather than silently clobber.
	 *
	 * `current_version_set` is false when no manifest has been resolved
	 * yet (e.g. a freshly-initialised repo that has not gone through
	 * `create_on_disk`). In that case the CAS check is skipped because
	 * there is no prior value to compare against.
	 */
	char current_version[GIT_SHA256_HEXSZ + 1];
	bool current_version_set;
};

/*
 * Initialize and return the storage for the repository whose common
 * directory is `commondir`. If a storage has already been created for that
 * commondir, this returns the same storage instance (refcount bumped). This
 * makes the storage a shared resource for backends that want to use the
 * same underlying cache: any number of MVCC extensions configured against
 * the same repository will resolve to a single handle and therefore a
 * single in-memory manifest view.
 *
 * The cache directory is always `<commondir>/mvcc-cache`. The location of
 * the external manifest pointer file, if any, is taken from the
 * GIT_MVCC_MANIFEST_PATH environment variable; when set, the file must
 * already exist and must be a non-empty regular file naming the base
 * manifest to chain on top of. Both invariants are the caller's
 * responsibility (typically Gitaly's) to establish before invoking Git.
 *
 * When the GIT_MVCC_MANIFEST environment variable is set, the resulting
 * storage handle is marked read-only: any subsequent attempt to write a
 * manifest body or activate a new manifest dies.
 *
 * Dies when initializing the storage fails.
 */
struct mvcc_storage *mvcc_storage_get(const char *commondir);
void mvcc_storage_release(struct mvcc_storage *storage);

/*
 * Resolve the path of an artifact in the storage. `kind` is one of
 * "pack", "reftables", or "manifests"; `name` is the file's basename
 * within that subdirectory. The result is appended to `out`.
 *
 * Artifacts are always located under the cache directory: they are
 * content-addressed, so there is no need to keep newly-written files
 * apart from the existing set. Callers that require the file to exist
 * should `access(2)` the returned path.
 */
void mvcc_storage_resolve_path(struct mvcc_storage *storage,
			       const char *kind, const char *name,
			       struct strbuf *out);

/*
 * Compute the path at which a new artifact should be written. As with
 * mvcc_storage_resolve_path(), this always resolves to a location under
 * the cache directory. The result is appended to `out`. When `name` is
 * NULL, only the subdirectory path is returned.
 */
void mvcc_storage_write_path(struct mvcc_storage *storage,
			     const char *kind, const char *name,
			     struct strbuf *out);

/*
 * Get the current manifest from the storage. The manifest does not need to
 * be released, as it is owned by the storage itself.
 *
 * Note that the manifest is cached for the lifetime of the storage object:
 * once it has been resolved (or written to via mvcc_storage_activate_manifest()),
 * this function returns the cached value. Callers that need a fresh view
 * across processes must release and re-acquire the storage.
 */
const struct mvcc_manifest *mvcc_storage_get_manifest(struct mvcc_storage *storage);

/*
 * Write the manifest body to its content-addressed location under
 * "<cache_dir>/manifests/<sha256>".
 *
 * The resulting hash of the manifest is written into `version_hex_out` (if not
 * NULL), which must have room for at least GIT_SHA256_HEXSZ + 1 bytes.
 *
 * This does NOT update the manifest pointer; use
 * mvcc_storage_activate_manifest() to make the manifest current.
 */
int mvcc_storage_write_manifest(struct mvcc_storage *storage,
				const struct mvcc_manifest *manifest,
				char *version_hex_out);

/*
 * Write the manifest body and atomically activate it by rewriting the
 * manifest pointer file (either <cache_dir>/manifest or the external
 * pointer file named by GIT_MVCC_MANIFEST_PATH, whichever is in effect).
 * The pointer is updated using Git's lockfile machinery, so concurrent
 * activators on the same filesystem are mutually excluded.
 *
 * On success, the storage's cached manifest is updated so that subsequent
 * reads in this process see the new state.
 */
int mvcc_storage_activate_manifest(struct mvcc_storage *storage,
				   const struct mvcc_manifest *manifest);

/*
 * Compute the SHA-256 hex digest of the file at `path`, storing the
 * NUL-terminated hex result into `out`. Returns 0 on success, -1 on error
 * (errors are reported to stderr by the function itself).
 */
int mvcc_sha256_file_hex(const char *path, char *hex_out);

#endif
