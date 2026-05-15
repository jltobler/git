#ifndef ODB_SOURCE_MVCC_H
#define ODB_SOURCE_MVCC_H

#include "object.h"
#include "odb/source.h"

struct odb_source_packed;
struct mvcc_storage;

/*
 * MVCC-backed object database source
 *
 * Packfiles live under a local content-addressed cache directory whose
 * layout is owned by `struct mvcc_storage`. A manifest file enumerates
 * the active set of packs; the manifest pointer file holds the SHA-256
 * of the current manifest body.
 *
 * In typical deployments an external orchestrator (e.g. Gitaly) is
 * responsible for prefetching the packs referenced by the active
 * manifest into the cache directory before invoking Git. When writes
 * are expected, the orchestrator additionally provides an external
 * manifest pointer file via GIT_MVCC_MANIFEST_PATH; Git deposits the
 * resulting packs into the cache directory and advances the external
 * pointer to the new manifest. The orchestrator inspects the new
 * pointer afterwards and uploads the newly referenced files to
 * durable storage.
 *
 * Read path
 * ---------
 *   prepare() resolves the manifest and verifies that every referenced
 *   pack is available locally in the cache. Missing files are a hard
 *   error: there is no automatic download. Verified packs are
 *   registered with an embedded odb_source_packed, to which all
 *   subsequent object lookups are delegated.
 *
 * Write path
 * ----------
 *   write_object() and write_object_stream() write a new packfile to
 *   the cache directory, then build a new manifest that lists the new
 *   pack alongside the previously active ones, and atomically activate
 *   the new manifest by rewriting the active manifest pointer file
 *   (either <cache_dir>/manifest or the external file named by
 *   GIT_MVCC_MANIFEST_PATH, whichever is in effect).
 *
 * MVCC / pinning
 * --------------
 *
 * Setting GIT_MVCC_MANIFEST to a manifest version hash bypasses the
 * pointer file entirely. Any process can therefore obtain a consistent,
 * point-in-time view of the object store simply by noting the hash
 * printed after a flush and exporting it into the environment of
 * readers.
 */
struct odb_source_mvcc {
	struct odb_source base;

	struct mvcc_storage *storage;

	/*
	 * Whether the manifest has already been resolved and local packs
	 * registered with ->packed. Cleared by reprepare() to force a
	 * refresh on the next access.
	 */
	bool initialized;

	/*
	 * Embedded packfile store used for all object lookups after the
	 * relevant packfiles have been registered. We set initialized=true
	 * immediately after construction to suppress its automatic directory
	 * scan and instead manage its pack list ourselves.
	 */
	struct odb_source_packed *packed;
};

/*
 * Allocate and initialize a new MVCC-backed ODB source.
 *
 * payload - the path portion of the mvcc:// URI after the schema is
 *           stripped. Unused: the cache directory is always derived from
 *           the repository's common directory ("<commondir>/mvcc-cache").
 *           Accepted only because the ODB source factory dispatches by
 *           URL schema and passes the payload uniformly to every backend.
 * local   - whether this is the primary (local) object source
 */
struct odb_source_mvcc *odb_source_mvcc_new(struct object_database *odb,
					    const char *payload,
					    bool local);

/*
 * Downcast to odb_source_mvcc; dies with BUG() on type mismatch.
 */
static inline struct odb_source_mvcc *
odb_source_mvcc_downcast(struct odb_source *source)
{
	if (source->type != ODB_SOURCE_MVCC)
		BUG("trying to downcast source of type '%d' to MVCC", source->type);
	return (struct odb_source_mvcc *)source;
}

#endif /* ODB_SOURCE_MVCC_H */
