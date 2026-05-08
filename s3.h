#ifndef S3_H
#define S3_H

#include "git-compat-util.h"
#include "hash.h"
#include "string-list.h"
#include "thread-utils.h"

struct strbuf;
struct repository;

/*
 * The entity tag for a given S3 object. This entity tag is typically a
 * checksum of the stored remote object and is opaque to us. It can be used for
 * compare-and-swap operations to ensure that we only overwrite an object in
 * case it matches the ETag.
 */
struct s3_etag {
	char *tag;
};

#define S3_ETAG_INIT { 0 }

/*
 * The manifest tracks the list of currently active packs and reftables.
 * Manifests are stored in S3 under "<prefix>/manifests/<sha256>" and are
 * content-addressable.
 */
struct s3_manifest {
	struct s3_etag etag;
	struct string_list packs;
	struct string_list reftables;
};

#define S3_MANIFEST_INIT { \
	.etag = S3_ETAG_INIT, \
	.packs = STRING_LIST_INIT_DUP, \
	.reftables = STRING_LIST_INIT_DUP, \
}

/* Release memory associated with the given manifest. */
void s3_manifest_release(struct s3_manifest *manifest);

/* Copy the given manifest. */
void s3_manifest_copy(const struct s3_manifest *from,
		      struct s3_manifest *to);

/*
 * A growable list of (key, local_path) pairs feeding the array-based
 * upload/download primitives. Both strings are owned by the list and
 * freed by s3_transfers_release(); callers retain ownership of the
 * inputs they pass to s3_transfers_append().
 *
 * The two arrays grow in lockstep, so a single items_alloc covers both.
 */
struct s3_transfers {
	struct s3_storage *storage;
	char **keys;
	char **local_paths;
	size_t items_nr, items_alloc;
};

#define S3_TRANSFERS_INIT(s) { \
	.storage = (s), \
}

void s3_transfers_release(struct s3_transfers *transfers);
void s3_transfers_append(struct s3_transfers *transfers,
			 const char *local_path);

struct s3_storage {
	struct repository *repo;

	unsigned refcount;

	/* The full URL of the S3 repository. */
	char *url;

	/*
	 * Base endpoint URL (scheme + authority), taken directly from the
	 * s3:// payload.  Always set.  Path-style addressing is used for all
	 * requests, so this field is valid for both AWS
	 * ("https://s3.us-east-1.amazonaws.com") and S3-compatible services
	 * ("http://localhost:9000").
	 */
	char *endpoint_url;

	/*
	 * HTTP authority extracted from `endpoint_url` (i.e. the scheme
	 * stripped). Used as the value of the Host: header and for SigV4
	 * signing. Computed once at construction.
	 */
	char *host;

	/* Region, e.g. "us-east-1". */
	char *region;

	/* S3 bucket name. */
	char *bucket;

	/*
	 * S3 key prefix without a trailing slash. When the prefix is an empty
	 * string the manifest lives at "manifest" and packs at "pack/...".
	 */
	char *prefix;

	/*
	 * Local directory used as the on-disk mirror of artefacts pulled
	 * from S3 plus a few small persistent caches. Layout:
	 *
	 *   <cache_dir>/packs/                downloaded packs / indexes /
	 *                                     reverse indexes
	 *   <cache_dir>/manifests/            downloaded manifest versions
	 *   <cache_dir>/reftables/            downloaded reftables
	 *                                     (one per active table)
	 *   <cache_dir>/altsvc.txt            libcurl Alt-Svc cache
	 */
	char *cache_dir;

	/* Authentication. */
	char *access_key_id;
	char *secret_access_key;

	/*
	 * Shared CURL connection / DNS / TLS-session cache used by every
	 * easy handle created against this storage. Reusing the underlying
	 * TCP+TLS connection across requests removes the per-request DNS,
	 * TCP and TLS handshake cost which otherwise dominates the latency
	 * of small requests like the manifest pointer fetch.
	 *
	 * Typed as void* here to keep curl out of this header; it is a
	 * CURLSH* internally. Created lazily on the first request and
	 * destroyed in s3_storage_release(). The mutex protects the share
	 * itself via the CURLSHOPT_LOCKFUNC / CURLSHOPT_UNLOCKFUNC
	 * callbacks; callers do not need to lock directly.
	 */
	void *curl_share;
	pthread_mutex_t curl_share_mutex;

	/* The currently resolved manifest. */
	struct s3_manifest manifest;
	bool manifest_initialized;
};

/*
 * Initialize and return the storage for a given repository and URL. If such a
 * storage has already been created, this will return the same storage. This
 * essentially makes the storage a shared resource for all backends that want
 * to store data in the same bucket and prefix.
 *
 * The URL is expected to be of the form "<scheme>://<host>/<bucket>/<prefix>".
 *
 * Cached data will be stored in the repository's common directory.
 *
 * Dies when initalizing the storage fails.
 */
struct s3_storage *s3_storage_get(struct repository *repo, const char *url);
void s3_storage_release(struct s3_storage *storage);

/*
 * Get the current manifest from object storage. The manifest does not need to
 * be released, as it is owned by the storage itself.
 *
 * Note that the manifest is cached for the lifetime of the storage object:
 * once it has been resolved (or written to via s3_transfers_publish()), this
 * function returns the cached value and does not re-fetch from S3. Callers
 * that need a fresh view across processes must release and re-acquire the
 * storage.
 */
const struct s3_manifest *s3_storage_get_manifest(struct s3_storage *storage);

/* Write the manifest to the storage's cache, without writing it to S3. */
int s3_storage_write_manifest(struct s3_storage *storage,
			      const struct s3_manifest *manifest,
			      struct strbuf *manifest_path);

/*
 * Download the given S3 keys into local files in parallel via
 * curl_multi. Each file is written to a tempfile and atomically renamed
 * into place when the request completes successfully; failed transfers
 * leave their destinations untouched.
 *
 * Returns 0 if every transfer succeeded, -1 if any failed.
 *
 * Safe to call with nr == 0 (no-op).
 */
int s3_storage_download_files(struct s3_storage *storage,
			      const struct s3_transfers *transfers);

/*
 * Prepare the manifest, upload all files, and then advance the manifest
 * pointer.
 */
int s3_transfers_publish(struct s3_storage *storage,
			 const struct s3_manifest *manifest,
			 struct s3_transfers *transfer);

/*
 * Compute the SHA-256 hex digest of the file at `path`, storing the
 * NUL-terminated hex result into `out`. Returns 0 on success, -1 on error
 * (errors are reported to stderr by the function itself).
 */
int s3_sha256_file_hex(const char *path, char out[GIT_SHA256_HEXSZ + 1]);

#endif
