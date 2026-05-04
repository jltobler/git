#ifndef S3_H
#define S3_H

#include "git-compat-util.h"

struct strbuf;
struct repository;

/*
 * The manifest tracks the list of currently active packs and reftables.
 * Manifests are stored in S3 under "<prefix>/manifests/<sha256>" and are
 * content-addressable.
 */
struct s3_manifest {
};

#define S3_MANIFEST_INIT { \
}

/* Release memory associated with the given manifest. */
void s3_manifest_release(struct s3_manifest *manifest);

/* Copy the given manifest. */
void s3_manifest_copy(const struct s3_manifest *from,
		      struct s3_manifest *to);

struct s3_storage {
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
	 * Local directory used as a packfile cache. Downloaded packfiles land
	 * in <cache_dir>/packs/.
	 */
	char *cache_dir;

	/* Authentication. */
	char *access_key_id;
	char *secret_access_key;

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
 * Note that the manifest is cached -- once it's been resolved, it's not going
 * to get resolved again.
 */
const struct s3_manifest *s3_storage_get_manifest(struct s3_storage *storage);

/* Update the manifest stored in object storage. */
int s3_storage_update_manifest(struct s3_storage *storage,
			       const struct s3_manifest *manifest);

int s3_get_to_file(struct s3_storage *storage, const char *key,
		   const char *local_path);
int s3_put_from_file(struct s3_storage *storage, const char *key,
		     const char *local_path);
int s3_put_from_buffer(struct s3_storage *storage, const char *key,
		       const unsigned char *data, size_t data_len);

/*
 * Given a file suffix relative to a repository's storage root, compute the
 * full S3 path where the file should be stored.
 */
const char *s3_key(const struct s3_storage *storage, struct strbuf *buf,
		   const char *suffix);

#endif
