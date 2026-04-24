#ifndef ODB_SOURCE_S3_H
#define ODB_SOURCE_S3_H

#include "object.h"
#include "odb/source.h"
#include "odb/source-packed.h"

/*
 * S3-backed object database source
 *
 * Packfiles are stored in an S3 bucket. Manifest files list the active
 * packfiles. A local cache directory mirrors downloaded packfiles so that
 * repeated reads are served from disk.
 *
 * Read path
 * ---------
 *   prepare() resolves the manifest, fetches it, downloads any packs not
 *   already cached, and loads them into an embedded odb_source_packed. All
 *   object lookups then delegate to that store without further S3 traffic.
 *
 * Write path
 * ----------
 *   write_object() and write_object_stream() write a new packfile and upload
 *   it to object storage. Transactions queue objects and then pack all of them
 *   into a single packfile on commit.
 *
 * Layout
 * ------
 *
 * <prefix>/manifest
 *     Mutable pointer. Contains the SHA-256 hex hash of the current manifest
 *     version.
 *
 * <prefix>/manifests/<sha256-hex>
 *     Immutable, content-addressed manifest version. Each line holds the bare
 *     pack hash of one active packfile, e.g.:
 *
 *       3472ebb5128456d753dee031162e0a4daef8e0bd
 *
 *     Callers derive the .pack/.idx filenames and S3 keys by surrounding the
 *     hash with the standard "pack-" prefix and the appropriate suffix.
 *
 * <prefix>/packs/<sha256-hex>.pack
 * <prefix>/packs/<sha256-hex>.idx
 *     The immutable pack and accompanying index.
 *
 * MVCC / pinning
 * --------------
 *
 * Setting GIT_S3_MANIFEST to a manifest version hash bypasses the pointer
 * fetch entirely. Any process can therefore obtain a consistent, point-in-
 * time view of the object store simply by noting the hash printed after a
 * flush and exporting it into the environment of readers.
 */
struct odb_source_s3 {
	struct odb_source base;

	/*
	 * Whether the manifest has already been fetched and local packs
	 * registered with ->packed. Cleared by reprepare() to force a
	 * refresh on the next access.
	 */
	bool initialized;

	/*
	 * Embedded packfile store used for all object lookups after the
	 * relevant packfiles have been downloaded. We set initialized=true
	 * immediately after construction to suppress its automatic directory
	 * scan and instead manage its pack list ourselves.
	 */
	struct odb_source_packed *packed;

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
};

/*
 * Allocate and initialize a new S3-backed ODB source. Packs are fetched from
 * S3 lazily on first access.
 *
 * payload - the path portion of the s3:// URI after the schema is stripped,
 *           i.e. "<bucket>" or "<bucket>/<prefix>". The first path component
 *           becomes the S3 bucket name; everything after the first '/' becomes
 *           the key prefix (empty string when absent).
 * local   - whether this is the primary (local) object source
 *
 * The local packfile cache is placed under
 * "<commondir>/objects/s3-cache/<bucket>/<prefix>" so that it is stable
 * across invocations and scoped to this repository.
 *
 * Returns NULL on failure (e.g. no AWS credentials could be found).
 */
struct odb_source_s3 *odb_source_s3_new(struct object_database *odb,
					const char *payload,
					bool local);

/*
 * Downcast to odb_source_s3; dies with BUG() on type mismatch.
 */
static inline struct odb_source_s3 *
odb_source_s3_downcast(struct odb_source *source)
{
	if (source->type != ODB_SOURCE_S3)
		BUG("trying to downcast source of type '%d' to S3", source->type);
	return (struct odb_source_s3 *)source;
}

#endif /* ODB_SOURCE_S3_H */
