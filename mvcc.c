#include "git-compat-util.h"
#include "gettext.h"
#include "hash.h"
#include "hex.h"
#include "lockfile.h"
#include "mvcc.h"
#include "strbuf.h"
#include "string-list.h"
#include "strmap.h"
#include "tempfile.h"
#include "wrapper.h"

static struct strmap storages_by_commondir = STRMAP_INIT;

static void sha256_to_hex(const unsigned char *digest, char *out)
{
	hash_to_hex_algop_r(out, digest, &hash_algos[GIT_HASH_SHA256]);
}

static void sha256_buf_hex(const void *buf, size_t len,
			   char out[GIT_SHA256_HEXSZ + 1])
{
	git_SHA256_CTX ctx;
	unsigned char digest[GIT_SHA256_RAWSZ];
	git_SHA256_Init(&ctx);
	git_SHA256_Update(&ctx, buf, len);
	git_SHA256_Final(digest, &ctx);
	sha256_to_hex(digest, out);
}

int mvcc_sha256_file_hex(const char *path, char *hex_out)
{
	unsigned char digest[GIT_SHA256_RAWSZ];
	git_SHA256_CTX ctx;
	char buf[65536];
	ssize_t bytes_read;
	int fd;

	fd = open(path, O_RDONLY);
	if (fd < 0)
		return error_errno("mvcc: cannot open '%s' for hashing", path);

	git_SHA256_Init(&ctx);
	while ((bytes_read = xread(fd, buf, sizeof(buf))) > 0)
		git_SHA256_Update(&ctx, buf, bytes_read);
	close(fd);

	if (bytes_read < 0)
		return error_errno("mvcc: read error while hashing '%s'", path);

	git_SHA256_Final(digest, &ctx);
	sha256_to_hex(digest, hex_out);
	return 0;
}

static void ensure_storage_dirs(const char *base)
{
	const char *dirs[] = {
		"pack",
		"manifests",
		"reftables",
	};
	struct strbuf path = STRBUF_INIT;

	if (mkdir(base, 0777) < 0 && errno != EEXIST)
		die_errno(_("mvcc: cannot create directory '%s'"), base);

	for (size_t i = 0; i < ARRAY_SIZE(dirs); i++) {
		strbuf_reset(&path);
		strbuf_addf(&path, "%s/%s", base, dirs[i]);

		if (mkdir(path.buf, 0777) < 0 && errno != EEXIST)
			die_errno(_("mvcc: cannot create directory '%s'"), path.buf);
	}

	strbuf_release(&path);
}

void mvcc_storage_resolve_path(struct mvcc_storage *storage,
			       const char *kind, const char *name,
			       struct strbuf *out)
{
	strbuf_addf(out, "%s/%s/%s", storage->cache_dir, kind, name);
}

void mvcc_storage_write_path(struct mvcc_storage *storage,
			     const char *kind, const char *name,
			     struct strbuf *out)
{
	if (name)
		strbuf_addf(out, "%s/%s/%s", storage->cache_dir, kind, name);
	else
		strbuf_addf(out, "%s/%s", storage->cache_dir, kind);
}

/*
 * Compute the location of the manifest pointer file: the external file
 * named by GIT_MVCC_MANIFEST_PATH when set, otherwise <cache_dir>/manifest.
 */
static void manifest_pointer_path(struct mvcc_storage *storage,
				  struct strbuf *out)
{
	if (storage->manifest_path)
		strbuf_addstr(out, storage->manifest_path);
	else
		strbuf_addf(out, "%s/manifest", storage->cache_dir);
}

/*
 * Read the on-disk manifest pointer at `path` into `out`, trimming any
 * trailing newline. Returns 0 on success, -1 if the file is missing,
 * empty, or unreadable.
 */
static int read_pointer_file(const char *path, struct strbuf *out)
{
	if (strbuf_read_file(out, path, GIT_SHA256_HEXSZ + 1) < 0)
		return -1;
	strbuf_trim(out);
	if (!out->len)
		return -1;
	return 0;
}

/*
 * Invalidate the in-process manifest cache. Used after a CAS failure so
 * that the next call to mvcc_storage_get_manifest re-resolves the pointer
 * from disk rather than returning the stale cached state.
 */
static void invalidate_manifest_cache(struct mvcc_storage *storage)
{
	struct mvcc_manifest empty = MVCC_MANIFEST_INIT;

	mvcc_manifest_release(&storage->manifest);
	storage->manifest = empty;
	storage->manifest_initialized = false;
	storage->current_version_set = false;
}

/*
 * Resolve the SHA-256 of the current manifest. Returns a newly allocated
 * string that the caller must free. Returns an empty string when no manifest
 * has been written yet. Returns NULL only on real failures.
 *
 * The lookup order is:
 *
 *   1. The GIT_MVCC_MANIFEST environment variable (snapshot pinning).
 *   2. The external pointer file named by GIT_MVCC_MANIFEST_PATH, if set.
 *   3. <cache_dir>/manifest.
 */
static char *resolve_manifest_version(struct mvcc_storage *storage)
{
	const char *pinned = getenv("GIT_MVCC_MANIFEST");
	struct strbuf path = STRBUF_INIT;
	struct strbuf content = STRBUF_INIT;
	char *result = NULL;
	ssize_t ret;

	if (pinned)
		return xstrdup(pinned);

	manifest_pointer_path(storage, &path);

	ret = strbuf_read_file(&content, path.buf, 0);
	if (ret < 0) {
		if (errno != ENOENT) {
			error_errno(_("mvcc: cannot read '%s'"), path.buf);
			goto out;
		}

		result = xstrdup("");
		goto out;
	}

	strbuf_trim(&content);
	result = strbuf_detach(&content, NULL);

out:
	strbuf_release(&path);
	strbuf_release(&content);
	return result;
}

const struct mvcc_manifest *mvcc_storage_get_manifest(struct mvcc_storage *storage)
{
	struct mvcc_manifest manifest = MVCC_MANIFEST_INIT;
	struct string_list lines = STRING_LIST_INIT_DUP;
	struct strbuf content = STRBUF_INIT;
	struct strbuf path = STRBUF_INIT;
	char *version = NULL;

	if (storage->manifest_initialized)
		return &storage->manifest;

	version = resolve_manifest_version(storage);
	if (!version)
		die(_("mvcc: failed resolving manifest version"));
	if (!*version) {
		storage->manifest = manifest;
		storage->manifest_initialized = true;
		goto out;
	}

	mvcc_storage_resolve_path(storage, "manifests", version, &path);
	if (access(path.buf, R_OK) < 0)
		die(_("mvcc: manifest body '%s' is not available locally; "
		      "ensure dependencies have been prefetched"), version);

	if (strbuf_read_file(&content, path.buf, 0) < 0)
		die_errno(_("mvcc: failed reading manifest '%s'"), path.buf);

	strbuf_trim(&content);
	string_list_split(&lines, content.buf, "\n", -1);
	for (size_t i = 0; i < lines.nr; i++) {
		const char *line = lines.items[i].string;
		if (skip_prefix(line, "p: ", &line))
			string_list_append(&manifest.packs, line);
		else if (skip_prefix(line, "r: ", &line))
			string_list_append(&manifest.reftables, line);
		else if (*line)
			die(_("mvcc: malformed manifest line '%s'"), line);
	}

	storage->manifest = manifest;
	storage->manifest_initialized = true;

	/*
	 * Record the resolved version so that the next activation can use
	 * it as the expected-old value of its compare-and-swap.
	 */
	if (strlen(version) == GIT_SHA256_HEXSZ) {
		memcpy(storage->current_version, version, GIT_SHA256_HEXSZ + 1);
		storage->current_version_set = true;
	}

out:
	free(version);
	string_list_clear(&lines, 0);
	strbuf_release(&content);
	strbuf_release(&path);
	return &storage->manifest;
}

int mvcc_storage_write_manifest(struct mvcc_storage *storage,
				const struct mvcc_manifest *manifest,
				char *version_hex_out)
{
	struct strbuf content = STRBUF_INIT;
	struct strbuf path = STRBUF_INIT;
	struct strbuf manifests_dir = STRBUF_INIT;
	char version_hex[GIT_SHA256_HEXSZ + 1];
	struct tempfile *tempfile = NULL;
	int ret = 0;

	if (storage->pinned)
		die(_("mvcc: cannot write while GIT_MVCC_MANIFEST is set"));

	for (size_t i = 0; i < manifest->packs.nr; i++)
		strbuf_addf(&content, "p: %s\n", manifest->packs.items[i].string);
	for (size_t i = 0; i < manifest->reftables.nr; i++)
		strbuf_addf(&content, "r: %s\n", manifest->reftables.items[i].string);

	sha256_buf_hex(content.buf, content.len, version_hex);

	mvcc_storage_write_path(storage, "manifests", NULL, &manifests_dir);
	if (mkdir(manifests_dir.buf, 0777) < 0 && errno != EEXIST) {
		ret = error_errno(_("mvcc: cannot create '%s'"),
				  manifests_dir.buf);
		goto out;
	}

	strbuf_addf(&path, "%s/tmp_manifest_XXXXXX", manifests_dir.buf);
	tempfile = xmks_tempfile(path.buf);

	if (write_in_full(get_tempfile_fd(tempfile), content.buf, content.len) < 0) {
		ret = error_errno(_("mvcc: cannot write manifest temp file"));
		goto out;
	}

	strbuf_reset(&path);
	strbuf_addf(&path, "%s/%s", manifests_dir.buf, version_hex);

	if (rename_tempfile(&tempfile, path.buf) < 0) {
		ret = error_errno(_("mvcc: cannot move manifest tempfile into place"));
		goto out;
	}

	if (version_hex_out)
		memcpy(version_hex_out, version_hex, sizeof(version_hex));

out:
	delete_tempfile(&tempfile);
	strbuf_release(&content);
	strbuf_release(&path);
	strbuf_release(&manifests_dir);
	return ret;
}

int mvcc_storage_activate_manifest(struct mvcc_storage *storage,
				   const struct mvcc_manifest *manifest)
{
	struct lock_file lock = LOCK_INIT;
	struct strbuf pointer_path = STRBUF_INIT;
	struct strbuf observed = STRBUF_INIT;
	char version_hex[GIT_SHA256_HEXSZ + 1];
	int fd, ret;

	if (storage->pinned)
		die(_("mvcc: cannot write while GIT_MVCC_MANIFEST is set"));

	/*
	 * Make sure the in-memory manifest cache has been populated so
	 * that the compare-and-swap below has an expected-old value to
	 * compare against. For repositories that have not yet seen any
	 * manifest activation, current_version_set remains false and the
	 * CAS is skipped (first-ever activation).
	 */
	mvcc_storage_get_manifest(storage);

	ret = mvcc_storage_write_manifest(storage, manifest, version_hex);
	if (ret < 0)
		goto out;

	manifest_pointer_path(storage, &pointer_path);

	fd = hold_lock_file_for_update(&lock, pointer_path.buf, 0);
	if (fd < 0) {
		ret = error_errno(_("mvcc: cannot lock manifest pointer '%s'"),
				  pointer_path.buf);
		goto out;
	}

	/*
	 * Compare-and-swap: with the lockfile held, no other writer can
	 * mutate the pointer concurrently, so reading it here gives us the
	 * value that any prospective concurrent writer would have observed
	 * had it been able to acquire the lock instead of us. If that
	 * value differs from what we built our new manifest on top of,
	 * another writer beat us to the activation and we must abort
	 * rather than clobber its changes.
	 */
	if (storage->current_version_set) {
		if (read_pointer_file(pointer_path.buf, &observed) < 0) {
			ret = error_errno(_("mvcc: cannot read manifest pointer '%s' for CAS check"),
					  pointer_path.buf);
			invalidate_manifest_cache(storage);
			rollback_lock_file(&lock);
			goto out;
		}
		if (strcmp(observed.buf, storage->current_version)) {
			ret = error(_("mvcc: stale base manifest at '%s'; another writer advanced the pointer"),
				    pointer_path.buf);
			invalidate_manifest_cache(storage);
			rollback_lock_file(&lock);
			goto out;
		}
	}

	if (write_in_full(fd, version_hex, GIT_SHA256_HEXSZ) < 0 ||
	    write_in_full(fd, "\n", 1) < 0) {
		ret = error_errno(_("mvcc: cannot write manifest pointer"));
		rollback_lock_file(&lock);
		goto out;
	}

	if (commit_lock_file(&lock) < 0) {
		ret = error_errno(_("mvcc: cannot commit manifest pointer"));
		goto out;
	}

	/*
	 * Update the cached manifest so that subsequent reads in this
	 * process see the new state without re-reading the pointer file.
	 * Record the new version as the next CAS's expected-old value.
	 */
	mvcc_manifest_release(&storage->manifest);
	mvcc_manifest_copy(manifest, &storage->manifest);
	storage->manifest_initialized = true;
	memcpy(storage->current_version, version_hex, GIT_SHA256_HEXSZ + 1);
	storage->current_version_set = true;

	ret = 0;

out:
	strbuf_release(&pointer_path);
	strbuf_release(&observed);
	return ret;
}

/*
 * Verify that the external manifest pointer file at `path` exists, is a
 * regular file, and is non-empty. Dies with a distinct diagnostic for
 * each failure mode so that orchestrator-side bugs are easy to attribute.
 */
static void require_manifest_pointer_file(const char *path)
{
	struct stat st;

	if (stat(path, &st) < 0) {
		if (errno == ENOENT)
			die(_("mvcc: manifest pointer file '%s' does not exist"),
			    path);
		die_errno(_("mvcc: cannot stat '%s'"), path);
	}

	if (!S_ISREG(st.st_mode))
		die(_("mvcc: manifest pointer '%s' is not a regular file"),
		    path);

	if (st.st_size == 0)
		die(_("mvcc: manifest pointer file '%s' is empty"), path);
}

struct mvcc_storage *mvcc_storage_get(const char *commondir)
{
	struct mvcc_manifest empty_manifest = MVCC_MANIFEST_INIT;
	struct mvcc_storage *storage;
	const char *manifest_path;
	const char *pinned;

	if (!commondir || !*commondir)
		BUG("mvcc_storage_get requires a common directory");

	/*
	 * Look up by `commondir` so that any number of backends (e.g. the
	 * ODB source and the refs backend) configured against the same
	 * repository share a single handle, and therefore a single
	 * in-memory manifest view.
	 */
	storage = strmap_get(&storages_by_commondir, commondir);
	if (storage) {
		storage->refcount++;
		return storage;
	}

	CALLOC_ARRAY(storage, 1);
	storage->refcount++;
	storage->cache_dir = xstrfmt("%s/mvcc-cache", commondir);
	storage->manifest = empty_manifest;

	pinned = getenv("GIT_MVCC_MANIFEST");
	storage->pinned = pinned && *pinned;

	ensure_storage_dirs(storage->cache_dir);

	manifest_path = getenv("GIT_MVCC_MANIFEST_PATH");
	if (manifest_path && *manifest_path) {
		require_manifest_pointer_file(manifest_path);
		storage->manifest_path = xstrdup(manifest_path);
	}

	if (strmap_put(&storages_by_commondir, commondir, storage))
		BUG("mvcc storage already cached");

	return storage;
}

void mvcc_storage_release(struct mvcc_storage *storage)
{
	size_t suffix_len;
	size_t cache_len;
	char *commondir;

	if (!storage)
		return;
	if (storage->refcount-- > 1)
		return;

	/*
	 * Remove ourselves from the lookup map. The map key is the
	 * commondir we were keyed under, which is the suffix-trimmed form
	 * of our cache_dir.
	 */
	suffix_len = strlen("/mvcc-cache");
	cache_len = strlen(storage->cache_dir);

	if (cache_len < suffix_len)
		BUG("mvcc storage cache_dir '%s' has unexpected shape",
		    storage->cache_dir);
	commondir = xstrndup(storage->cache_dir,
			     cache_len - suffix_len);
	strmap_remove(&storages_by_commondir, commondir, 0);

	mvcc_manifest_release(&storage->manifest);
	free(storage->cache_dir);
	free(storage->manifest_path);
	free(commondir);
	free(storage);
}

void mvcc_manifest_release(struct mvcc_manifest *manifest)
{
	string_list_clear(&manifest->packs, 0);
	string_list_clear(&manifest->reftables, 0);
}

void mvcc_manifest_copy(const struct mvcc_manifest *from,
			struct mvcc_manifest *to)
{
	struct string_list_item *item;
	for_each_string_list_item(item, &from->packs)
		string_list_append(&to->packs, item->string);
	for_each_string_list_item(item, &from->reftables)
		string_list_append(&to->reftables, item->string);
}
