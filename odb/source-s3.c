#define USE_THE_REPOSITORY_VARIABLE

#include "git-compat-util.h"
#include "config.h"
#include "csum-file.h"
#include "environment.h"
#include "gettext.h"
#include "git-zlib.h"
#include "hash.h"
#include "hex.h"
#include "object-file.h"
#include "odb/source.h"
#include "odb/source-packed.h"
#include "odb/source-s3.h"
#include "odb/streaming.h"
#include "odb/transaction.h"
#include "pack.h"
#include "pack-revindex.h"
#include "packfile.h"
#include "path.h"
#include "repository.h"
#include "strbuf.h"
#include "string-list.h"
#include "s3.h"
#include "tempfile.h"
#include "wrapper.h"
#include "write-or-die.h"

#include <curl/curl.h>
#include <curl/easy.h>

/*
 * Fetch the manifest, download any packs that are not yet in the local cache,
 * and load them into the embedded odb_source_packed.
 */
static void odb_source_s3_prepare(struct odb_source_s3 *s3)
{
	const struct s3_manifest *manifest;
	struct strbuf path = STRBUF_INIT;
	struct strbuf key = STRBUF_INIT;

	if (s3->initialized)
		return;

	manifest = s3_storage_get_manifest(s3->storage);

	for (size_t i = 0; i < manifest->packs.nr; i++) {
		const char *hash = manifest->packs.items[i].string;

		strbuf_reset(&path);
		strbuf_addf(&path, "%s/packs/%s.pack", s3->storage->cache_dir, hash);
		if (access(path.buf, R_OK) < 0) {
			const char *suffix = path.buf + strlen(s3->storage->cache_dir) + 1;
			if (s3_get_to_file(s3->storage, s3_key(s3->storage, &key, suffix), path.buf) < 0)
				die("failed downloading pack '%s'", key.buf);
		}

		strbuf_reset(&path);
		strbuf_addf(&path, "%s/packs/%s.rev", s3->storage->cache_dir, hash);
		if (access(path.buf, R_OK) < 0) {
			const char *suffix = path.buf + strlen(s3->storage->cache_dir) + 1;
			if (s3_get_to_file(s3->storage, s3_key(s3->storage, &key, suffix), path.buf) < 0)
				die("failed downloading reverse index '%s'", key.buf);
		}

		strbuf_reset(&path);
		strbuf_addf(&path, "%s/packs/%s.idx", s3->storage->cache_dir, hash);
		if (access(path.buf, R_OK) < 0) {
			const char *suffix = path.buf + strlen(s3->storage->cache_dir) + 1;
			if (s3_get_to_file(s3->storage, s3_key(s3->storage, &key, suffix), path.buf) < 0)
				die("failed downloading index '%s'", key.buf);
		}

		/* Register the pack with the embedded packed store. */
		if (!packfile_store_load_pack(s3->packed, path.buf, 1))
			die("failed to load pack '%s'", hash);
	}

	s3->initialized = true;

	strbuf_release(&path);
	strbuf_release(&key);
}

static void odb_source_s3_free(struct odb_source *source)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);

	odb_source_free(&s3->packed->base);
	s3_storage_release(s3->storage);
	odb_source_release(source);
	free(s3);
}

static void odb_source_s3_close(struct odb_source *source)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	odb_source_close(&s3->packed->base);
}

static void odb_source_s3_reprepare(struct odb_source *source)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);

	/*
	 * Drop the initialized flag so that the next access re-fetches the
	 * manifest. The embedded packed store keeps its existing packs; new
	 * ones discovered in the refreshed manifest are added incrementally.
	 */
	s3->initialized = false;
	odb_source_s3_prepare(s3);
}

static int odb_source_s3_read_object_info(struct odb_source *source,
					  const struct object_id *oid,
					  struct object_info *oi,
					  enum object_info_flags flags)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	odb_source_s3_prepare(s3);

	/* Strip `OBJECT_INFO_SECOND_READ` so that we don't end up repreparing. */
	return odb_source_read_object_info(&s3->packed->base, oid, oi,
					   flags & ~OBJECT_INFO_SECOND_READ);
}

static int odb_source_s3_read_object_stream(struct odb_read_stream **out,
					    struct odb_source *source,
					    const struct object_id *oid)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	odb_source_s3_prepare(s3);
	return odb_source_read_object_stream(out, &s3->packed->base, oid);
}

static int odb_source_s3_for_each_object(
	struct odb_source *source,
	const struct object_info *request,
	odb_for_each_object_cb cb,
	void *cb_data,
	const struct odb_for_each_object_options *opts)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	odb_source_s3_prepare(s3);
	return odb_source_for_each_object(&s3->packed->base, request,
					  cb, cb_data, opts);
}

static int odb_source_s3_count_objects(struct odb_source *source,
				       enum odb_count_objects_flags flags,
				       unsigned long *out)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	odb_source_s3_prepare(s3);
	return odb_source_count_objects(&s3->packed->base, flags, out);
}

static int odb_source_s3_find_abbrev_len(struct odb_source *source,
					 const struct object_id *oid,
					 unsigned min_len,
					 unsigned *out)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	odb_source_s3_prepare(s3);
	return odb_source_find_abbrev_len(&s3->packed->base, oid,
					  min_len, out);
}

static int odb_source_s3_freshen_object(struct odb_source *source,
					const struct object_id *oid,
					const time_t *mtime)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	odb_source_s3_prepare(s3);
	return odb_source_freshen_object(&s3->packed->base, oid, mtime);
}

struct s3_pending_object {
	struct object_id oid;
	enum object_type type;
	unsigned char *data;
	unsigned long len;
};

static int write_pending_to_pack(struct odb_source_s3 *s3,
				 struct s3_pending_object *objects,
				 size_t objects_nr,
				 char **pack_path_out,
				 char **idx_path_out,
				 char **rev_path_out)
{
	struct repository *repo = s3->base.odb->repo;
	const struct git_hash_algo *algo = repo->hash_algo;
	char *pack_tmp_path = NULL, *pack_path = NULL, *idx_path = NULL, *rev_path = NULL;
	unsigned char pack_hash[GIT_MAX_RAWSZ];
	struct pack_idx_entry **idx_entries;
	struct pack_idx_option idx_opts;
	struct hashfile *f = NULL;
	struct tempfile *tempfile;
	int ret;

	pack_tmp_path = xstrfmt("%s/packs/tmp_pack_XXXXXX",
				s3->storage->cache_dir);
	tempfile = xmks_tempfile(pack_tmp_path);
	f = hashfd(algo, get_tempfile_fd(tempfile), pack_tmp_path);

	CALLOC_ARRAY(idx_entries, objects_nr);
	for (size_t i = 0; i < objects_nr; i++)
		CALLOC_ARRAY(idx_entries[i], 1);

	write_pack_header(f, objects_nr);

	for (size_t i = 0; i < objects_nr; i++) {
		struct s3_pending_object *obj = &objects[i];
		unsigned long compressed_bound;
		unsigned char *compressed;
		unsigned char hdr[10];
		git_zstream zs;
		int hdr_len;

		idx_entries[i]->offset = hashfile_total(f);
		oidcpy(&idx_entries[i]->oid, &obj->oid);

		hdr_len = encode_in_pack_object_header(hdr, sizeof(hdr),
						       obj->type, obj->len);
		crc32_begin(f);
		hashwrite(f, hdr, hdr_len);

		git_deflate_init(&zs, zlib_compression_level);
		compressed_bound = git_deflate_bound(&zs, obj->len);
		compressed = xmalloc(compressed_bound);

		zs.next_in = obj->data;
		zs.avail_in = obj->len;
		zs.next_out = compressed;
		zs.avail_out = compressed_bound;

		if (git_deflate(&zs, Z_FINISH) != Z_STREAM_END) {
			git_deflate_end(&zs);
			free(compressed);
			ret = error("s3: deflate failed for object %s",
				    oid_to_hex(&obj->oid));
			goto out;
		}

		hashwrite(f, compressed, zs.total_out);
		idx_entries[i]->crc32 = crc32_end(f);

		git_deflate_end(&zs);
		free(compressed);
	}

	/*
	 * Finalise the pack file. This appends the pack's trailing hash,
	 * writes it into pack_hash[], and closes the file descriptor.
	 */
	finalize_hashfile(f, pack_hash, FSYNC_COMPONENT_PACK,
			  CSUM_HASH_IN_STREAM | CSUM_FSYNC);
	f = NULL;

	pack_path = xstrfmt("%s/packs/%s.pack", s3->storage->cache_dir,
			    hash_to_hex_algop(pack_hash, algo));
	if (rename_tempfile(&tempfile, pack_path) < 0) {
		ret = error_errno("s3: cannot move packfile into place '%s'", pack_path);
		goto out;
	}

	reset_pack_idx_option(&idx_opts);
	idx_path = xstrfmt("%s/packs/%s.idx", s3->storage->cache_dir,
			   hash_to_hex_algop(pack_hash, algo));
	write_idx_file(repo, idx_path, idx_entries, objects_nr,
		       &idx_opts, pack_hash);

	rev_path = xstrfmt("%s/packs/%s.rev", s3->storage->cache_dir,
			   hash_to_hex_algop(pack_hash, algo));
	free(write_rev_file(repo, rev_path, idx_entries, objects_nr,
			    pack_hash, WRITE_REV));

	*pack_path_out = pack_path;
	pack_path = NULL;
	*idx_path_out = idx_path;
	idx_path = NULL;
	*rev_path_out = rev_path;
	rev_path = NULL;

	ret = 0;

out:
	for (size_t i = 0; i < objects_nr; i++)
		free(idx_entries[i]);
	delete_tempfile(&tempfile);
	free(pack_tmp_path);
	free(idx_entries);
	free(pack_path);
	free(idx_path);
	free(rev_path);
	if (f)
		free_hashfile(f);
	return ret;
}

static int write_objects(struct odb_source_s3 *s3,
			 struct s3_pending_object *objects,
			 size_t objects_nr)
{
	char *pack_path = NULL, *idx_path = NULL, *rev_path = NULL;
	const char *pack_basename, *idx_basename, *rev_basename, *hash_end;
	struct s3_manifest new_manifest = S3_MANIFEST_INIT;
	struct strbuf key = STRBUF_INIT;
	int ret = 0;

	if (!objects_nr)
		return 0;

	if (write_pending_to_pack(s3, objects, objects_nr,
				  &pack_path, &idx_path, &rev_path) < 0) {
		ret = -1;
		goto out;
	}

	/* Upload the .pack and .idx to S3. */
	pack_basename = strrchr(pack_path, '/') + 1;
	s3_key(s3->storage, &key, pack_path + strlen(s3->storage->cache_dir) + 1);
	if (s3_put_from_file(s3->storage, key.buf, pack_path) < 0) {
		ret = error("failed uploading pack '%s'", pack_basename);
		goto out;
	}

	idx_basename = strrchr(idx_path, '/') + 1;
	s3_key(s3->storage, &key, idx_path + strlen(s3->storage->cache_dir) + 1);
	if (s3_put_from_file(s3->storage, key.buf, idx_path) < 0) {
		ret = error("failed uploading index '%s'", idx_basename);
		goto out;
	}

	rev_basename = strrchr(rev_path, '/') + 1;
	s3_key(s3->storage, &key, rev_path + strlen(s3->storage->cache_dir) + 1);
	if (s3_put_from_file(s3->storage, key.buf, rev_path) < 0) {
		ret = error("failed uploading reverse index '%s'", rev_basename);
		goto out;
	}

	/* Fetch the current manifest, append the hash, re-upload. */
	s3_manifest_copy(s3_storage_get_manifest(s3->storage), &new_manifest);

	hash_end = strrchr(pack_basename, '.');
	string_list_append_nodup(&new_manifest.packs,
				 xstrndup(pack_basename, hash_end - pack_basename));

	if (s3_storage_update_manifest(s3->storage, &new_manifest) < 0) {
		ret = -1;
		goto out;
	}

	if (!packfile_store_load_pack(s3->packed, idx_path, 1))
		die("s3: failed to activate newly written pack '%s'", pack_basename);

out:
	s3_manifest_release(&new_manifest);
	strbuf_release(&key);
	free(pack_path);
	free(idx_path);
	free(rev_path);
	return ret;
}

static int odb_source_s3_write_object(struct odb_source *source,
				      const void *buf,
				      unsigned long len,
				      enum object_type type,
				      const struct object_id *oid,
				      const struct object_id *compat_oid UNUSED,
				      const time_t *mtime UNUSED,
				      enum odb_write_object_flags flags UNUSED)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	struct s3_pending_object object = {
		.type = type,
		.len = len,
		.data = (unsigned char *)buf,
	};

	oidcpy(&object.oid, oid);

	return write_objects(s3, &object, 1);
}

static int read_write_stream(struct odb_write_stream *stream,
			     size_t len, unsigned char **out)
{
	unsigned char buf[16384];
	unsigned char *data;
	size_t total_read = 0;
	int ret;

	CALLOC_ARRAY(data, len);
	while (!stream->is_finished) {
		ssize_t bytes_read = odb_write_stream_read(stream, buf, sizeof(buf));
		if (bytes_read < 0) {
			ret = error("s3: stream read error");
			goto out;
		}

		if (total_read + (size_t)bytes_read > len) {
			ret = error("s3: stream yielded more bytes than expected");
			goto out;
		}

		memcpy(data + total_read, buf, bytes_read);
		total_read += bytes_read;
	}

	if (total_read != len) {
		ret = error("s3: stream yielded fewer bytes than expected (%" PRIuMAX " of %" PRIuMAX ")",
			    (uintmax_t)total_read, (uintmax_t)len);
		goto out;
	}

	*out = data;
	data = NULL;
	ret = 0;

out:
	free(data);
	return ret;
}

static int odb_source_s3_write_object_stream(struct odb_source *source,
					     struct odb_write_stream *stream,
					     size_t len,
					     struct object_id *oid)
{
	struct object_id computed_oid;
	unsigned char *data = NULL;
	int ret;

	ret = read_write_stream(stream, len, &data);
	if (ret)
		goto out;

	hash_object_file(source->odb->repo->hash_algo, data, len,
			 OBJ_BLOB, &computed_oid);
	oidcpy(oid, &computed_oid);
	ret = odb_source_s3_write_object(source, data, len, OBJ_BLOB, oid,
					 NULL, NULL, 0);

out:
	free(data);
	return ret;
}

struct odb_transaction_s3 {
	struct odb_transaction base;
	struct odb_source_s3 *s3;
	struct s3_pending_object *objects;
	size_t objects_nr, objects_alloc;
};

static void odb_transaction_s3_commit(struct odb_transaction *base)
{
	struct odb_transaction_s3 *tx =
		container_of(base, struct odb_transaction_s3, base);

	if (write_objects(tx->s3, tx->objects, tx->objects_nr) < 0)
		die("s3: failed to flush objects on transaction commit");

	for (size_t i = 0; i < tx->objects_nr; i++)
		free(tx->objects[i].data);
	free(tx->objects);
}

static int odb_transaction_s3_write_object_stream(struct odb_transaction *base,
						  struct odb_write_stream *stream,
						  size_t len,
						  struct object_id *oid)
{
	struct odb_transaction_s3 *tx = container_of(base, struct odb_transaction_s3, base);
	unsigned char *data;
	int ret;

	ret = read_write_stream(stream, len, &data);
	if (ret)
		return ret;

	ALLOC_GROW(tx->objects, tx->objects_nr, tx->objects_alloc);
	tx->objects[tx->objects_nr].type = OBJ_BLOB;
	tx->objects[tx->objects_nr].data = data;
	tx->objects[tx->objects_nr].len = len;
	hash_object_file(tx->s3->base.odb->repo->hash_algo, data, len,
			 OBJ_BLOB, &tx->objects[tx->objects_nr].oid);
	oidcpy(oid, &tx->objects[tx->objects_nr].oid);

	tx->objects_nr++;

	return 0;
}

static int odb_source_s3_begin_transaction(struct odb_source *source,
					   struct odb_transaction **out,
					   enum odb_transaction_flags flags UNUSED)
{
	struct odb_transaction_s3 *tx;

	CALLOC_ARRAY(tx, 1);
	tx->s3 = odb_source_s3_downcast(source);
	tx->base.source = source;
	tx->base.commit = odb_transaction_s3_commit;
	tx->base.write_object_stream = odb_transaction_s3_write_object_stream;

	*out = &tx->base;
	return 0;
}

static int odb_source_s3_read_alternates(struct odb_source *source UNUSED,
					 struct strvec *out UNUSED)
{
	return 0;
}

static int odb_source_s3_write_alternate(struct odb_source *source UNUSED,
					 const char *alternate UNUSED)
{
	return error("s3 source: alternates are not supported");
}

static int odb_source_s3_get_packs(struct odb_source *source,
				   struct packfile_list_entry **out)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	odb_source_s3_prepare(s3);
	return odb_source_get_packs(&s3->packed->base, out);
}

static int odb_source_s3_fsck(struct odb_source *source UNUSED,
			      struct odb_fsck_options *opts UNUSED)
{
	/* TODO: we don't implement fsck yet. */
	return 0;
}

static int s3_pack_cmp(const void *va, const void *vb)
{
	uint32_t a = (*(const struct packed_git **)va)->num_objects;
	uint32_t b = (*(const struct packed_git **)vb)->num_objects;
	return (a > b) - (a < b);
}

/*
 * Collect all packed_git pointers from the embedded packed store, sort them
 * in ascending order by object count, and return a heap-allocated array.
 * The caller must free() the returned array. *nr_out receives the count.
 *
 * Returns NULL (with *nr_out == 0) when there are no packs.
 */
static struct packed_git **s3_sorted_packs(struct odb_source_s3 *s3,
					   size_t *nr_out)
{
	struct packfile_list_entry *e;
	struct packed_git **packs = NULL;
	size_t nr = 0, alloc = 0;

	for (e = packfile_store_get_packs(s3->packed); e; e = e->next) {
		ALLOC_GROW(packs, nr + 1, alloc);
		packs[nr++] = e->pack;
	}

	/*
	 * Ensure the object count is loaded for every pack so that the sort
	 * key is available without touching the index again later.
	 */
	for (size_t i = 0; i < nr; i++) {
		if (open_pack_index(packs[i]))
			die(_("s3: cannot open index for '%s'"), packs[i]->pack_name);
	}

	/* Sort ascending by num_objects, matching pack_geometry_cmp(). */
	QSORT(packs, nr, s3_pack_cmp);

	*nr_out = nr;
	return packs;
}

/*
 * Determine how many of the smallest packs (in ascending object-count order)
 * need to be rolled up to restore a geometric progression with the given
 * factor. Returns the split index: packs[0..split-1] must be merged.
 *
 * This implements the same algorithm as compute_pack_geometry_split() in
 * repack-geometry.c, adapted to operate on a plain array of packed_git
 * pointers rather than a struct pack_geometry.
 */
static size_t s3_compute_geometry_split(struct packed_git **packs, size_t nr,
					int factor)
{
	size_t i, split;
	uint32_t total = 0;

	if (!nr)
		return 0;

	/*
	 * Walk from the largest pack down and find the first consecutive pair
	 * that violates the geometric property.
	 */
	for (i = nr - 1; i > 0; i--) {
		uint32_t cur  = packs[i]->num_objects;
		uint32_t prev = packs[i - 1]->num_objects;

		if (unsigned_mult_overflows(factor, prev))
			die(_("s3: pack '%s' too large for geometric progression"),
			    packs[i - 1]->pack_name);

		if (cur < (uint32_t)factor * prev)
			break;
	}

	split = i;
	if (split)
		split++; /* the larger pack in the violating pair stays */

	/*
	 * Account for the combined weight of the roll-up shifting the
	 * split point further right.
	 */
	for (i = 0; i < split; i++) {
		if (unsigned_add_overflows(total, packs[i]->num_objects))
			die(_("s3: pack '%s' too large to roll up"),
			    packs[i]->pack_name);
		total += packs[i]->num_objects;
	}

	/*
	 * Anything to the left of 'split' must be merged into a new pack. But
	 * creating that new pack may cause packs above the split to no longer
	 * form a geometric progression.
	 *
	 * Compute the expected size of the merged pack, then absorb as many
	 * packs from the heavy half as needed to restore the progression.
	 */
	for (i = split; i < nr; i++) {
		uint32_t cur = packs[i]->num_objects;

		if (unsigned_mult_overflows(factor, total))
			die(_("s3: pack '%s' too large to roll up"),
			    packs[i]->pack_name);

		if (cur < (uint32_t)factor * total) {
			if (unsigned_add_overflows(total, cur))
				die(_("s3: pack '%s' too large to roll up"),
				    packs[i]->pack_name);
			split++;
			total += cur;
		} else {
			break;
		}
	}

	return split;
}

static bool odb_source_s3_optimize_required(struct odb_source *source,
					    const struct odb_optimize_options *opts)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	struct packed_git **packs;
	size_t split, nr;

	switch (opts->strategy) {
	case ODB_OPTIMIZE_INCREMENTAL:
		/* No loose objects in the S3 backend. */
		return false;
	case ODB_OPTIMIZE_GEOMETRIC:
		break;
	default:
		BUG("unknown optimize strategy '%d'", opts->strategy);
	}

	odb_source_s3_prepare(s3);
	packs = s3_sorted_packs(s3, &nr);
	split = s3_compute_geometry_split(packs, nr, 2);

	free(packs);
	return split > 0;
}

/*
 * Merge the packs[0..packs_nr-1] into a single new pack stored under
 * s3->storage.cache_dir/packs/. On success, *pack_path_out and *idx_path_out
 * are set to heap-allocated paths that the caller must free().
 *
 * All S3-managed packs contain only fully-resolved (non-delta) objects,
 * so the raw compressed object data from each source pack can be copied
 * verbatim without any delta-offset fixups.
 */
static int s3_merge_packs(struct odb_source_s3 *s3,
			  struct packed_git **packs, size_t packs_nr,
			  char **pack_path_out, char **idx_path_out,
			  char **rev_path_out)
{
	struct repository *repo = s3->base.odb->repo;
	const struct git_hash_algo *algo = repo->hash_algo;
	char *pack_tmp_path = NULL, *pack_path = NULL, *idx_path = NULL,
	     *rev_path = NULL;
	unsigned char pack_hash[GIT_MAX_RAWSZ];
	struct pack_idx_entry **idx_entries = NULL;
	struct pack_idx_option idx_opts;
	struct hashfile *f = NULL;
	struct tempfile *tempfile;
	uint32_t total_objects = 0;
	size_t idx_nr = 0;
	int ret;

	for (size_t i = 0; i < packs_nr; i++)
		total_objects += packs[i]->num_objects;

	CALLOC_ARRAY(idx_entries, total_objects);
	for (size_t i = 0; i < total_objects; i++)
		CALLOC_ARRAY(idx_entries[i], 1);

	pack_tmp_path = xstrfmt("%s/packs/tmp_pack_XXXXXX",
				s3->storage->cache_dir);
	tempfile = xmks_tempfile(pack_tmp_path);
	f = hashfd(algo, get_tempfile_fd(tempfile), pack_tmp_path);

	write_pack_header(f, total_objects);

	/*
	 * TODO: We're writing data as-is, without computing any deltas or even
	 * checking for duplicate objects. This needs to be fixed up
	 * eventually, but it's good enough for now.
	 */
	for (size_t i = 0; i < packs_nr; i++) {
		struct packed_git *p = packs[i];
		struct pack_window *w_curs = NULL;

		if (open_pack_index(p)) {
			ret = error("s3: cannot open index for '%s'",
				    p->pack_name);
			goto out;
		}

		if (load_pack_revindex(repo, p)) {
			ret = error("s3: cannot load revindex for '%s'",
				    p->pack_name);
			goto out;
		}

		/*
		 * Iterate objects in pack-file order (not OID-sorted index
		 * order) using the reverse index so that adjacent positions
		 * give us contiguous byte ranges we can copy verbatim.
		 */
		for (uint32_t j = 0; j < p->num_objects; j++) {
			off_t obj_offset = pack_pos_to_offset(p, j);
			off_t next_offset = pack_pos_to_offset(p, j + 1);
			off_t obj_size = next_offset - obj_offset;
			uint32_t index_pos = pack_pos_to_index(p, j);

			idx_entries[idx_nr]->offset = hashfile_total(f);
			if (nth_packed_object_id(&idx_entries[idx_nr]->oid,
						 p, index_pos)) {
				ret = error("s3: cannot read object id at "
					    "position %"PRIu32" in '%s'",
					    j, p->pack_name);
				unuse_pack(&w_curs);
				goto out;
			}

			crc32_begin(f);
			while (obj_size > 0) {
				unsigned long avail;
				unsigned char *data = use_pack(p, &w_curs,
							       obj_offset,
							       &avail);
				if (avail > (unsigned long)obj_size)
					avail = (unsigned long)obj_size;
				hashwrite(f, data, avail);
				obj_offset += avail;
				obj_size -= avail;
			}
			idx_entries[idx_nr]->crc32 = crc32_end(f);

			idx_nr++;
		}

		unuse_pack(&w_curs);
	}

	finalize_hashfile(f, pack_hash, FSYNC_COMPONENT_PACK,
			  CSUM_HASH_IN_STREAM | CSUM_FSYNC);
	f = NULL;

	pack_path = xstrfmt("%s/packs/%s.pack", s3->storage->cache_dir,
			    hash_to_hex_algop(pack_hash, algo));
	if (rename_tempfile(&tempfile, pack_path) < 0) {
		ret = error_errno("s3: cannot rename merged packfile to '%s'",
				  pack_path);
		goto out;
	}

	reset_pack_idx_option(&idx_opts);
	idx_path = xstrfmt("%s/packs/%s.idx", s3->storage->cache_dir,
			   hash_to_hex_algop(pack_hash, algo));
	write_idx_file(repo, idx_path, idx_entries, total_objects,
		       &idx_opts, pack_hash);

	/*
	 * Write the reverse index alongside the pack and regular index so
	 * that pack_pos_to_offset() and pack_pos_to_index() can be served
	 * from disk rather than recomputed in memory on every access.
	 */
	rev_path = xstrfmt("%s/packs/%s.rev", s3->storage->cache_dir,
			   hash_to_hex_algop(pack_hash, algo));
	free(write_rev_file(repo, rev_path, idx_entries, total_objects,
			    pack_hash, WRITE_REV));

	*pack_path_out = pack_path;
	pack_path = NULL;
	*idx_path_out = idx_path;
	idx_path = NULL;
	*rev_path_out = rev_path;
	rev_path = NULL;
	ret = 0;

out:
	for (size_t i = 0; i < total_objects; i++)
		free(idx_entries[i]);
	free(idx_entries);
	delete_tempfile(&tempfile);
	free(pack_tmp_path);
	free(pack_path);
	free(idx_path);
	free(rev_path);
	if (f)
		free_hashfile(f);
	return ret;
}

static int odb_source_s3_optimize(struct odb_source *source,
				  const struct odb_optimize_options *opts)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	char *pack_path = NULL, *idx_path = NULL, *rev_path = NULL;
	struct s3_manifest new_manifest = S3_MANIFEST_INIT;
	struct packed_git **packs = NULL;
	struct strbuf buf = STRBUF_INIT;
	const char *merged_basename;
	size_t nr, split;
	int factor = 2;
	int ret;

	switch (opts->strategy) {
	case ODB_OPTIMIZE_INCREMENTAL:
		/* No loose objects in the S3 backend; nothing to do. */
		return 0;
	case ODB_OPTIMIZE_GEOMETRIC:
		break;
	default:
		BUG("unknown optimize strategy '%d'", opts->strategy);
	}

	repo_config_get_int(source->odb->repo,
			    "maintenance.geometric-repack.splitFactor", &factor);

	odb_source_s3_prepare(s3);
	packs = s3_sorted_packs(s3, &nr);
	split = s3_compute_geometry_split(packs, nr, factor);
	if (!split) {
		ret = 0;
		goto out; /* already a geometric progression */
	}

	if (s3_merge_packs(s3, packs, split, &pack_path, &idx_path,
			   &rev_path) < 0) {
		ret = -1;
		goto out;
	}

	/* Upload .pack */
	s3_key(s3->storage, &buf, pack_path + strlen(s3->storage->cache_dir) + 1);
	if (s3_put_from_file(s3->storage, buf.buf, pack_path) < 0) {
		ret = error("s3: failed to upload merged pack");
		goto out;
	}

	/* Upload .idx */
	s3_key(s3->storage, &buf, idx_path + strlen(s3->storage->cache_dir) + 1);
	if (s3_put_from_file(s3->storage, buf.buf, idx_path) < 0) {
		ret = error("s3: failed to upload merged index");
		goto out;
	}

	/* Upload .rev */
	s3_key(s3->storage, &buf, rev_path + strlen(s3->storage->cache_dir) + 1);
	if (s3_put_from_file(s3->storage, buf.buf, rev_path) < 0) {
		ret = error("s3: failed to upload merged reverse index");
		goto out;
	}

	/* Activate the new pack in the embedded packed store. */
	if (!packfile_store_load_pack(s3->packed, idx_path, 1))
		die("s3: failed to load merged pack");

	/*
	 * Build an updated manifest: drop the split packs that were rolled up
	 * and append the new merged pack.
	 *
	 * We re-fetch the current manifest rather than operating on the
	 * in-memory copy so that any concurrent writes are not lost.
	 */
	s3_manifest_copy(s3_storage_get_manifest(s3->storage), &new_manifest);

	/*
	 * Remove the entries corresponding to the packs we merged. The
	 * manifest stores bare hex pack hashes (without the "pack-" prefix),
	 * while the local pack_name is "<hash>.pack". Strip the extension to
	 * get the bare hash for comparison.
	 *
	 * Mark matched entries by clearing their string to "" and remove
	 * them all in one pass at the end.
	 */
	for (size_t i = 0; i < split; i++) {
		const char *basename = pack_basename(packs[i]);

		strbuf_reset(&buf);
		strbuf_add(&buf, basename, strchrnul(basename, '.') - basename);

		unsorted_string_list_remove(&new_manifest.packs, buf.buf, 1);
	}

	/* Append the new merged pack. */
	merged_basename = strrchr(pack_path, '/') + 1;
	strbuf_reset(&buf);
	strbuf_add(&buf, merged_basename, strrchr(merged_basename, '.') - merged_basename);
	string_list_append(&new_manifest.packs, buf.buf);

	if (s3_storage_update_manifest(s3->storage, &new_manifest) < 0) {
		ret = -1;
		goto out;
	}

	ret = 0;

out:
	s3_manifest_release(&new_manifest);
	strbuf_release(&buf);
	free(pack_path);
	free(idx_path);
	free(rev_path);
	free(packs);
	return ret;
}

struct odb_source_s3 *odb_source_s3_new(struct object_database *odb,
					const char *payload,
					bool local)
{
	struct strbuf path = STRBUF_INIT;
	struct odb_source_s3 *s3;

	CALLOC_ARRAY(s3, 1);
	s3->storage = s3_storage_get(odb->repo, payload);
	odb_source_init(&s3->base, odb, ODB_SOURCE_S3,
			s3->storage->cache_dir, local);

	/*
	 * Create the embedded packed store and inhibit its automatic
	 * directory scan. We will populate it manually from the manifest.
	 */
	s3->packed = odb_source_packed_new(odb, s3->storage->cache_dir, local);
	s3->packed->initialized = true;

	safe_create_dir(odb->repo, s3->storage->cache_dir, 0);
	strbuf_addf(&path, "%s/packs", s3->storage->cache_dir);
	safe_create_dir(odb->repo, path.buf, 0);

	s3->base.free = odb_source_s3_free;
	s3->base.close = odb_source_s3_close;
	s3->base.reprepare = odb_source_s3_reprepare;
	s3->base.read_object_info = odb_source_s3_read_object_info;
	s3->base.read_object_stream = odb_source_s3_read_object_stream;
	s3->base.for_each_object = odb_source_s3_for_each_object;
	s3->base.count_objects = odb_source_s3_count_objects;
	s3->base.find_abbrev_len = odb_source_s3_find_abbrev_len;
	s3->base.freshen_object = odb_source_s3_freshen_object;
	s3->base.write_object = odb_source_s3_write_object;
	s3->base.write_object_stream = odb_source_s3_write_object_stream;
	s3->base.begin_transaction = odb_source_s3_begin_transaction;
	s3->base.read_alternates = odb_source_s3_read_alternates;
	s3->base.write_alternate = odb_source_s3_write_alternate;
	s3->base.get_packs = odb_source_s3_get_packs;
	s3->base.fsck = odb_source_s3_fsck;
	s3->base.optimize = odb_source_s3_optimize;
	s3->base.optimize_required = odb_source_s3_optimize_required;

	strbuf_release(&path);
	return s3;
}
