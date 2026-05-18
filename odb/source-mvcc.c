#define USE_THE_REPOSITORY_VARIABLE

#include "git-compat-util.h"
#include "csum-file.h"
#include "environment.h"
#include "gettext.h"
#include "git-zlib.h"
#include "hash.h"
#include "hex.h"
#include "mvcc.h"
#include "object-file.h"
#include "odb/source.h"
#include "odb/source-packed.h"
#include "odb/source-mvcc.h"
#include "odb/streaming.h"
#include "odb/transaction.h"
#include "pack.h"
#include "pack-revindex.h"
#include "packfile.h"
#include "path.h"
#include "repository.h"
#include "run-command.h"
#include "strbuf.h"
#include "string-list.h"
#include "strvec.h"
#include "tempfile.h"
#include "wrapper.h"
#include "write-or-die.h"

/*
 * Resolve the manifest and register every referenced pack with the
 * embedded packed source. Missing pack files are a hard error: the
 * orchestrator is responsible for ensuring they have been prefetched
 * before Git is invoked.
 */
static void odb_source_mvcc_prepare(struct odb_source *source,
				    enum odb_prepare_flags flags UNUSED)
{
	struct odb_source_mvcc *mvcc = odb_source_mvcc_downcast(source);
	const struct mvcc_manifest *manifest;
	struct strbuf path = STRBUF_INIT;
	struct strbuf name = STRBUF_INIT;

	if (mvcc->initialized)
		return;

	manifest = mvcc_storage_get_manifest(mvcc->storage);

	for (size_t i = 0; i < manifest->packs.nr; i++) {
		const char *hash = manifest->packs.items[i].string;

		strbuf_reset(&name);
		strbuf_addf(&name, "%s.idx", hash);

		strbuf_reset(&path);
		mvcc_storage_resolve_path(mvcc->storage, "pack", name.buf,
					  &path);

		if (access(path.buf, R_OK) < 0)
			die_errno(_("mvcc: pack index '%s' is not available locally; "
				    "ensure dependencies have been prefetched"),
				  path.buf);

		/* Register the pack with the embedded packed store. */
		if (!packfile_store_load_pack(mvcc->packed, path.buf, 1))
			die("mvcc: failed to load pack '%s'", hash);
	}

	mvcc->initialized = true;

	strbuf_release(&name);
	strbuf_release(&path);
}

static void odb_source_mvcc_free(struct odb_source *source)
{
	struct odb_source_mvcc *mvcc = odb_source_mvcc_downcast(source);

	odb_source_free(&mvcc->packed->base);
	mvcc_storage_release(mvcc->storage);
	odb_source_release(source);
	free(mvcc);
}

static void odb_source_mvcc_close(struct odb_source *source)
{
	struct odb_source_mvcc *mvcc = odb_source_mvcc_downcast(source);
	odb_source_close(&mvcc->packed->base);
}

static int odb_source_mvcc_read_object_info(struct odb_source *source,
					    const struct object_id *oid,
					    struct object_info *oi,
					    enum object_info_flags flags)
{
	struct odb_source_mvcc *mvcc = odb_source_mvcc_downcast(source);
	odb_source_mvcc_prepare(&mvcc->base, 0);

	/* Strip `OBJECT_INFO_SECOND_READ` so that we don't end up repreparing. */
	return odb_source_read_object_info(&mvcc->packed->base, oid, oi,
					   flags & ~OBJECT_INFO_SECOND_READ);
}

static int odb_source_mvcc_read_object_stream(struct odb_read_stream **out,
					      struct odb_source *source,
					      const struct object_id *oid)
{
	struct odb_source_mvcc *mvcc = odb_source_mvcc_downcast(source);
	odb_source_mvcc_prepare(&mvcc->base, 0);
	return odb_source_read_object_stream(out, &mvcc->packed->base, oid);
}

static int odb_source_mvcc_for_each_object(
	struct odb_source *source,
	const struct object_info *request,
	odb_for_each_object_cb cb,
	void *cb_data,
	const struct odb_for_each_object_options *opts)
{
	struct odb_source_mvcc *mvcc = odb_source_mvcc_downcast(source);
	odb_source_mvcc_prepare(&mvcc->base, 0);
	return odb_source_for_each_object(&mvcc->packed->base, request,
					  cb, cb_data, opts);
}

static int odb_source_mvcc_count_objects(struct odb_source *source,
					 enum odb_count_objects_flags flags,
					 unsigned long *out)
{
	struct odb_source_mvcc *mvcc = odb_source_mvcc_downcast(source);
	odb_source_mvcc_prepare(&mvcc->base, 0);
	return odb_source_count_objects(&mvcc->packed->base, flags, out);
}

static int odb_source_mvcc_find_abbrev_len(struct odb_source *source,
					   const struct object_id *oid,
					   unsigned min_len,
					   unsigned *out)
{
	struct odb_source_mvcc *mvcc = odb_source_mvcc_downcast(source);
	odb_source_mvcc_prepare(&mvcc->base, 0);
	return odb_source_find_abbrev_len(&mvcc->packed->base, oid,
					  min_len, out);
}

static int odb_source_mvcc_freshen_object(struct odb_source *source,
					  const struct object_id *oid,
					  const time_t *mtime)
{
	struct odb_source_mvcc *mvcc = odb_source_mvcc_downcast(source);
	odb_source_mvcc_prepare(&mvcc->base, 0);
	return odb_source_freshen_object(&mvcc->packed->base, oid, mtime);
}

struct mvcc_pending_object {
	struct object_id oid;
	enum object_type type;
	unsigned char *data;
	unsigned long len;
};

struct mvcc_pending_pack {
	char *pack_path;
	char *idx_path;
	char *rev_path;

	const char *pack_basename;
};

static void mvcc_pending_pack_release(struct mvcc_pending_pack *pending)
{
	free(pending->pack_path);
	free(pending->idx_path);
	free(pending->rev_path);
}

static int write_pending_to_pack(struct odb_source_mvcc *mvcc,
				 struct mvcc_pending_object *objects,
				 size_t objects_nr,
				 struct mvcc_pending_pack *out)
{
	struct repository *repo = mvcc->base.odb->repo;
	const struct git_hash_algo *algo = repo->hash_algo;
	struct strbuf packs_dir = STRBUF_INIT;
	char *pack_tmp_path = NULL, *pack_path = NULL, *idx_path = NULL, *rev_path = NULL;
	unsigned char pack_hash[GIT_MAX_RAWSZ];
	struct pack_idx_entry **idx_entries = NULL;
	struct pack_idx_option idx_opts;
	struct hashfile *f = NULL;
	struct tempfile *tempfile;
	int ret;

	mvcc_storage_write_path(mvcc->storage, "pack", NULL, &packs_dir);
	if (mkdir(packs_dir.buf, 0777) < 0 && errno != EEXIST) {
		ret = error_errno(_("mvcc: cannot create '%s'"), packs_dir.buf);
		goto out;
	}

	pack_tmp_path = xstrfmt("%s/tmp_pack_XXXXXX", packs_dir.buf);
	tempfile = xmks_tempfile(pack_tmp_path);
	f = hashfd(algo, get_tempfile_fd(tempfile), pack_tmp_path);

	CALLOC_ARRAY(idx_entries, objects_nr);
	for (size_t i = 0; i < objects_nr; i++)
		CALLOC_ARRAY(idx_entries[i], 1);

	write_pack_header(f, objects_nr);

	for (size_t i = 0; i < objects_nr; i++) {
		struct mvcc_pending_object *obj = &objects[i];
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
			ret = error("mvcc: deflate failed for object %s",
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

	pack_path = xstrfmt("%s/%s.pack", packs_dir.buf,
			    hash_to_hex_algop(pack_hash, algo));
	if (rename_tempfile(&tempfile, pack_path) < 0) {
		ret = error_errno("mvcc: cannot move packfile into place '%s'", pack_path);
		goto out;
	}

	reset_pack_idx_option(&idx_opts);
	idx_path = xstrfmt("%s/%s.idx", packs_dir.buf,
			   hash_to_hex_algop(pack_hash, algo));
	write_idx_file(repo, idx_path, idx_entries, objects_nr,
		       &idx_opts, pack_hash);

	rev_path = xstrfmt("%s/%s.rev", packs_dir.buf,
			   hash_to_hex_algop(pack_hash, algo));
	free(write_rev_file(repo, rev_path, idx_entries, objects_nr,
			    pack_hash, WRITE_REV));

	out->pack_path = pack_path;
	pack_path = NULL;
	out->idx_path = idx_path;
	idx_path = NULL;
	out->rev_path = rev_path;
	rev_path = NULL;
	out->pack_basename = strrchr(out->pack_path, '/') + 1;

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
	strbuf_release(&packs_dir);
	return ret;
}

static int write_objects(struct odb_source_mvcc *mvcc,
			 struct mvcc_pending_object *objects,
			 size_t objects_nr)
{
	struct mvcc_manifest new_manifest = MVCC_MANIFEST_INIT;
	struct mvcc_pending_pack pending_pack = { 0 };
	const char *hash_end;
	int ret = 0;

	if (!objects_nr)
		return 0;

	if (write_pending_to_pack(mvcc, objects, objects_nr, &pending_pack) < 0) {
		ret = -1;
		goto out;
	}

	mvcc_manifest_copy(mvcc_storage_get_manifest(mvcc->storage), &new_manifest);
	hash_end = strrchr(pending_pack.pack_basename, '.');
	string_list_append_nodup(&new_manifest.packs,
				 xstrndup(pending_pack.pack_basename,
					  hash_end - pending_pack.pack_basename));

	ret = mvcc_storage_activate_manifest(mvcc->storage, &new_manifest);
	if (ret < 0)
		goto out;

	if (!packfile_store_load_pack(mvcc->packed, pending_pack.idx_path, 1))
		die("mvcc: failed to activate newly written pack '%s'",
		    pending_pack.pack_basename);

out:
	mvcc_pending_pack_release(&pending_pack);
	mvcc_manifest_release(&new_manifest);
	return ret;
}

static int odb_source_mvcc_write_object(struct odb_source *source,
					const void *buf,
					unsigned long len,
					enum object_type type,
					const struct object_id *oid,
					const struct object_id *compat_oid UNUSED,
					const time_t *mtime UNUSED,
					enum odb_write_object_flags flags UNUSED)
{
	struct odb_source_mvcc *mvcc = odb_source_mvcc_downcast(source);
	struct mvcc_pending_object object = {
		.type = type,
		.len = len,
		.data = (unsigned char *)buf,
	};

	oidcpy(&object.oid, oid);

	return write_objects(mvcc, &object, 1);
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
			ret = error("mvcc: stream read error");
			goto out;
		}

		if (total_read + (size_t)bytes_read > len) {
			ret = error("mvcc: stream yielded more bytes than expected");
			goto out;
		}

		memcpy(data + total_read, buf, bytes_read);
		total_read += bytes_read;
	}

	if (total_read != len) {
		ret = error("mvcc: stream yielded fewer bytes than expected (%" PRIuMAX " of %" PRIuMAX ")",
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

static int odb_source_mvcc_write_object_stream(struct odb_source *source,
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
	ret = odb_source_mvcc_write_object(source, data, len, OBJ_BLOB, oid,
					   NULL, NULL, 0);

out:
	free(data);
	return ret;
}

struct odb_transaction_mvcc {
	struct odb_transaction base;
	struct odb_source_mvcc *mvcc;
	struct mvcc_manifest manifest;
	struct mvcc_pending_object *objects;
	size_t objects_nr, objects_alloc;
	struct mvcc_pending_pack *packs;
	size_t packs_nr, packs_alloc;
	struct strvec env;
};

static int odb_transaction_mvcc_commit(struct odb_transaction *base)
{
	struct odb_transaction_mvcc *tx = container_of(base, struct odb_transaction_mvcc, base);
	struct mvcc_manifest new_manifest = MVCC_MANIFEST_INIT;
	struct strbuf basename = STRBUF_INIT;
	int ret;

	if (!tx->objects_nr && !tx->packs_nr) {
		ret = 0;
		goto out;
	}

	/*
	 * If we have pending loose objects we pack them into a new pack first.
	 * This pack is then appended to the other packs that we're about to
	 * commit.
	 */
	if (tx->objects_nr) {
		struct mvcc_pending_pack objects_pack = { 0 };

		if (write_pending_to_pack(tx->mvcc, tx->objects, tx->objects_nr,
					  &objects_pack) < 0) {
			ret = -1;
			goto out;
		}

		ALLOC_GROW(tx->packs, tx->packs_nr + 1, tx->packs_alloc);
		tx->packs[tx->packs_nr++] = objects_pack;
	}

	mvcc_manifest_copy(mvcc_storage_get_manifest(tx->mvcc->storage),
			   &new_manifest);
	for (size_t i = 0; i < tx->packs_nr; i++) {
		strbuf_reset(&basename);
		strbuf_addstr(&basename, tx->packs[i].pack_basename);
		strbuf_strip_suffix(&basename, ".pack");
		string_list_append(&new_manifest.packs, basename.buf);
	}

	if (mvcc_storage_activate_manifest(tx->mvcc->storage, &new_manifest)) {
		ret = -1;
		goto out;
	}

	for (size_t i = 0; i < tx->packs_nr; i++)
		if (!packfile_store_load_pack(tx->mvcc->packed,
					      tx->packs[i].idx_path, 1))
			die("mvcc: failed to activate newly written pack '%s'",
			    tx->packs[i].pack_basename);

	ret = 0;

out:
	mvcc_manifest_release(&new_manifest);
	mvcc_manifest_release(&tx->manifest);
	strbuf_release(&basename);

	for (size_t i = 0; i < tx->objects_nr; i++)
		free(tx->objects[i].data);
	free(tx->objects);

	for (size_t i = 0; i < tx->packs_nr; i++)
		mvcc_pending_pack_release(&tx->packs[i]);
	free(tx->packs);
	strvec_clear(&tx->env);

	return ret;
}

static int odb_transaction_mvcc_write_object_stream(struct odb_transaction *base,
						    struct odb_write_stream *stream,
						    size_t len,
						    struct object_id *oid)
{
	struct odb_transaction_mvcc *tx = container_of(base, struct odb_transaction_mvcc, base);
	unsigned char *data;
	int ret;

	ret = read_write_stream(stream, len, &data);
	if (ret)
		return ret;

	ALLOC_GROW(tx->objects, tx->objects_nr + 1, tx->objects_alloc);
	tx->objects[tx->objects_nr].type = OBJ_BLOB;
	tx->objects[tx->objects_nr].data = data;
	tx->objects[tx->objects_nr].len = len;
	hash_object_file(tx->mvcc->base.odb->repo->hash_algo, data, len,
			 OBJ_BLOB, &tx->objects[tx->objects_nr].oid);
	oidcpy(oid, &tx->objects[tx->objects_nr].oid);

	tx->objects_nr++;

	return 0;
}

static char *read_pack_hash(struct repository *repo, int output_fd)
{
	char packname[GIT_MAX_HEXSZ + 6];
	const int len = repo->hash_algo->hexsz + 6;

	if (read_in_full(output_fd, packname, len) == len && packname[len-1] == '\n') {
		const char *name;

		packname[len-1] = 0;
		if (skip_prefix(packname, "pack\t", &name))
			return xstrfmt("%s", name);
		return NULL;
	}

	return NULL;
}

static int odb_transaction_mvcc_write_pack(struct odb_transaction *base, int fd,
					   struct odb_transaction_write_pack_opts *opts)
{
	struct odb_transaction_mvcc *tx = container_of(base, struct odb_transaction_mvcc, base);
	struct repository *repo = tx->mvcc->base.odb->repo;
	struct child_process child = CHILD_PROCESS_INIT;
	struct tempfile *tmp_pack_tempfile = NULL;
	struct strbuf tmp_pack_template = STRBUF_INIT;
	struct strbuf packs_dir = STRBUF_INIT;
	char *tmp_pack_path = NULL, *tmp_idx_path = NULL, *tmp_rev_path = NULL;
	char *pack_path = NULL, *idx_path = NULL, *rev_path = NULL;
	size_t base_len;
	char *hash = NULL;
	int ret = 0;

	mvcc_storage_write_path(tx->mvcc->storage, "pack", NULL, &packs_dir);
	if (mkdir(packs_dir.buf, 0777) < 0 && errno != EEXIST) {
		ret = error_errno(_("mvcc: cannot create '%s'"), packs_dir.buf);
		goto out;
	}

	/*
	 * Reserve a unique tempfile name so that concurrent transactions
	 * against the same destination directory do not clobber each other.
	 * We immediately delete the tempfile after creation so that
	 * index-pack can recreate it with O_CREAT|O_EXCL; the .idx and
	 * .rev paths are derived from the same unique base.
	 */
	strbuf_addf(&tmp_pack_template, "%s/tmp_pack_XXXXXX.pack",
		    packs_dir.buf);
	tmp_pack_tempfile = mks_tempfile_s(tmp_pack_template.buf, strlen(".pack"));
	if (!tmp_pack_tempfile) {
		ret = error_errno("mvcc: cannot create tempfile under '%s'",
				  packs_dir.buf);
		goto out;
	}
	tmp_pack_path = xstrdup(get_tempfile_path(tmp_pack_tempfile));
	delete_tempfile(&tmp_pack_tempfile);

	strvec_pushl(&child.args, "index-pack", "--stdin", tmp_pack_path, NULL);

	if (opts->shallow_file) {
		strvec_push(&child.args, "--shallow-file");
		strvec_push(&child.args, opts->shallow_file);
	}

	if (opts->fsck_objects)
		strvec_pushf(&child.args, "--strict%s", opts->fsck_msg_types ? opts->fsck_msg_types : "");
	if (!opts->use_thin_pack)
		strvec_push(&child.args, "--fix-thin");
	if (opts->max_pack_size)
		strvec_pushf(&child.args, "--max-input-size=%"PRIuMAX, (uintmax_t)opts->max_pack_size);
	strvec_push(&child.args, "--rev-index");

	child.out = -1;
	child.in = fd;
	child.err = opts->err_fd;
	child.git_cmd = 1;

	if (start_command(&child)) {
		opts->error_msg = "index-pack fork failed";
		ret = -1;
		goto out;
	}

	hash = read_pack_hash(repo, child.out);
	if (!hash) {
		ret = -1;
		goto out;
	}
	close(child.out);

	opts->index_pack_exit_status = finish_command(&child);
	if (opts->index_pack_exit_status &&
	    !(opts->check_self_contained_and_connected &&
	      opts->index_pack_exit_status == 1)) {
		opts->error_msg = "index-pack abnormal exit";
		ret = -1;
		goto out;
	}

	base_len = strlen(tmp_pack_path) - strlen(".pack");
	tmp_idx_path = xstrfmt("%.*s.idx", (int)base_len, tmp_pack_path);
	tmp_rev_path = xstrfmt("%.*s.rev", (int)base_len, tmp_pack_path);

	pack_path = xstrfmt("%s/%s.pack", packs_dir.buf, hash);
	idx_path = xstrfmt("%s/%s.idx", packs_dir.buf, hash);
	rev_path = xstrfmt("%s/%s.rev", packs_dir.buf, hash);

	if (rename(tmp_pack_path, pack_path) < 0) {
		ret = error_errno("mvcc: cannot rename '%s' -> '%s'",
				  tmp_pack_path, pack_path);
		goto out;
	}
	if (rename(tmp_idx_path, idx_path) < 0) {
		ret = error_errno("mvcc: cannot rename '%s' -> '%s'",
				  tmp_idx_path, idx_path);
		goto out;
	}
	if (rename(tmp_rev_path, rev_path) < 0) {
		ret = error_errno("mvcc: cannot rename '%s' -> '%s'",
				  tmp_rev_path, rev_path);
		goto out;
	}

	ALLOC_GROW(tx->packs, tx->packs_nr + 1, tx->packs_alloc);
	tx->packs[tx->packs_nr].pack_path = pack_path;
	tx->packs[tx->packs_nr].idx_path = idx_path;
	tx->packs[tx->packs_nr].rev_path = rev_path;
	tx->packs[tx->packs_nr].pack_basename = strrchr(tx->packs[tx->packs_nr].pack_path, '/') + 1;
	tx->packs_nr++;

	pack_path = NULL;
	idx_path = NULL;
	rev_path = NULL;

	/* Write the packfile to the transaction manifest. */
	string_list_append_nodup(&tx->manifest.packs, hash);
	hash = NULL;

out:
	delete_tempfile(&tmp_pack_tempfile);
	strbuf_release(&tmp_pack_template);
	strbuf_release(&packs_dir);
	free(tmp_pack_path);
	free(tmp_idx_path);
	free(tmp_rev_path);
	free(pack_path);
	free(idx_path);
	free(rev_path);
	free(hash);
	return ret;
}

static const char **odb_transaction_mvcc_env(struct odb_transaction *base)
{
	struct odb_transaction_mvcc *tx = container_of(base, struct odb_transaction_mvcc, base);
	char version_hex[GIT_SHA256_HEXSZ + 1];

	/*
	 * Write the in-flight manifest body to disk (without activating it)
	 * so that child processes can resolve it via GIT_MVCC_MANIFEST.
	 */
	mvcc_storage_write_manifest(tx->mvcc->storage, &tx->manifest, version_hex);
	strvec_pushf(&tx->env, "GIT_MVCC_MANIFEST=%s", version_hex);

	return tx->env.v;
}

static int odb_source_mvcc_begin_transaction(struct odb_source *source,
					     struct odb_transaction **out,
					     enum odb_transaction_flags flags UNUSED)
{
	struct mvcc_manifest manifest = MVCC_MANIFEST_INIT;
	struct odb_transaction_mvcc *tx;

	CALLOC_ARRAY(tx, 1);
	tx->mvcc = odb_source_mvcc_downcast(source);
	tx->base.source = source;
	tx->base.commit = odb_transaction_mvcc_commit;
	tx->base.write_object_stream = odb_transaction_mvcc_write_object_stream;
	tx->base.write_pack = odb_transaction_mvcc_write_pack;
	tx->base.env = odb_transaction_mvcc_env;
	tx->manifest = manifest;

	mvcc_manifest_copy(mvcc_storage_get_manifest(tx->mvcc->storage),
			   &tx->manifest);

	strvec_init(&tx->env);

	*out = &tx->base;
	return 0;
}

static int odb_source_mvcc_read_alternates(struct odb_source *source UNUSED,
					   struct strvec *out UNUSED)
{
	return 0;
}

static int odb_source_mvcc_write_alternate(struct odb_source *source UNUSED,
					   const char *alternate UNUSED)
{
	return error("mvcc source: alternates are not supported");
}

static int odb_source_mvcc_get_packs(struct odb_source *source,
				     struct packfile_list_entry **out)
{
	struct odb_source_mvcc *mvcc = odb_source_mvcc_downcast(source);
	odb_source_mvcc_prepare(&mvcc->base, 0);
	return odb_source_get_packs(&mvcc->packed->base, out);
}

static int odb_source_mvcc_fsck(struct odb_source *source UNUSED,
				struct odb_fsck_options *opts UNUSED)
{
	/* TODO: we don't implement fsck yet. */
	return 0;
}

/*
 * Packs whose on-disk size exceeds this threshold are excluded from
 * geometric compaction entirely. Once a pack grows past this size,
 * subsequent optimize calls leave it alone, so the long-term steady
 * state is one or more frozen large packs alongside a small tail of
 * eligible packs that participate in the geometric progression.
 */
#define MVCC_GEOMETRIC_BIG_PACK_BYTES (5LL * 1024 * 1024 * 1024)

static int mvcc_pack_cmp(const void *va, const void *vb)
{
	off_t a = (*(const struct packed_git **)va)->pack_size;
	off_t b = (*(const struct packed_git **)vb)->pack_size;
	return (a > b) - (a < b);
}

/*
 * Collect all packed_git pointers from the embedded packed store, sort them
 * in ascending order by on-disk pack size, and return a heap-allocated
 * array. The caller must free() the returned array. *nr_out receives the
 * count.
 *
 * Returns NULL (with *nr_out == 0) when there are no packs.
 */
static struct packed_git **mvcc_sorted_packs(struct odb_source_mvcc *mvcc,
					     size_t *nr_out)
{
	struct packfile_list_entry *e;
	struct packed_git **packs = NULL;
	size_t nr = 0, alloc = 0;

	for (e = packfile_store_get_packs(mvcc->packed); e; e = e->next) {
		ALLOC_GROW(packs, nr + 1, alloc);
		packs[nr++] = e->pack;
	}

	QSORT(packs, nr, mvcc_pack_cmp);

	*nr_out = nr;
	return packs;
}

/*
 * Partition a pack-size-ascending array into eligible and frozen subsets.
 * Returns the partition point: packs[0..eligible_nr-1] have
 * pack_size <= MVCC_GEOMETRIC_BIG_PACK_BYTES and may participate in
 * geometric compaction; packs[eligible_nr..nr-1] are too large and are
 * left untouched.
 */
static size_t mvcc_eligible_packs(struct packed_git **packs, size_t nr)
{
	for (size_t i = 0; i < nr; i++)
		if (packs[i]->pack_size > MVCC_GEOMETRIC_BIG_PACK_BYTES)
			return i;
	return nr;
}

/*
 * Determine how many of the smallest packs (in ascending pack-size order)
 * need to be rolled up to restore a geometric progression with a factor of 2.
 * Returns the split index: packs[0..split-1] must be merged.
 *
 * This implements the same algorithm as compute_pack_geometry_split() in
 * repack-geometry.c, adapted to operate on a plain array of packed_git
 * pointers and to weigh packs by on-disk size rather than object count.
 */
static size_t mvcc_compute_geometry_split(struct packed_git **packs, size_t nr)
{
	size_t i, split;
	uint64_t total = 0;

	if (!nr)
		return 0;

	/*
	 * Walk from the largest pack down and find the first consecutive pair
	 * that violates the geometric property.
	 */
	for (i = nr - 1; i > 0; i--) {
		uint64_t cur  = (uint64_t)packs[i]->pack_size;
		uint64_t prev = (uint64_t)packs[i - 1]->pack_size;

		if (unsigned_mult_overflows(2, prev))
			die(_("mvcc: pack '%s' too large for geometric progression"),
			    packs[i - 1]->pack_name);

		if (cur < 2 * prev)
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
		uint64_t cur = (uint64_t)packs[i]->pack_size;

		if (unsigned_add_overflows(total, cur))
			die(_("mvcc: pack '%s' too large to roll up"),
			    packs[i]->pack_name);
		total += cur;
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
		uint64_t cur = (uint64_t)packs[i]->pack_size;

		if (unsigned_mult_overflows(2, total))
			die(_("mvcc: pack '%s' too large to roll up"),
			    packs[i]->pack_name);

		if (cur < 2 * total) {
			if (unsigned_add_overflows(total, cur))
				die(_("mvcc: pack '%s' too large to roll up"),
				    packs[i]->pack_name);
			split++;
			total += cur;
		} else {
			break;
		}
	}

	return split;
}

static bool odb_source_mvcc_optimize_required(struct odb_source *source,
					      const struct odb_optimize_options *opts)
{
	struct odb_source_mvcc *mvcc = odb_source_mvcc_downcast(source);
	struct packed_git **packs;
	size_t split, nr, eligible_nr;

	switch (opts->strategy) {
	case ODB_OPTIMIZE_INCREMENTAL:
		/* No loose objects in the MVCC backend. */
		return false;
	case ODB_OPTIMIZE_GEOMETRIC:
		break;
	default:
		BUG("unknown optimize strategy '%d'", opts->strategy);
	}

	odb_source_mvcc_prepare(&mvcc->base, 0);
	packs = mvcc_sorted_packs(mvcc, &nr);
	eligible_nr = mvcc_eligible_packs(packs, nr);
	split = mvcc_compute_geometry_split(packs, eligible_nr);

	free(packs);
	return split > 0;
}

static int mvcc_merge_packs(struct odb_source_mvcc *mvcc,
			    struct packed_git **include_packs,
			    size_t include_nr,
			    struct packed_git **exclude_packs,
			    size_t exclude_nr,
			    char **pack_path_out,
			    char **idx_path_out,
			    char **rev_path_out)
{
	struct repository *repo = mvcc->base.odb->repo;
	const struct git_hash_algo *algo = repo->hash_algo;
	struct child_process cmd = CHILD_PROCESS_INIT;
	struct strbuf packs_dir = STRBUF_INIT;
	struct strbuf line = STRBUF_INIT;
	struct strbuf path = STRBUF_INIT;
	char *packtmp = NULL;
	char *pack_path = NULL, *idx_path = NULL, *rev_path = NULL;
	FILE *in = NULL, *out = NULL;
	int ret;

	mvcc_storage_write_path(mvcc->storage, "pack", NULL, &packs_dir);
	if (mkdir(packs_dir.buf, 0777) < 0 && errno != EEXIST) {
		ret = error_errno(_("mvcc: cannot create '%s'"), packs_dir.buf);
		goto out;
	}

	packtmp = xstrfmt("%s/tmp_compact_%"PRIuMAX, packs_dir.buf,
			  (uintmax_t)getpid());

	strvec_pushl(&cmd.args, "pack-objects", "--stdin-packs",
		     "--delta-base-offset", "--non-empty", "--quiet",
		     packtmp, NULL);
	/*
	 * Override the child's object directory so it sees the MVCC cache as
	 * a plain "files" ODB. This is hacky, but works around the limitation
	 * that git-pack-objects(1) does not otherwise work with the "mvcc"
	 * backend yet.
	 */
	strvec_pushf(&cmd.env, "GIT_OBJECT_DIRECTORY=%s",
		     mvcc->storage->cache_dir);
	cmd.git_cmd = 1;
	cmd.in = -1;
	cmd.out = -1;

	if (start_command(&cmd) < 0) {
		ret = error(_("mvcc: cannot start pack-objects"));
		goto out;
	}

	in = xfdopen(cmd.in, "w");
	for (size_t i = 0; i < include_nr; i++)
		fprintf(in, "%s\n", pack_basename(include_packs[i]));
	for (size_t i = 0; i < exclude_nr; i++)
		fprintf(in, "^%s\n", pack_basename(exclude_packs[i]));
	fclose(in);
	in = NULL;

	out = xfdopen(cmd.out, "r");
	if (strbuf_getline_lf(&line, out) == EOF) {
		/*
		 * --non-empty caused pack-objects to abort without
		 * producing a pack; there was nothing to compact.
		 */
		fclose(out);
		out = NULL;
		ret = finish_command(&cmd) ? -1 : 1;
		goto out;
	}
	if (line.len != algo->hexsz) {
		ret = error(_("mvcc: pack-objects produced malformed hash '%s'"),
			    line.buf);
		fclose(out);
		out = NULL;
		finish_command(&cmd);
		goto out;
	}
	fclose(out);
	out = NULL;

	if (finish_command(&cmd) < 0) {
		ret = error(_("mvcc: pack-objects failed"));
		goto out;
	}

	/*
	 * pack-objects wrote files at "<packtmp>-<hash>.{pack,idx,rev}". Move
	 * them into their content-addressed destinations.
	 */
	pack_path = xstrfmt("%s/%s.pack", packs_dir.buf, line.buf);
	idx_path = xstrfmt("%s/%s.idx", packs_dir.buf, line.buf);
	rev_path = xstrfmt("%s/%s.rev", packs_dir.buf, line.buf);

	strbuf_addf(&path, "%s-%s.pack", packtmp, line.buf);
	if (rename(path.buf, pack_path) < 0) {
		ret = error_errno(_("mvcc: cannot move '%s' to '%s'"),
				  path.buf, pack_path);
		goto out;
	}

	strbuf_reset(&path);
	strbuf_addf(&path, "%s-%s.idx", packtmp, line.buf);
	if (rename(path.buf, idx_path) < 0) {
		ret = error_errno(_("mvcc: cannot move '%s' to '%s'"),
				  path.buf, idx_path);
		goto out;
	}

	strbuf_reset(&path);
	strbuf_addf(&path, "%s-%s.rev", packtmp, line.buf);
	if (rename(path.buf, rev_path) < 0) {
		ret = error_errno(_("mvcc: cannot move '%s' to '%s'"),
				  path.buf, rev_path);
		goto out;
	}

	*pack_path_out = pack_path;
	pack_path = NULL;
	*idx_path_out = idx_path;
	idx_path = NULL;
	*rev_path_out = rev_path;
	rev_path = NULL;
	ret = 0;

out:
	if (in)
		fclose(in);
	if (out)
		fclose(out);
	free(pack_path);
	free(idx_path);
	free(rev_path);
	free(packtmp);
	strbuf_release(&packs_dir);
	strbuf_release(&line);
	strbuf_release(&path);
	child_process_clear(&cmd);
	return ret;
}

static int odb_source_mvcc_optimize(struct odb_source *source,
				    const struct odb_optimize_options *opts)
{
	struct odb_source_mvcc *mvcc = odb_source_mvcc_downcast(source);
	char *pack_path = NULL, *idx_path = NULL, *rev_path = NULL;
	struct mvcc_manifest new_manifest = MVCC_MANIFEST_INIT;
	struct packed_git **packs = NULL;
	struct strbuf buf = STRBUF_INIT;
	const char *merged_basename;
	size_t nr, eligible_nr, split;
	int ret;

	switch (opts->strategy) {
	case ODB_OPTIMIZE_INCREMENTAL:
		/* No loose objects in the MVCC backend; nothing to do. */
		return 0;
	case ODB_OPTIMIZE_GEOMETRIC:
		break;
	default:
		BUG("unknown optimize strategy '%d'", opts->strategy);
	}

	odb_source_mvcc_prepare(&mvcc->base, 0);
	packs = mvcc_sorted_packs(mvcc, &nr);
	eligible_nr = mvcc_eligible_packs(packs, nr);
	split = mvcc_compute_geometry_split(packs, eligible_nr);
	if (!split) {
		ret = 0;
		goto out; /* already a geometric progression among eligible packs */
	}

	/*
	 * Roll up packs[0..split-1]. Pass *all* surviving packs -- both the
	 * untouched-but-eligible packs[split..eligible_nr-1] and the frozen
	 * packs[eligible_nr..nr-1] -- as excludes so that pack-objects does
	 * not duplicate objects already present in any surviving pack.
	 */
	ret = mvcc_merge_packs(mvcc, packs, split, packs + split, nr - split,
			       &pack_path, &idx_path, &rev_path);
	if (ret < 0) {
		goto out;
	} else if (ret > 0) {
		/* pack-objects produced no pack; treat as a benign no-op. */
		ret = 0;
		goto out;
	}

	/*
	 * Compose the new manifest. We re-fetch the current manifest rather
	 * than operating on the in-memory copy so that any concurrent writes
	 * are not lost.
	 */
	mvcc_manifest_copy(mvcc_storage_get_manifest(mvcc->storage),
			   &new_manifest);

	/*
	 * Remove the entries corresponding to the packs we merged. The
	 * manifest stores bare hex pack hashes (without the "pack-" prefix),
	 * while the local pack_name is "<hash>.pack". Strip the extension to
	 * get the bare hash for comparison.
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

	if (mvcc_storage_activate_manifest(mvcc->storage, &new_manifest) < 0) {
		ret = -1;
		goto out;
	}

	/* Activate the new pack in the embedded packed store. */
	if (!packfile_store_load_pack(mvcc->packed, idx_path, 1))
		die("mvcc: failed to load merged pack");

	ret = 0;

out:
	mvcc_manifest_release(&new_manifest);
	strbuf_release(&buf);
	free(pack_path);
	free(idx_path);
	free(rev_path);
	free(packs);
	return ret;
}

struct odb_source_mvcc *odb_source_mvcc_new(struct object_database *odb,
					    const char *payload UNUSED,
					    bool local)
{
	struct odb_source_mvcc *mvcc;

	CALLOC_ARRAY(mvcc, 1);
	mvcc->storage = mvcc_storage_get(odb->repo->commondir);
	odb_source_init(&mvcc->base, odb, ODB_SOURCE_MVCC,
			mvcc->storage->cache_dir, local);

	/*
	 * Create the embedded packed store and inhibit its automatic
	 * directory scan. We will populate it manually from the manifest.
	 */
	mvcc->packed = odb_source_packed_new(odb, mvcc->storage->cache_dir, local);
	mvcc->packed->initialized = true;

	mvcc->base.free = odb_source_mvcc_free;
	mvcc->base.close = odb_source_mvcc_close;
	mvcc->base.prepare = odb_source_mvcc_prepare;
	mvcc->base.read_object_info = odb_source_mvcc_read_object_info;
	mvcc->base.read_object_stream = odb_source_mvcc_read_object_stream;
	mvcc->base.for_each_object = odb_source_mvcc_for_each_object;
	mvcc->base.count_objects = odb_source_mvcc_count_objects;
	mvcc->base.find_abbrev_len = odb_source_mvcc_find_abbrev_len;
	mvcc->base.freshen_object = odb_source_mvcc_freshen_object;
	mvcc->base.write_object = odb_source_mvcc_write_object;
	mvcc->base.write_object_stream = odb_source_mvcc_write_object_stream;
	mvcc->base.begin_transaction = odb_source_mvcc_begin_transaction;
	mvcc->base.read_alternates = odb_source_mvcc_read_alternates;
	mvcc->base.write_alternate = odb_source_mvcc_write_alternate;
	mvcc->base.get_packs = odb_source_mvcc_get_packs;
	mvcc->base.fsck = odb_source_mvcc_fsck;
	mvcc->base.optimize = odb_source_mvcc_optimize;
	mvcc->base.optimize_required = odb_source_mvcc_optimize_required;

	return mvcc;
}
