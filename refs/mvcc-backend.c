#include "git-compat-util.h"
#include "dir.h"
#include "gettext.h"
#include "hash.h"
#include "hex.h"
#include "iterator.h"
#include "mvcc.h"
#include "object.h"
#include "parse.h"
#include "path.h"
#include "refs.h"
#include "refs/refs-internal.h"
#include "reftable/reftable-basics.h"
#include "reftable/reftable-blocksource.h"
#include "reftable/reftable-error.h"
#include "reftable/reftable-iterator.h"
#include "reftable/reftable-merged.h"
#include "reftable/reftable-record.h"
#include "reftable/reftable-table.h"
#include "reftable/reftable-writer.h"
#include "strbuf.h"
#include "string-list.h"
#include "tempfile.h"
#include "wrapper.h"
#include "write-or-die.h"

/*
 * The MVCC-backed reference store uses the reftable library directly,
 * bypassing reftable_stack: the manifest's `r:` entries are themselves the
 * authoritative active set, so no local "tables.list" file is needed. Each
 * reftable file is content-addressed: the on-disk filename is
 * "<sha256>.ref" where <sha256> is the hex hash of the file's contents.
 *
 * Tables live under "reftables/" of the MVCC cache directory.
 *
 * Reflog and worktree support are intentionally omitted.
 */
struct mvcc_ref_store {
	struct ref_store base;
	struct mvcc_storage *storage;
	struct reftable_write_options write_options;
	enum reftable_hash hash_id;
	unsigned int store_flags;

	/*
	 * The materialised view of the manifest's `r:` entries. Lazily built
	 * from the manifest by `mvcc_load_view`, freed by `mvcc_release_view`.
	 * `view_dirty` indicates that the cached view is potentially stale
	 * (typically because we just performed a write ourselves) and should
	 * be rebuilt before the next read.
	 */
	struct reftable_table **tables;
	size_t tables_nr;
	struct reftable_merged_table *merged;
	bool view_dirty;
};

extern struct ref_storage_be refs_be_mvcc;

static struct mvcc_ref_store *mvcc_be_downcast(struct ref_store *ref_store,
					       unsigned int required_flags,
					       const char *caller)
{
	struct mvcc_ref_store *mvcc;

	if (ref_store->be != &refs_be_mvcc)
		BUG("ref_store is type \"%s\" not \"mvcc\" in %s",
		    ref_store->be->name, caller);

	mvcc = (struct mvcc_ref_store *)ref_store;

	if ((mvcc->store_flags & required_flags) != required_flags)
		BUG("operation %s requires abilities 0x%x, but only have 0x%x",
		    caller, required_flags, mvcc->store_flags);

	return mvcc;
}

struct mvcc_fd_writer {
	int fd;
};

static ssize_t mvcc_fd_write(void *arg, const void *data, size_t sz)
{
	struct mvcc_fd_writer *fw = arg;
	const char *p = data;
	size_t total = sz;

	while (sz) {
		ssize_t n = xwrite(fw->fd, p, sz);
		if (n < 0) {
			if (errno == EINTR)
				continue;
			return -1;
		}
		p += n;
		sz -= n;
	}

	return total;
}

static int mvcc_fd_flush(void *arg)
{
	struct mvcc_fd_writer *fw = arg;
	return fsync(fw->fd);
}

static void mvcc_release_view(struct mvcc_ref_store *mvcc)
{
	reftable_merged_table_free(mvcc->merged);
	mvcc->merged = NULL;

	for (size_t i = 0; i < mvcc->tables_nr; i++)
		reftable_table_decref(mvcc->tables[i]);
	FREE_AND_NULL(mvcc->tables);
	mvcc->tables_nr = 0;

	mvcc->view_dirty = true;
}

/*
 * Build a fresh merged-table view of the manifest's `r:` entries. Verifies
 * that every referenced reftable is available locally and opens each as a
 * block source. Missing tables are a hard error: the orchestrator is
 * responsible for prefetching them.
 */
static int mvcc_load_view(struct mvcc_ref_store *mvcc)
{
	const struct mvcc_manifest *manifest;
	struct reftable_table **tables = NULL;
	size_t tables_nr = 0;
	struct strbuf path = STRBUF_INIT;
	struct strbuf name = STRBUF_INIT;
	int ret = 0;

	if (!mvcc->view_dirty)
		return 0;

	mvcc_release_view(mvcc);

	manifest = mvcc_storage_get_manifest(mvcc->storage);
	if (manifest->reftables.nr)
		ALLOC_ARRAY(tables, manifest->reftables.nr);

	for (size_t i = 0; i < manifest->reftables.nr; i++) {
		const char *hex = manifest->reftables.items[i].string;
		struct reftable_block_source src = { 0 };

		strbuf_reset(&name);
		strbuf_addf(&name, "%s.ref", hex);

		strbuf_reset(&path);
		mvcc_storage_resolve_path(mvcc->storage, "reftables", name.buf,
					  &path);

		if (access(path.buf, R_OK) < 0) {
			ret = error_errno(_("mvcc: reftable '%s' is not available locally; "
					    "ensure dependencies have been prefetched"),
					  path.buf);
			goto out;
		}

		ret = reftable_block_source_from_file(&src, path.buf);
		if (ret < 0) {
			ret = error("mvcc: cannot open '%s' as block source: %s",
				    path.buf, reftable_error_str(ret));
			goto out;
		}

		ret = reftable_table_new(&tables[tables_nr], &src, name.buf);
		if (ret < 0) {
			ret = error("mvcc: cannot read reftable: %s",
				    reftable_error_str(ret));
			goto out;
		}

		tables_nr++;
	}

	ret = reftable_merged_table_new(&mvcc->merged, tables, tables_nr,
					mvcc->hash_id);
	if (ret < 0) {
		ret = error("mvcc: cannot build merged view: %s",
			    reftable_error_str(ret));
		goto out;
	}

	mvcc->view_dirty = false;
	mvcc->tables = tables;
	mvcc->tables_nr = tables_nr;
	tables = NULL;
	tables_nr = 0;

out:
	for (size_t i = 0; i < tables_nr; i++)
		reftable_table_decref(tables[i]);
	free(tables);

	strbuf_release(&path);
	strbuf_release(&name);
	return ret;
}

static uint64_t mvcc_next_update_index(struct mvcc_ref_store *mvcc)
{
	if (mvcc->tables_nr == 0)
		return 1;
	return reftable_table_max_update_index(mvcc->tables[mvcc->tables_nr - 1]) + 1;
}

/*
 * Write a single new reftable to disk, rename it to its content-addressed
 * name, build a new manifest containing the updated `r:` entries, and
 * activate the new manifest atomically. If the writer produces no records
 * (empty table), return 0 without writing anything.
 *
 * The new `r:` entry replaces the half-open range
 * [replace_start, replace_end) in the current manifest. Use
 * replace_start == replace_end == tables_nr to append; use
 * replace_start == 0 and replace_end == tables_nr to compact everything
 * down to a single table; use any other half-open range for a partial
 * (geometric) compaction.
 */
static int mvcc_install_table(struct mvcc_ref_store *mvcc,
			      int (*write_table)(struct reftable_writer *, void *),
			      void *cb_data,
			      uint64_t update_index_min,
			      int64_t update_index_max,
			      size_t replace_start,
			      size_t replace_end)
{
	const struct mvcc_manifest *current;
	struct mvcc_manifest new_manifest = MVCC_MANIFEST_INIT;
	struct mvcc_fd_writer fd_writer = { 0 };
	struct reftable_writer *writer = NULL;
	struct strbuf reftables_dir = STRBUF_INIT;
	struct strbuf temp_template = STRBUF_INIT;
	struct strbuf final_path = STRBUF_INIT;
	struct tempfile *tempfile = NULL;
	char hex[GIT_SHA256_HEXSZ + 1];
	int ret;

	mvcc_storage_write_path(mvcc->storage, "reftables", NULL,
				&reftables_dir);
	if (mkdir(reftables_dir.buf, 0777) < 0 && errno != EEXIST) {
		ret = error_errno(_("mvcc: cannot create '%s'"),
				  reftables_dir.buf);
		goto out;
	}

	strbuf_addf(&temp_template, "%s/tmp_table_XXXXXX", reftables_dir.buf);
	tempfile = xmks_tempfile(temp_template.buf);
	fd_writer.fd = get_tempfile_fd(tempfile);

	ret = reftable_writer_new(&writer, mvcc_fd_write, mvcc_fd_flush,
				  &fd_writer, &mvcc->write_options);
	if (ret < 0) {
		ret = error("mvcc: cannot create reftable writer: %s",
			    reftable_error_str(ret));
		goto out;
	}

	ret = reftable_writer_set_limits(writer, update_index_min,
					 update_index_max);
	if (ret < 0) {
		ret = error("mvcc: cannot set reftable limits: %s",
			    reftable_error_str(ret));
		goto out;
	}

	ret = write_table(writer, cb_data);
	if (ret < 0)
		goto out;

	ret = reftable_writer_close(writer);
	if (ret == REFTABLE_EMPTY_TABLE_ERROR) {
		/* Nothing to publish; the writer produced no records. */
		ret = 0;
		goto out;
	}
	if (ret < 0) {
		ret = error("mvcc: cannot close reftable writer: %s",
			    reftable_error_str(ret));
		goto out;
	}

	if (close_tempfile_gently(tempfile) < 0) {
		ret = error_errno("mvcc: cannot close '%s'",
				  get_tempfile_path(tempfile));
		goto out;
	}

	if (mvcc_sha256_file_hex(get_tempfile_path(tempfile), hex) < 0) {
		ret = -1;
		goto out;
	}

	strbuf_addf(&final_path, "%s/%s.ref", reftables_dir.buf, hex);
	if (rename_tempfile(&tempfile, final_path.buf) < 0) {
		ret = error_errno("mvcc: cannot rename '%s' -> '%s'",
				  get_tempfile_path(tempfile),
				  final_path.buf);
		goto out;
	}

	/*
	 * Build the new manifest: keep the `r:` entries before
	 * `replace_start`, insert the new hex, then keep the entries
	 * from `replace_end` onwards.
	 */
	current = mvcc_storage_get_manifest(mvcc->storage);
	mvcc_manifest_copy(current, &new_manifest);
	string_list_clear(&new_manifest.reftables, 0);

	for (size_t i = 0; i < replace_start && i < current->reftables.nr; i++)
		string_list_append(&new_manifest.reftables,
				   current->reftables.items[i].string);
	string_list_append(&new_manifest.reftables, hex);
	for (size_t i = replace_end; i < current->reftables.nr; i++)
		string_list_append(&new_manifest.reftables,
				   current->reftables.items[i].string);

	ret = mvcc_storage_activate_manifest(mvcc->storage, &new_manifest);
	if (ret < 0) {
		ret = error("mvcc: failed updating manifest");
		goto out;
	}

	mvcc->view_dirty = true;

out:
	if (writer)
		reftable_writer_free(writer);
	delete_tempfile(&tempfile);
	mvcc_manifest_release(&new_manifest);
	strbuf_release(&reftables_dir);
	strbuf_release(&temp_template);
	strbuf_release(&final_path);
	return ret;
}

struct mvcc_ref_iterator {
	struct ref_iterator base;
	struct mvcc_ref_store *refs;
	struct reftable_iterator iter;
	struct reftable_ref_record ref;
	struct object_id oid;
	struct object_id peeled_oid;

	char *prefix;
	size_t prefix_len;
	char **exclude_patterns;
	size_t exclude_patterns_index;
	size_t exclude_patterns_strlen;
	unsigned int flags;
	int err;
};

static int should_exclude_current_ref(struct mvcc_ref_iterator *iter)
{
	while (iter->exclude_patterns[iter->exclude_patterns_index]) {
		const char *pattern = iter->exclude_patterns[iter->exclude_patterns_index];
		char *ref_after_pattern;
		int cmp;

		if (!iter->exclude_patterns_strlen)
			iter->exclude_patterns_strlen = strlen(pattern);

		cmp = strncmp(iter->ref.refname, pattern,
			      iter->exclude_patterns_strlen);
		if (cmp > 0) {
			iter->exclude_patterns_index++;
			iter->exclude_patterns_strlen = 0;
			continue;
		}
		if (cmp < 0)
			return 0;

		ref_after_pattern = xstrfmt("%s%c", pattern, 0xff);
		iter->err = reftable_iterator_seek_ref(&iter->iter, ref_after_pattern);
		iter->exclude_patterns_index++;
		iter->exclude_patterns_strlen = 0;

		free(ref_after_pattern);
		return 1;
	}

	return 0;
}

static int mvcc_ref_iterator_advance(struct ref_iterator *ref_iterator)
{
	struct mvcc_ref_iterator *iter = (struct mvcc_ref_iterator *)ref_iterator;
	struct mvcc_ref_store *refs = iter->refs;
	const char *referent = NULL;

	while (!iter->err) {
		int flags = 0;

		iter->err = reftable_iterator_next_ref(&iter->iter, &iter->ref);
		if (iter->err)
			break;

		/*
		 * The merged_table iterator returns deletion tombstones too;
		 * we filter them out here. (`reftable_stack` sets an internal
		 * `suppress_deletions` flag for the same purpose.)
		 */
		if (iter->ref.value_type == REFTABLE_REF_DELETION)
			continue;

		if (!starts_with(iter->ref.refname, "refs/") &&
		    !(iter->flags & REFS_FOR_EACH_INCLUDE_ROOT_REFS &&
		      is_root_ref(iter->ref.refname))) {
			continue;
		}

		if (iter->prefix_len &&
		    strncmp(iter->prefix, iter->ref.refname, iter->prefix_len)) {
			iter->err = 1;
			break;
		}

		if (iter->exclude_patterns && should_exclude_current_ref(iter))
			continue;

		switch (iter->ref.value_type) {
		case REFTABLE_REF_VAL1:
			oidread(&iter->oid, iter->ref.value.val1,
				refs->base.repo->hash_algo);
			break;
		case REFTABLE_REF_VAL2:
			oidread(&iter->oid, iter->ref.value.val2.value,
				refs->base.repo->hash_algo);
			oidread(&iter->peeled_oid, iter->ref.value.val2.target_value,
				refs->base.repo->hash_algo);
			break;
		case REFTABLE_REF_SYMREF:
			referent = refs_resolve_ref_unsafe(&iter->refs->base,
							   iter->ref.refname,
							   RESOLVE_REF_READING,
							   &iter->oid, &flags);
			if (!referent)
				oidclr(&iter->oid, refs->base.repo->hash_algo);
			break;
		default:
			BUG("unhandled reference value type %d", iter->ref.value_type);
		}

		if (is_null_oid(&iter->oid))
			flags |= REF_ISBROKEN;

		if (check_refname_format(iter->ref.refname, REFNAME_ALLOW_ONELEVEL)) {
			if (!refname_is_safe(iter->ref.refname))
				die(_("refname is dangerous: %s"), iter->ref.refname);
			oidclr(&iter->oid, refs->base.repo->hash_algo);
			flags |= REF_BAD_NAME | REF_ISBROKEN;
		}

		if (iter->flags & REFS_FOR_EACH_OMIT_DANGLING_SYMREFS &&
		    flags & REF_ISSYMREF &&
		    flags & REF_ISBROKEN)
			continue;

		if (!(iter->flags & REFS_FOR_EACH_INCLUDE_BROKEN) &&
		    !ref_resolves_to_object(iter->ref.refname, refs->base.repo,
					    &iter->oid, flags))
				continue;

		memset(&iter->base.ref, 0, sizeof(iter->base.ref));
		iter->base.ref.name = iter->ref.refname;
		iter->base.ref.target = referent;
		iter->base.ref.oid = &iter->oid;
		if (iter->ref.value_type == REFTABLE_REF_VAL2)
			iter->base.ref.peeled_oid = &iter->peeled_oid;
		iter->base.ref.flags = flags;

		break;
	}

	if (iter->err > 0)
		return ITER_DONE;
	if (iter->err < 0)
		return ITER_ERROR;
	return ITER_OK;
}

static int mvcc_ref_iterator_seek(struct ref_iterator *ref_iterator,
				  const char *refname, unsigned int flags)
{
	struct mvcc_ref_iterator *iter = (struct mvcc_ref_iterator *)ref_iterator;

	FREE_AND_NULL(iter->prefix);
	iter->prefix_len = 0;

	if (flags & REF_ITERATOR_SEEK_SET_PREFIX) {
		iter->prefix = xstrdup_or_null(refname);
		iter->prefix_len = refname ? strlen(refname) : 0;
	}
	iter->err = reftable_iterator_seek_ref(&iter->iter, refname);

	return iter->err;
}

static void mvcc_ref_iterator_release(struct ref_iterator *ref_iterator)
{
	struct mvcc_ref_iterator *iter = (struct mvcc_ref_iterator *)ref_iterator;

	reftable_ref_record_release(&iter->ref);
	reftable_iterator_destroy(&iter->iter);

	if (iter->exclude_patterns) {
		for (size_t i = 0; iter->exclude_patterns[i]; i++)
			free(iter->exclude_patterns[i]);
		free(iter->exclude_patterns);
	}

	free(iter->prefix);
}

static struct ref_iterator_vtable mvcc_ref_iterator_vtable = {
	.advance = mvcc_ref_iterator_advance,
	.seek = mvcc_ref_iterator_seek,
	.release = mvcc_ref_iterator_release,
};

static int qsort_strcmp(const void *va, const void *vb)
{
	const char *a = *(const char **)va;
	const char *b = *(const char **)vb;
	return strcmp(a, b);
}

static char **filter_exclude_patterns(const char **exclude_patterns)
{
	size_t filtered_size = 0, filtered_alloc = 0;
	char **filtered = NULL;

	if (!exclude_patterns)
		return NULL;

	for (size_t i = 0; ; i++) {
		const char *exclude_pattern = exclude_patterns[i];
		int has_glob = 0;

		if (!exclude_pattern)
			break;

		for (const char *p = exclude_pattern; *p; p++) {
			has_glob = is_glob_special(*p);
			if (has_glob)
				break;
		}
		if (has_glob)
			continue;

		ALLOC_GROW(filtered, filtered_size + 1, filtered_alloc);
		filtered[filtered_size++] = xstrdup(exclude_pattern);
	}

	if (filtered_size) {
		QSORT(filtered, filtered_size, qsort_strcmp);
		ALLOC_GROW(filtered, filtered_size + 1, filtered_alloc);
		filtered[filtered_size++] = NULL;
	}

	return filtered;
}

static struct ref_iterator *mvcc_be_iterator_begin(struct ref_store *ref_store,
						   const char *prefix,
						   const char **exclude_patterns,
						   unsigned int flags)
{
	struct mvcc_ref_iterator *iter;
	struct mvcc_ref_store *refs;
	unsigned int required_flags = REF_STORE_READ;
	int ret;

	if (!(flags & REFS_FOR_EACH_INCLUDE_BROKEN))
		required_flags |= REF_STORE_ODB;
	refs = mvcc_be_downcast(ref_store, required_flags, "ref_iterator_begin");

	iter = xcalloc(1, sizeof(*iter));
	base_ref_iterator_init(&iter->base, &mvcc_ref_iterator_vtable);
	iter->base.ref.oid = &iter->oid;
	iter->flags = flags;
	iter->refs = refs;
	iter->exclude_patterns = filter_exclude_patterns(exclude_patterns);

	if (mvcc_load_view(refs) < 0) {
		ret = -1;
		goto done;
	}

	ret = reftable_merged_table_init_ref_iterator(refs->merged, &iter->iter);
	if (ret)
		goto done;

	ret = mvcc_ref_iterator_seek(&iter->base, prefix,
				     REF_ITERATOR_SEEK_SET_PREFIX);
	if (ret)
		goto done;

done:
	iter->err = ret;
	return &iter->base;
}

/*
 * Look up `refname` in the merged reftable view. Returns 0 on success,
 * positive when the reference is not found (or shadowed by a tombstone),
 * negative on error.
 */
static int mvcc_read_ref(struct mvcc_ref_store *mvcc, const char *refname,
			 struct object_id *oid, struct strbuf *referent,
			 unsigned int *type)
{
	struct reftable_iterator iter = { 0 };
	struct reftable_ref_record ref = { 0 };
	int ret;

	if (!mvcc->merged)
		return 1;

	ret = reftable_merged_table_init_ref_iterator(mvcc->merged, &iter);
	if (ret)
		goto done;

	ret = reftable_iterator_seek_ref(&iter, refname);
	if (ret)
		goto done;

	ret = reftable_iterator_next_ref(&iter, &ref);
	if (ret)
		goto done;

	if (strcmp(ref.refname, refname)) {
		ret = 1;
		goto done;
	}

	switch (ref.value_type) {
	case REFTABLE_REF_DELETION:
		ret = 1;
		goto done;
	case REFTABLE_REF_SYMREF:
		strbuf_reset(referent);
		strbuf_addstr(referent, ref.value.symref);
		*type |= REF_ISSYMREF;
		break;
	case REFTABLE_REF_VAL1:
	case REFTABLE_REF_VAL2: {
		unsigned int hash_id;

		switch (mvcc->hash_id) {
		case REFTABLE_HASH_SHA1:
			hash_id = GIT_HASH_SHA1;
			break;
		case REFTABLE_HASH_SHA256:
			hash_id = GIT_HASH_SHA256;
			break;
		default:
			BUG("unhandled hash ID %d", mvcc->hash_id);
		}
		oidread(oid, reftable_ref_record_val1(&ref),
			&hash_algos[hash_id]);
		break;
	}
	default:
		BUG("unhandled reference value type %d", ref.value_type);
	}

done:
	assert(ret != REFTABLE_API_ERROR);
	reftable_ref_record_release(&ref);
	reftable_iterator_destroy(&iter);
	return ret;
}

static int mvcc_be_read_raw_ref(struct ref_store *ref_store, const char *refname,
				struct object_id *oid, struct strbuf *referent,
				unsigned int *type, int *failure_errno)
{
	struct mvcc_ref_store *mvcc =
		mvcc_be_downcast(ref_store, REF_STORE_READ, "read_raw_ref");
	int ret;

	if (mvcc_load_view(mvcc) < 0) {
		*failure_errno = ENOENT;
		return -1;
	}

	ret = mvcc_read_ref(mvcc, refname, oid, referent, type);
	if (ret < 0)
		return ret;
	if (ret > 0) {
		*failure_errno = ENOENT;
		return -1;
	}
	return 0;
}

static int mvcc_be_read_symbolic_ref(struct ref_store *ref_store,
				     const char *refname,
				     struct strbuf *referent)
{
	struct mvcc_ref_store *mvcc = mvcc_be_downcast(ref_store, REF_STORE_READ, "read_symbolic_ref");
	struct object_id oid;
	unsigned int type = 0;
	int ret;

	if (mvcc_load_view(mvcc) < 0)
		return -1;

	ret = mvcc_read_ref(mvcc, refname, &oid, referent, &type);
	if (ret)
		return ret < 0 ? ret : -1;
	if (type & REF_ISSYMREF)
		return 0;
	return NOT_A_SYMREF;
}

struct mvcc_transaction_update {
	struct ref_update *update;
	struct object_id current_oid;
};

struct mvcc_write_transaction_arg {
	struct mvcc_ref_store *refs;
	struct mvcc_transaction_update *updates;
	size_t updates_nr;
	size_t updates_alloc;
	uint64_t ts;
};

struct mvcc_transaction_data {
	struct mvcc_write_transaction_arg arg;
};

static enum ref_transaction_error mvcc_prepare_single_update(struct mvcc_ref_store *refs,
							     struct mvcc_write_transaction_arg *arg,
							     struct ref_transaction *transaction,
							     struct ref_update *u,
							     size_t update_idx,
							     struct string_list *refnames_to_check,
							     struct strbuf *referent,
							     struct strbuf *err)
{
	enum ref_transaction_error ret = 0;
	struct object_id current_oid = { 0 };

	/* Reflog is unsupported; quietly drop the update. */
	if (u->flags & REF_LOG_ONLY)
		return 0;

	if ((u->flags & REF_HAVE_NEW) && !is_null_oid(&u->new_oid) &&
	    !(u->flags & REF_SKIP_OID_VERIFICATION)) {
		struct object *o = parse_object(refs->base.repo, &u->new_oid);
		if (!o) {
			strbuf_addf(err,
				    _("trying to write ref '%s' with nonexistent object %s"),
				    u->refname, oid_to_hex(&u->new_oid));
			return REF_TRANSACTION_ERROR_INVALID_NEW_VALUE;
		}

		if (o->type != OBJ_COMMIT && is_branch(u->refname)) {
			strbuf_addf(err,
				    _("trying to write non-commit object %s to branch '%s'"),
				    oid_to_hex(&u->new_oid), u->refname);
			return REF_TRANSACTION_ERROR_INVALID_NEW_VALUE;
		}
	}

	ret = mvcc_read_ref(refs, u->refname, &current_oid, referent, &u->type);
	if (ret < 0)
		return REF_TRANSACTION_ERROR_GENERIC;
	if (ret > 0 && !ref_update_expects_existing_old_ref(u)) {
		struct string_list_item *item;

		item = string_list_append(refnames_to_check, u->refname);
		item->util = xmalloc(sizeof(update_idx));
		memcpy(item->util, &update_idx, sizeof(update_idx));

		if ((u->flags & REF_HAVE_NEW) && !ref_update_has_null_new_value(u)) {
			ALLOC_GROW(arg->updates, arg->updates_nr + 1,
				   arg->updates_alloc);
			arg->updates[arg->updates_nr].update = u;
			oidcpy(&arg->updates[arg->updates_nr].current_oid,
			       &current_oid);
			u->backend_data = &arg->updates[arg->updates_nr++];
		}

		return 0;
	}
	if (ret > 0) {
		strbuf_addf(err, _("cannot lock ref '%s': "
				   "unable to resolve reference '%s'"),
			    ref_update_original_update_refname(u), u->refname);
		return REF_TRANSACTION_ERROR_NONEXISTENT_REF;
	}

	if (u->type & REF_ISSYMREF) {
		const char *resolved = refs_resolve_ref_unsafe(&refs->base,
							       u->refname, 0,
							       &current_oid, NULL);

		if (u->flags & REF_NO_DEREF) {
			if (u->flags & REF_HAVE_OLD && !resolved) {
				strbuf_addf(err, _("cannot lock ref '%s': "
						   "error reading reference"),
					    u->refname);
				return REF_TRANSACTION_ERROR_GENERIC;
			}
		} else {
			struct ref_update *new_update;

			if (string_list_has_string(&transaction->refnames,
						   referent->buf)) {
				strbuf_addf(err,
					    _("multiple updates for '%s' (including one "
					      "via symref '%s') are not allowed"),
					    referent->buf, u->refname);
				return REF_TRANSACTION_ERROR_NAME_CONFLICT;
			}

			new_update = ref_transaction_add_update(
				transaction, referent->buf, u->flags,
				u->new_target ? NULL : &u->new_oid,
				u->old_target ? NULL : &u->old_oid,
				u->new_target, u->old_target,
				u->committer_info, u->msg);
			new_update->parent_update = u;

			/*
			 * Mark the symref update as LOG_ONLY, which will cause
			 * it to be dropped on commit.
			 */
			u->flags |= REF_LOG_ONLY | REF_NO_DEREF;
			return 0;
		}
	}

	if (u->old_target) {
		if (!(u->type & REF_ISSYMREF)) {
			strbuf_addf(err, _("cannot lock ref '%s': "
					   "expected symref with target '%s': "
					   "but is a regular ref"),
				    ref_update_original_update_refname(u),
				    u->old_target);
			return REF_TRANSACTION_ERROR_EXPECTED_SYMREF;
		}

		ret = ref_update_check_old_target(referent->buf, u, err);
		if (ret)
			return ret;
	} else if (u->flags & REF_HAVE_OLD) {
		if (oideq(&current_oid, &u->old_oid)) {
			if ((u->flags & REF_NO_DEREF) &&
			    referent->len &&
			    is_null_oid(&u->old_oid)) {
				strbuf_addf(err, _("cannot lock ref '%s': "
					    "dangling symref already exists"),
					    ref_update_original_update_refname(u));
				return REF_TRANSACTION_ERROR_CREATE_EXISTS;
			}
		} else if (is_null_oid(&u->old_oid)) {
			strbuf_addf(err, _("cannot lock ref '%s': "
					   "reference already exists"),
				    ref_update_original_update_refname(u));
			return REF_TRANSACTION_ERROR_CREATE_EXISTS;
		} else if (is_null_oid(&current_oid)) {
			strbuf_addf(err, _("cannot lock ref '%s': "
					   "reference is missing but expected %s"),
				    ref_update_original_update_refname(u),
				    oid_to_hex(&u->old_oid));
			return REF_TRANSACTION_ERROR_NONEXISTENT_REF;
		} else {
			strbuf_addf(err, _("cannot lock ref '%s': "
					   "is at %s but expected %s"),
				    ref_update_original_update_refname(u),
				    oid_to_hex(&current_oid),
				    oid_to_hex(&u->old_oid));
			return REF_TRANSACTION_ERROR_INCORRECT_OLD_VALUE;
		}
	}

	if ((u->type & REF_ISSYMREF) ||
	    (u->flags & REF_HAVE_NEW && !oideq(&current_oid, &u->new_oid))) {
		ALLOC_GROW(arg->updates, arg->updates_nr + 1,
			   arg->updates_alloc);
		arg->updates[arg->updates_nr].update = u;
		oidcpy(&arg->updates[arg->updates_nr].current_oid, &current_oid);
		u->backend_data = &arg->updates[arg->updates_nr++];
	}

	return 0;
}

static int mvcc_be_transaction_prepare(struct ref_store *ref_store,
				       struct ref_transaction *transaction,
				       struct strbuf *err)
{
	struct mvcc_ref_store *mvcc = mvcc_be_downcast(ref_store,
						       REF_STORE_WRITE | REF_STORE_MAIN,
						       "ref_transaction_prepare");
	struct strbuf referent = STRBUF_INIT;
	struct string_list refnames_to_check = STRING_LIST_INIT_NODUP;
	struct mvcc_transaction_data *tx_data = NULL;
	int ret;

	if (mvcc_load_view(mvcc) < 0) {
		strbuf_addstr(err, "mvcc: failed to sync");
		ret = -1;
		goto done;
	}

	tx_data = xcalloc(1, sizeof(*tx_data));
	tx_data->arg.refs = mvcc;

	for (size_t i = 0; i < transaction->nr; i++) {
		ret = mvcc_prepare_single_update(mvcc, &tx_data->arg, transaction,
						 transaction->updates[i], i,
						 &refnames_to_check,
						 &referent, err);
		if (ret) {
			if (ref_transaction_maybe_set_rejected(transaction, i, ret, err)) {
				ret = 0;
				continue;
			}
			goto done;
		}
	}

	ret = refs_verify_refnames_available(ref_store, &refnames_to_check,
					     &transaction->refnames, NULL,
					     transaction,
					     transaction->flags & REF_TRANSACTION_FLAG_INITIAL,
					     err);
	if (ret < 0)
		goto done;

	transaction->backend_data = tx_data;
	transaction->state = REF_TRANSACTION_PREPARED;

done:
	if (ret < 0) {
		if (tx_data) {
			free(tx_data->arg.updates);
			free(tx_data);
		}
		transaction->state = REF_TRANSACTION_CLOSED;
		if (!err->len)
			strbuf_addstr(err, "mvcc: transaction prepare failed");
	}
	strbuf_release(&referent);
	string_list_clear(&refnames_to_check, 1);
	return ret;
}

static int mvcc_be_transaction_abort(struct ref_store *ref_store UNUSED,
				     struct ref_transaction *transaction,
				     struct strbuf *err UNUSED)
{
	struct mvcc_transaction_data *tx_data = transaction->backend_data;

	if (tx_data) {
		free(tx_data->arg.updates);
		free(tx_data);
	}
	transaction->backend_data = NULL;
	transaction->state = REF_TRANSACTION_CLOSED;
	return 0;
}

static int transaction_update_cmp(const void *a, const void *b)
{
	struct mvcc_transaction_update *update_a = (struct mvcc_transaction_update *)a;
	struct mvcc_transaction_update *update_b = (struct mvcc_transaction_update *)b;

	if (update_a->update->index || update_b->update->index)
		return update_a->update->index - update_b->update->index;

	return strcmp(update_a->update->refname, update_b->update->refname);
}

static int write_transaction_table(struct reftable_writer *writer, void *cb_data)
{
	struct mvcc_write_transaction_arg *arg = cb_data;
	int ret = 0;

	QSORT(arg->updates, arg->updates_nr, transaction_update_cmp);

	for (size_t i = 0; i < arg->updates_nr; i++) {
		struct mvcc_transaction_update *tx_update = &arg->updates[i];
		struct ref_update *u = tx_update->update;

		if (u->rejection_err)
			continue;
		if (u->flags & REF_LOG_ONLY)
			continue;

		if (u->new_target) {
			struct reftable_ref_record ref = {
				.refname = (char *)u->refname,
				.value_type = REFTABLE_REF_SYMREF,
				.value.symref = (char *)u->new_target,
				.update_index = arg->ts,
			};

			ret = reftable_writer_add_ref(writer, &ref);
			if (ret < 0)
				return ret;
		} else if ((u->flags & REF_HAVE_NEW) &&
			   ref_update_has_null_new_value(u)) {
			struct reftable_ref_record ref = {
				.refname = (char *)u->refname,
				.update_index = arg->ts,
				.value_type = REFTABLE_REF_DELETION,
			};

			ret = reftable_writer_add_ref(writer, &ref);
			if (ret < 0)
				return ret;
		} else if (u->flags & REF_HAVE_NEW) {
			struct reftable_ref_record ref = {
				.refname = (char *)u->refname,
				.update_index = arg->ts,
			};
			struct object_id peeled;
			int peel_error;

			peel_error = peel_object(arg->refs->base.repo, &u->new_oid,
						 &peeled,
						 PEEL_OBJECT_VERIFY_TAGGED_OBJECT_TYPE);
			if (!peel_error) {
				ref.value_type = REFTABLE_REF_VAL2;
				memcpy(ref.value.val2.target_value, peeled.hash, GIT_MAX_RAWSZ);
				memcpy(ref.value.val2.value, u->new_oid.hash, GIT_MAX_RAWSZ);
			} else if (!is_null_oid(&u->new_oid)) {
				ref.value_type = REFTABLE_REF_VAL1;
				memcpy(ref.value.val1, u->new_oid.hash, GIT_MAX_RAWSZ);
			}

			ret = reftable_writer_add_ref(writer, &ref);
			if (ret < 0)
				return ret;
		}
	}

	return 0;
}

static int mvcc_be_transaction_finish(struct ref_store *ref_store UNUSED,
				      struct ref_transaction *transaction,
				      struct strbuf *err)
{
	struct mvcc_transaction_data *tx_data = transaction->backend_data;
	struct mvcc_ref_store *mvcc;
	int ret = 0;

	if (!tx_data) {
		strbuf_addstr(err, "mvcc: transaction has no backend data");
		ret = -1;
		goto done;
	}

	mvcc = tx_data->arg.refs;
	tx_data->arg.ts = mvcc_next_update_index(mvcc);

	ret = mvcc_install_table(mvcc, write_transaction_table, &tx_data->arg,
				 tx_data->arg.ts,
				 tx_data->arg.ts + transaction->max_index,
				 /* replace nothing, append */
				 mvcc->tables_nr, mvcc->tables_nr);
	if (ret < 0) {
		strbuf_addstr(err, "mvcc: failed to publish reftable");
		goto done;
	}

done:
	if (tx_data) {
		free(tx_data->arg.updates);
		free(tx_data);
	}
	transaction->backend_data = NULL;
	transaction->state = REF_TRANSACTION_CLOSED;
	return ret;
}

struct write_copy_arg {
	struct mvcc_ref_store *refs;
	const char *oldname;
	const char *newname;
	uint64_t deletion_ts;
	uint64_t creation_ts;
	int delete_old;
};

static int write_copy_table(struct reftable_writer *writer, void *cb_data)
{
	struct write_copy_arg *arg = cb_data;
	struct reftable_iterator iter = { 0 };
	struct reftable_ref_record old_ref = { 0 }, refs[2] = { { 0 } };
	int ret;

	/* Look up the source ref via the merged view. */
	ret = reftable_merged_table_init_ref_iterator(arg->refs->merged, &iter);
	if (ret)
		goto done;

	ret = reftable_iterator_seek_ref(&iter, arg->oldname);
	if (ret)
		goto done;

	ret = reftable_iterator_next_ref(&iter, &old_ref);
	if (ret > 0 || (ret == 0 && strcmp(old_ref.refname, arg->oldname))) {
		ret = error(_("refname %s not found"), arg->oldname);
		goto done;
	}
	if (ret < 0)
		goto done;

	if (old_ref.value_type == REFTABLE_REF_SYMREF) {
		ret = error(_("refname %s is a symbolic ref, copying it is not supported"),
			    arg->oldname);
		goto done;
	}
	if (old_ref.value_type == REFTABLE_REF_DELETION) {
		ret = error(_("refname %s not found"), arg->oldname);
		goto done;
	}

	if (!strcmp(arg->oldname, arg->newname)) {
		ret = 0;
		goto done;
	}

	refs[0] = old_ref;
	refs[0].refname = xstrdup(arg->newname);
	refs[0].update_index = arg->creation_ts;
	if (arg->delete_old) {
		refs[1].refname = xstrdup(arg->oldname);
		refs[1].value_type = REFTABLE_REF_DELETION;
		refs[1].update_index = arg->deletion_ts;
	}
	ret = reftable_writer_add_refs(writer, refs, arg->delete_old ? 2 : 1);

done:
	reftable_iterator_destroy(&iter);
	for (size_t i = 0; i < ARRAY_SIZE(refs); i++)
		reftable_ref_record_release(&refs[i]);
	reftable_ref_record_release(&old_ref);
	return ret;
}

static int mvcc_rename_or_copy(struct ref_store *ref_store,
			       const char *oldref, const char *newref,
			       int delete_old)
{
	struct mvcc_ref_store *mvcc = mvcc_be_downcast(ref_store, REF_STORE_WRITE, delete_old ? "rename_ref" : "copy_ref");
	struct write_copy_arg arg = {
		.refs = mvcc,
		.oldname = oldref,
		.newname = newref,
		.delete_old = delete_old,
	};
	struct string_list skip = STRING_LIST_INIT_NODUP;
	struct strbuf errbuf = STRBUF_INIT;
	int ret;

	if (mvcc_load_view(mvcc) < 0) {
		ret = -1;
		goto done;
	}

	if (delete_old)
		string_list_insert(&skip, oldref);
	ret = refs_verify_refname_available(ref_store, newref, NULL, &skip,
					    0, &errbuf);
	if (ret < 0) {
		error("%s", errbuf.buf);
		goto done;
	}

	arg.deletion_ts = mvcc_next_update_index(mvcc);
	arg.creation_ts = arg.deletion_ts + (delete_old ? 1 : 0);

	ret = mvcc_install_table(mvcc, write_copy_table, &arg,
				 arg.deletion_ts, arg.creation_ts,
				 mvcc->tables_nr, mvcc->tables_nr);
	if (ret < 0)
		goto done;

done:
	string_list_clear(&skip, 0);
	strbuf_release(&errbuf);
	return ret;
}

static int mvcc_be_rename_ref(struct ref_store *ref_store,
			      const char *oldref, const char *newref,
			      const char *logmsg UNUSED)
{
	return mvcc_rename_or_copy(ref_store, oldref, newref, 1);
}

static int mvcc_be_copy_ref(struct ref_store *ref_store,
			    const char *oldref, const char *newref,
			    const char *logmsg UNUSED)
{
	return mvcc_rename_or_copy(ref_store, oldref, newref, 0);
}

struct mvcc_compact_segment {
	size_t start;
	size_t end; /* exclusive */
};

struct write_compact_arg {
	struct mvcc_ref_store *refs;
	struct mvcc_compact_segment seg;
};

static int write_compact_table(struct reftable_writer *writer, void *cb_data)
{
	struct write_compact_arg *arg = cb_data;
	struct reftable_merged_table *sub = NULL;
	struct reftable_iterator iter = { 0 };
	struct reftable_ref_record ref = { 0 };
	int ret;

	ret = reftable_merged_table_new(&sub, arg->refs->tables + arg->seg.start,
					arg->seg.end - arg->seg.start,
					arg->refs->hash_id);
	if (ret < 0)
		goto done;

	ret = reftable_merged_table_init_ref_iterator(sub, &iter);
	if (ret < 0)
		goto done;

	ret = reftable_iterator_seek_ref(&iter, "");
	if (ret < 0)
		goto done;

	while (1) {
		ret = reftable_iterator_next_ref(&iter, &ref);
		if (ret > 0) {
			ret = 0;
			break;
		}
		if (ret < 0)
			goto done;

		/*
		 * Drop deletion tombstones only when compacting from the
		 * bottom of the stack: there is nothing left below to be
		 * shadowed. For middle ranges we must keep them since they
		 * may still need to shadow refs in the kept tables before
		 * the segment.
		 */
		if (!arg->seg.start && ref.value_type == REFTABLE_REF_DELETION)
			continue;

		ret = reftable_writer_add_ref(writer, &ref);
		if (ret < 0)
			goto done;
	}

done:
	reftable_iterator_destroy(&iter);
	reftable_ref_record_release(&ref);
	reftable_merged_table_free(sub);
	return ret;
}

static void mvcc_compute_segment(struct mvcc_ref_store *mvcc,
				 struct mvcc_compact_segment *seg)
{
	uint64_t bytes = 0;
	size_t i;

	memset(seg, 0, sizeof(*seg));

	if (mvcc->tables_nr < 2)
		return;

	/*
	 * Find the segment end: scan backwards until the preceding table
	 * is smaller than the current table multiplied by the factor.
	 */
	for (i = mvcc->tables_nr - 1; i > 0; i--) {
		if (mvcc->tables[i - 1]->size < mvcc->tables[i]->size * 2) {
			seg->end = i + 1;
			bytes = mvcc->tables[i]->size;
			break;
		}
	}

	/*
	 * Find the segment start: keep extending leftwards as long as
	 * the preceding table is smaller than the running sum times the
	 * factor.
	 */
	for (; i > 0; i--) {
		if (mvcc->tables[i - 1]->size < bytes * 2)
			seg->start = i - 1;
		bytes += mvcc->tables[i - 1]->size;
	}
}

static int mvcc_be_optimize(struct ref_store *ref_store,
			    struct refs_optimize_opts *opts UNUSED)
{
	struct mvcc_ref_store *mvcc = mvcc_be_downcast(ref_store, REF_STORE_WRITE, "optimize_refs");
	struct write_compact_arg arg = {
		.refs = mvcc,
	};
	int ret;

	if (mvcc_load_view(mvcc) < 0)
		return -1;

	mvcc_compute_segment(mvcc, &arg.seg);
	if (arg.seg.end <= arg.seg.start || arg.seg.end - arg.seg.start < 2)
		return 0;

	ret = mvcc_install_table(mvcc, write_compact_table, &arg,
				 reftable_table_min_update_index(mvcc->tables[arg.seg.start]),
				 reftable_table_min_update_index(mvcc->tables[arg.seg.end - 1]),
				 arg.seg.start, arg.seg.end);
	if (ret < 0)
		return -1;

	return 0;
}

static int mvcc_be_optimize_required(struct ref_store *ref_store,
				     struct refs_optimize_opts *opts UNUSED,
				     bool *required)
{
	struct mvcc_ref_store *mvcc = mvcc_be_downcast(ref_store, REF_STORE_READ, "optimize_refs_required");
	struct mvcc_compact_segment seg;

	*required = false;

	if (mvcc_load_view(mvcc) < 0)
		return -1;

	mvcc_compute_segment(mvcc, &seg);
	*required = (seg.end - seg.start) >= 2;

	return 0;
}

static struct ref_iterator *mvcc_be_reflog_iterator_begin(struct ref_store *ref_store UNUSED)
{
	return empty_ref_iterator_begin();
}

static int mvcc_be_for_each_reflog_ent(struct ref_store *ref_store UNUSED,
				       const char *refname UNUSED,
				       each_reflog_ent_fn fn UNUSED,
				       void *cb_data UNUSED)
{
	return 0;
}

static int mvcc_be_for_each_reflog_ent_reverse(struct ref_store *ref_store UNUSED,
					       const char *refname UNUSED,
					       each_reflog_ent_fn fn UNUSED,
					       void *cb_data UNUSED)
{
	return 0;
}

static int mvcc_be_reflog_exists(struct ref_store *ref_store UNUSED,
				 const char *refname UNUSED)
{
	return 0;
}

static int mvcc_be_create_reflog(struct ref_store *ref_store UNUSED,
				 const char *refname UNUSED,
				 struct strbuf *err UNUSED)
{
	return 0;
}

static int mvcc_be_delete_reflog(struct ref_store *ref_store UNUSED,
				 const char *refname UNUSED)
{
	return 0;
}

static int mvcc_be_reflog_expire(struct ref_store *ref_store UNUSED,
				 const char *refname UNUSED, unsigned int flags UNUSED,
				 reflog_expiry_prepare_fn prepare_fn UNUSED,
				 reflog_expiry_should_prune_fn should_prune_fn UNUSED,
				 reflog_expiry_cleanup_fn cleanup_fn UNUSED,
				 void *policy_cb_data UNUSED)
{
	return 0;
}

static int mvcc_be_fsck(struct ref_store *ref_store UNUSED,
			struct fsck_options *o UNUSED, struct worktree *wt UNUSED)
{
	return 0;
}

static struct ref_store *mvcc_be_init(struct repository *repo,
				      const char *payload UNUSED,
				      const char *gitdir, unsigned int store_flags)
{
	struct mvcc_ref_store *mvcc;
	struct strbuf ref_common_dir = STRBUF_INIT;
	struct strbuf refdir = STRBUF_INIT;
	bool is_worktree;

	refs_compute_filesystem_location(gitdir, NULL, &is_worktree, &refdir,
					 &ref_common_dir);
	strbuf_release(&ref_common_dir);
	strbuf_release(&refdir);

	if (is_worktree)
		die(_("the MVCC reference backend does not support worktrees"));

	CALLOC_ARRAY(mvcc, 1);
	base_ref_store_init(&mvcc->base, repo, gitdir, &refs_be_mvcc);
	mvcc->storage = mvcc_storage_get(repo->commondir);
	mvcc->store_flags = store_flags;
	mvcc->view_dirty = true;

	switch (repo->hash_algo->format_id) {
	case GIT_SHA1_FORMAT_ID:
		mvcc->hash_id = REFTABLE_HASH_SHA1;
		break;
	case GIT_SHA256_FORMAT_ID:
		mvcc->hash_id = REFTABLE_HASH_SHA256;
		break;
	default:
		BUG("unknown hash algorithm %d", repo->hash_algo->format_id);
	}
	mvcc->write_options.hash_id = mvcc->hash_id;
	mvcc->write_options.block_size = 4096;

	return &mvcc->base;
}

static void mvcc_be_release(struct ref_store *ref_store)
{
	struct mvcc_ref_store *mvcc = mvcc_be_downcast(ref_store, 0, "release");
	mvcc_release_view(mvcc);
	mvcc_storage_release(mvcc->storage);
}

static int mvcc_be_create_on_disk(struct ref_store *ref_store UNUSED,
				  int flags UNUSED, struct strbuf *err UNUSED)
{
	return 0;
}

static int mvcc_be_remove_on_disk(struct ref_store *ref_store,
				  struct strbuf *err)
{
	struct mvcc_ref_store *mvcc = mvcc_be_downcast(ref_store, REF_STORE_WRITE, "remove");
	struct strbuf sb = STRBUF_INIT;
	int ret = 0;

	mvcc_release_view(mvcc);

	mvcc_storage_write_path(mvcc->storage, "reftables", NULL, &sb);
	if (remove_dir_recursively(&sb, 0) < 0) {
		strbuf_addf(err, "could not delete reftables: %s",
			    strerror(errno));
		ret = -1;
	}

	strbuf_release(&sb);
	return ret;
}

struct ref_storage_be refs_be_mvcc = {
	.name = "mvcc",
	.init = mvcc_be_init,
	.release = mvcc_be_release,
	.create_on_disk = mvcc_be_create_on_disk,
	.remove_on_disk = mvcc_be_remove_on_disk,

	.transaction_prepare = mvcc_be_transaction_prepare,
	.transaction_finish = mvcc_be_transaction_finish,
	.transaction_abort = mvcc_be_transaction_abort,

	.optimize = mvcc_be_optimize,
	.optimize_required = mvcc_be_optimize_required,

	.rename_ref = mvcc_be_rename_ref,
	.copy_ref = mvcc_be_copy_ref,

	.iterator_begin = mvcc_be_iterator_begin,
	.read_raw_ref = mvcc_be_read_raw_ref,
	.read_symbolic_ref = mvcc_be_read_symbolic_ref,

	.reflog_iterator_begin = mvcc_be_reflog_iterator_begin,
	.for_each_reflog_ent = mvcc_be_for_each_reflog_ent,
	.for_each_reflog_ent_reverse = mvcc_be_for_each_reflog_ent_reverse,

	.reflog_exists = mvcc_be_reflog_exists,
	.create_reflog = mvcc_be_create_reflog,
	.delete_reflog = mvcc_be_delete_reflog,
	.reflog_expire = mvcc_be_reflog_expire,

	.fsck = mvcc_be_fsck,
};
