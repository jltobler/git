#include "git-compat-util.h"
#include "refs/refs-internal.h"
#include "s3.h"
#include "tempfile.h"

struct reftable_s3_ref_store {
	struct ref_store base;
	struct s3_storage *storage;
	struct ref_store *inner;

	struct tempfile *list_file;
	char *reftable_dir;
	unsigned int store_flags;
	bool tables_synced;
	bool existing_repo;
};

static int s3_upload_new_tables(struct reftable_s3_ref_store *s3)
{
	const struct s3_manifest *old_manifest;
	struct s3_manifest new_manifest = S3_MANIFEST_INIT;
	struct strbuf list_file = STRBUF_INIT;
	struct strbuf suffix = STRBUF_INIT;
	struct strbuf path = STRBUF_INIT;
	struct strbuf key = STRBUF_INIT;
	int ret;

	if (strbuf_read_file(&list_file, get_tempfile_path(s3->list_file), 0) < 0) {
		ret = error_errno("s3: cannot read '%s'",
				  get_tempfile_path(s3->list_file));
		goto out;
	}

	old_manifest = s3_storage_get_manifest(s3->storage);

	/* Update the manifest with the new tables. */
	s3_manifest_copy(old_manifest, &new_manifest);
	string_list_clear(&new_manifest.reftables, 0);
	strbuf_trim(&list_file);
	if (list_file.len)
		string_list_split(&new_manifest.reftables, list_file.buf, "\n", -1);

	/*
	 * Check for any tables that have changed between the old and new
	 * manifest and upload those new tables to S3.
	 */
	for (size_t i = 0; i < new_manifest.reftables.nr; i++) {
		const char *table_name = new_manifest.reftables.items[i].string;

		if (unsorted_string_list_has_string(&old_manifest->reftables,
						    table_name))
			continue;

		strbuf_reset(&path);
		strbuf_addf(&path, "%s/%s", s3->reftable_dir, table_name);
		strbuf_reset(&suffix);
		strbuf_addf(&suffix, "reftable/%s", table_name);

		ret = s3_put_from_file(s3->storage, s3_key(s3->storage, &key, suffix.buf),
				       path.buf);
		if (ret < 0) {
			ret = error("s3: failed to upload '%s'", path.buf);
			goto out;
		}
	}

	ret = s3_storage_update_manifest(s3->storage, &new_manifest);

out:
	s3_manifest_release(&new_manifest);
	strbuf_release(&list_file);
	strbuf_release(&suffix);
	strbuf_release(&path);
	strbuf_release(&key);
	return ret;
}

static int s3_sync_tables(struct reftable_s3_ref_store *s3)
{
	const struct s3_manifest *manifest;
	struct strbuf key = STRBUF_INIT;
	struct strbuf list = STRBUF_INIT;
	char *list_file_template;
	int ret;

	if (s3->tables_synced)
		return 0;

	manifest = s3_storage_get_manifest(s3->storage);

	for (size_t i = 0; i < manifest->reftables.nr; i++) {
		const char *table_name = manifest->reftables.items[i].string;
		char *local_path = xstrfmt("%s/%s", s3->reftable_dir, table_name);

		if (access(local_path, R_OK) < 0) {
			char *suffix = xstrfmt("reftable/%s", table_name);
			if (s3_get_to_file(s3->storage, s3_key(s3->storage, &key, suffix), local_path) < 0)
				die("s3: failed downloading '%s'", key.buf);
			free(suffix);
		}

		strbuf_addf(&list, "%s\n", table_name);
		free(local_path);
	}

	delete_tempfile(&s3->list_file);
	list_file_template = xstrfmt("%s/tables.list.XXXXXX", s3->reftable_dir);
	s3->list_file = xmks_tempfile(list_file_template);
	xsetenv("GIT_REFTABLE_LIST_FILE", get_tempfile_path(s3->list_file), 1);

	if (write_in_full(get_tempfile_fd(s3->list_file), list.buf, list.len) < 0) {
		ret = error_errno("s3: cannot write '%s'",
				  get_tempfile_path(s3->list_file));
		delete_tempfile(&s3->list_file);
		goto out;
	}

	close_tempfile_gently(s3->list_file);

	if (manifest->reftables.nr > 0)
		s3->existing_repo = true;
	s3->tables_synced = true;
	ret = 0;

out:
	strbuf_release(&key);
	strbuf_release(&list);
	return ret;
}

static struct ref_store *s3_be_init(struct repository *repo, const char *payload,
				    const char *gitdir, unsigned int store_flags)
{
	struct reftable_s3_ref_store *s3;

	CALLOC_ARRAY(s3, 1);

	s3->reftable_dir = xstrfmt("%s/reftable", gitdir);
	s3->storage = s3_storage_get(repo, payload);
	s3->store_flags = store_flags;

	if (mkdir(s3->reftable_dir, 0777) < 0 && errno != EEXIST)
		die_errno("s3: cannot create '%s'", s3->reftable_dir);
	if (s3_sync_tables(s3) < 0)
		die("s3: failed to sync tables from S3");

	s3->inner = refs_be_reftable.init(repo, NULL, gitdir, store_flags);

	base_ref_store_init(&s3->base, repo, gitdir, &refs_be_s3);

	return &s3->base;
}

static struct reftable_s3_ref_store *s3_be_downcast(struct ref_store *ref_store,
						    unsigned int required_flags,
						    const char *caller)
{
	struct reftable_s3_ref_store *s3;

	if (ref_store->be != &refs_be_s3)
		BUG("ref_store is type \"%s\" not \"s3\" in %s",
		    ref_store->be->name, caller);

	s3 = (struct reftable_s3_ref_store *)ref_store;

	if ((s3->store_flags & required_flags) != required_flags)
		BUG("operation %s requires abilities 0x%x, but only have 0x%x",
		    caller, required_flags, s3->store_flags);

	return s3;
}

static void s3_be_release(struct ref_store *ref_store)
{
	struct reftable_s3_ref_store *s3 = s3_be_downcast(ref_store, 0, "release");

	ref_store_release(s3->inner);
	s3_storage_release(s3->storage);
	delete_tempfile(&s3->list_file);
	free(s3->inner);
	free(s3->reftable_dir);
	free(s3->list_file);
}

static int s3_be_create_on_disk(struct ref_store *ref_store,
				int flags, struct strbuf *err)
{
	struct reftable_s3_ref_store *s3 =
		s3_be_downcast(ref_store, REF_STORE_WRITE, "create");

	if (s3->inner->be->create_on_disk(s3->inner, flags, err) < 0)
		return -1;

	return 0;
}

static int s3_be_remove_on_disk(struct ref_store *ref_store,
				struct strbuf *err)
{
	struct reftable_s3_ref_store *s3 =
		s3_be_downcast(ref_store, REF_STORE_WRITE, "remove");
	return s3->inner->be->remove_on_disk(s3->inner, err);
}

static int s3_be_transaction_prepare(struct ref_store *ref_store,
				     struct ref_transaction *transaction,
				     struct strbuf *err)
{
	struct reftable_s3_ref_store *s3 =
		s3_be_downcast(ref_store, REF_STORE_WRITE | REF_STORE_MAIN,
			       "transaction_prepare");

	if (s3_sync_tables(s3) < 0) {
		strbuf_addstr(err, "s3: failed to sync");
		return -1;
	}

	return s3->inner->be->transaction_prepare(s3->inner, transaction, err);
}

static int s3_be_transaction_finish(struct ref_store *ref_store,
				    struct ref_transaction *transaction,
				    struct strbuf *err)
{
	struct reftable_s3_ref_store *s3 =
		s3_be_downcast(ref_store, REF_STORE_WRITE | REF_STORE_MAIN,
			       "transaction_finish");
	int ret;

	ret = s3->inner->be->transaction_finish(s3->inner, transaction, err);
	if (ret < 0)
		return ret;

	s3->tables_synced = false;

	if (s3_upload_new_tables(s3) < 0) {
		strbuf_addstr(err, "s3: failed to upload tables");
		return -1;
	}

	return 0;
}

static int s3_be_transaction_abort(struct ref_store *ref_store,
				   struct ref_transaction *transaction,
				   struct strbuf *err)
{
	struct reftable_s3_ref_store *s3 =
		s3_be_downcast(ref_store, 0, "transaction_abort");
	return s3->inner->be->transaction_abort(s3->inner, transaction, err);
}

static int s3_be_rename_ref(struct ref_store *ref_store,
			    const char *oldref, const char *newref,
			    const char *logmsg)
{
	struct reftable_s3_ref_store *s3 = s3_be_downcast(ref_store,
							  REF_STORE_WRITE,
							  "rename_ref");
	int ret;

	if (s3_sync_tables(s3) < 0)
		return -1;
	ret = s3->inner->be->rename_ref(s3->inner, oldref, newref, logmsg);
	if (ret < 0)
		return ret;
	s3->tables_synced = false;
	return s3_upload_new_tables(s3);
}

static int s3_be_copy_ref(struct ref_store *ref_store,
			  const char *oldref, const char *newref,
			  const char *logmsg)
{
	struct reftable_s3_ref_store *s3 = s3_be_downcast(ref_store,
							  REF_STORE_WRITE,
							  "copy_ref");
	int ret;

	if (s3_sync_tables(s3) < 0)
		return -1;
	ret = s3->inner->be->copy_ref(s3->inner, oldref, newref, logmsg);
	if (ret < 0)
		return ret;
	s3->tables_synced = false;
	return s3_upload_new_tables(s3);
}

static int s3_be_read_raw_ref(struct ref_store *ref_store,
			      const char *refname, struct object_id *oid,
			      struct strbuf *referent, unsigned int *type,
			      int *failure_errno)
{
	struct reftable_s3_ref_store *s3 = s3_be_downcast(ref_store,
							  REF_STORE_READ,
							  "read_raw_ref");

	if (s3_sync_tables(s3) < 0) {
		*failure_errno = ENOENT;
		return -1;
	}

	return s3->inner->be->read_raw_ref(s3->inner, refname, oid,
					   referent, type, failure_errno);
}

static int s3_be_read_symbolic_ref(struct ref_store *ref_store,
				   const char *refname,
				   struct strbuf *referent)
{
	struct reftable_s3_ref_store *s3 = s3_be_downcast(ref_store,
							  REF_STORE_READ,
							  "read_symbolic_ref");

	if (s3_sync_tables(s3) < 0)
		return -1;

	return s3->inner->be->read_symbolic_ref(s3->inner, refname, referent);
}

static struct ref_iterator *s3_be_iterator_begin(struct ref_store *ref_store,
						 const char *prefix,
						 const char **exclude_patterns,
						 unsigned int flags)
{
	struct reftable_s3_ref_store *s3 = s3_be_downcast(ref_store,
							  REF_STORE_READ,
							  "iterator_begin");

	if (s3_sync_tables(s3) < 0)
		return empty_ref_iterator_begin();

	return s3->inner->be->iterator_begin(s3->inner, prefix,
					     exclude_patterns, flags);
}

static struct ref_iterator *s3_be_reflog_iterator_begin(struct ref_store *ref_store UNUSED)
{
	return empty_ref_iterator_begin();
}

static int s3_be_for_each_reflog_ent(struct ref_store *ref_store UNUSED,
				     const char *refname UNUSED,
				     each_reflog_ent_fn fn UNUSED,
				     void *cb_data UNUSED)
{
	return 0;
}

static int s3_be_for_each_reflog_ent_reverse(
	struct ref_store *ref_store UNUSED,
	const char *refname UNUSED,
	each_reflog_ent_fn fn UNUSED,
	void *cb_data UNUSED)
{
	return 0;
}

static int s3_be_reflog_exists(struct ref_store *ref_store UNUSED,
			       const char *refname UNUSED)
{
	return 0;
}

static int s3_be_create_reflog(struct ref_store *ref_store UNUSED,
			       const char *refname UNUSED,
			       struct strbuf *err UNUSED)
{
	return 0;
}

static int s3_be_delete_reflog(struct ref_store *ref_store UNUSED,
			       const char *refname UNUSED)
{
	return 0;
}

static int s3_be_reflog_expire(struct ref_store *ref_store UNUSED,
			       const char *refname UNUSED, unsigned int flags UNUSED,
			       reflog_expiry_prepare_fn prepare_fn UNUSED,
			       reflog_expiry_should_prune_fn should_prune_fn UNUSED,
			       reflog_expiry_cleanup_fn cleanup_fn UNUSED,
			       void *policy_cb_data UNUSED)
{
	return 0;
}

static int s3_be_optimize(struct ref_store *ref_store UNUSED,
			  struct refs_optimize_opts *opts UNUSED)
{
	return 0;
}

static int s3_be_optimize_required(struct ref_store *ref_store UNUSED,
				   struct refs_optimize_opts *opts UNUSED,
				   bool *required UNUSED)
{
	return 0;
}

static int s3_be_fsck(struct ref_store *ref_store UNUSED,
		      struct fsck_options *o UNUSED, struct worktree *wt UNUSED)
{
	return 0;
}

struct ref_storage_be refs_be_s3 = {
	.name = "s3",
	.init = s3_be_init,
	.release = s3_be_release,
	.create_on_disk = s3_be_create_on_disk,
	.remove_on_disk = s3_be_remove_on_disk,

	.transaction_prepare = s3_be_transaction_prepare,
	.transaction_finish = s3_be_transaction_finish,
	.transaction_abort = s3_be_transaction_abort,

	.rename_ref = s3_be_rename_ref,
	.copy_ref = s3_be_copy_ref,
	.read_raw_ref = s3_be_read_raw_ref,
	.read_symbolic_ref = s3_be_read_symbolic_ref,

	.iterator_begin = s3_be_iterator_begin,
	.reflog_iterator_begin = s3_be_reflog_iterator_begin,
	.for_each_reflog_ent = s3_be_for_each_reflog_ent,
	.for_each_reflog_ent_reverse = s3_be_for_each_reflog_ent_reverse,

	.reflog_exists = s3_be_reflog_exists,
	.create_reflog = s3_be_create_reflog,
	.delete_reflog = s3_be_delete_reflog,
	.reflog_expire = s3_be_reflog_expire,

	.optimize = s3_be_optimize,
	.optimize_required = s3_be_optimize_required,
	.fsck = s3_be_fsck,
};
