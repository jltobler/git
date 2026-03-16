#include "builtin.h"
#include "hash.h"
#include "hex.h"
#include "log-tree.h"
#include "object-name.h"
#include "parse-options.h"
#include "refs.h"
#include "revision.h"
#include "strbuf.h"

static const char *const series_usage[] = {
	"git series create <series-name>",
	"git series list",
	"git series log",
	"git series tag",
	NULL
};

static void create_series_ref(struct ref_transaction *txn, const char *series,
			      const char *name, struct object_id *oid,
			      const char *target)
{
	struct strbuf refname = STRBUF_INIT;
	struct strbuf err = STRBUF_INIT;

	strbuf_addf(&refname, "refs/series/%s/%s", series, name);
	if (ref_transaction_create(txn, refname.buf, oid, target, 0, NULL, &err))
		die(_("failed to create ref"));

	strbuf_release(&refname);
	strbuf_release(&err);
}

static int cmd_series_create(int argc, const char **argv, const char *prefix,
			     struct repository *repo)
{
	struct ref_transaction *transaction;
	struct strbuf branch = STRBUF_INIT;
	struct strbuf head = STRBUF_INIT;
	struct strbuf err = STRBUF_INIT;
	const char *series_name;
	struct object_id oid;
	int ret;
	struct option options[] = {
		OPT_END()
	};

	argc = parse_options(argc, argv, prefix, options, series_usage, 0);
	if (argc != 1)
		usage(_("too many arguments"));

	series_name = argv[0];

	if (check_refname_format(series_name, REFNAME_ALLOW_ONELEVEL))
		die(_("invalid series name"));

	ret = refs_read_symbolic_ref(get_main_ref_store(repo), "HEAD", &head);
	if (ret == NOT_A_SYMREF)
		die(_("cannot create series in detached head"));

	strbuf_addf(&branch, "refs/heads/%s", series_name);

	repo_get_oid(repo, "HEAD", &oid);
	if (is_null_oid(&oid))
		die(_("non-existent base"));

	transaction = ref_store_transaction_begin(get_main_ref_store(repo), 0,
						  &err);

	ref_transaction_create(transaction, branch.buf, &oid, NULL, 0, NULL, &err);
	create_series_ref(transaction, series_name, "base", &oid, NULL);
	create_series_ref(transaction, series_name, "HEAD", NULL, head.buf);

	if (ref_transaction_commit(transaction, &err))
		die("%s", err.buf);

	refs_update_symref(get_main_ref_store(repo), "HEAD", branch.buf, "series create");

	strbuf_release(&branch);
	strbuf_release(&head);
	strbuf_release(&err);

	return 0;
}

static int print_series(const struct reference *ref, void *cb_data UNUSED)
{
	size_t name_len;

	if (strip_suffix(ref->name, "/base", &name_len))
		printf("%.*s\n", (int)name_len, ref->name);

	return 0;
}

static int cmd_series_list(int argc, const char **argv, const char *prefix,
			   struct repository *repo)
{
	struct refs_for_each_ref_options opts = {
		.prefix = "refs/series/",
		.trim_prefix = strlen("refs/series/"),
		.pattern = "*/base",
	};
	struct option options[] = {
		OPT_END()
	};

	argc = parse_options(argc, argv, prefix, options, series_usage, 0);
	if (argc)
		usage(_("too many arguments"));

	refs_for_each_ref_ext(get_main_ref_store(repo), print_series, NULL, &opts);

	return 0;
}

static char *get_current_series(struct repository *repo)
{
	struct strbuf referent = STRBUF_INIT;
	struct strbuf base = STRBUF_INIT;
	const char *series;
	char *result = NULL;

	if (refs_read_symbolic_ref(get_main_ref_store(repo), "HEAD", &referent) == NOT_A_SYMREF)
		goto out;

	if (!skip_prefix(referent.buf, "refs/heads/", &series))
		goto out;

	strbuf_addf(&base, "refs/series/%s/base", series);
	if (!refs_ref_exists(get_main_ref_store(repo), base.buf))
		goto out;

	result = xstrdup(series);

out:
	strbuf_release(&referent);
	strbuf_release(&base);

	return result;
}

static int cmd_series_log(int argc, const char **argv, const char *prefix,
			  struct repository *repo)
{
	struct strbuf base_ref = STRBUF_INIT;
	struct object_id base;
	struct commit *commit;
	struct rev_info rev;
	char *series;
	struct option options[] = {
		OPT_END()
	};
	argc = parse_options(argc, argv, prefix, options, series_usage, 0);
	if (argc)
		usage(_("too many arguments"));

	repo_init_revisions(repo, &rev, prefix);
	rev.commit_format = CMIT_FMT_MEDIUM;
	rev.always_show_header = 1;
	rev.verbose_header = 1;

	series = get_current_series(repo);
	if (!series)
		die(_("not series found"));

	strbuf_addf(&base_ref, "refs/series/%s/base", series);
	repo_get_oid(repo, base_ref.buf, &base);

	add_head_to_pending(&rev);
	add_pending_oid(&rev, base_ref.buf, &base, UNINTERESTING);

	if (prepare_revision_walk(&rev))
		die(_("revision walk setup failed"));

	while ((commit = get_revision(&rev)) != NULL)
		log_tree_commit(&rev, commit);

	release_revisions(&rev);
	strbuf_release(&base_ref);
	free(series);

	return 0;
}

static int get_latest_version(struct repository *repo, const char *series)
{
	struct strbuf latest_ref = STRBUF_INIT;
	struct strbuf referent = STRBUF_INIT;
	struct strbuf prefix = STRBUF_INIT;
	const char *version = NULL;
	int ret;

	strbuf_addf(&latest_ref, "refs/series/%s/LATEST", series);

	if (!refs_ref_exists(get_main_ref_store(repo), latest_ref.buf)) {
		strbuf_release(&latest_ref);
		return 0;
	}

	ret = refs_read_symbolic_ref(get_main_ref_store(repo), latest_ref.buf, &referent);
	if (ret == NOT_A_SYMREF)
		die(_("invalid LATEST reference for series"));

	strbuf_addf(&prefix, "refs/series/%s/tags/v", series);
	if (!skip_prefix(referent.buf, prefix.buf, &version))
		die(_("invalid LATEST reference for series"));

	strbuf_release(&latest_ref);

	return atoi(version);
}

static int cmd_series_tag(int argc, const char **argv, const char *prefix,
			  struct repository *repo)
{
	struct ref_transaction *transaction;
	struct strbuf latest = STRBUF_INIT;
	struct strbuf suffix = STRBUF_INIT;
	struct strbuf tag = STRBUF_INIT;
	struct strbuf err = STRBUF_INIT;
	struct object_id oid;
	const char *series;
	int version;
	struct option options[] = {
		OPT_END()
	};
	argc = parse_options(argc, argv, prefix, options, series_usage, 0);
	if (argc)
		usage(_("too many arguments"));

	series = get_current_series(repo);
	if (!series)
		die(_("series not started"));

	version = get_latest_version(repo, series);
	version++;

	transaction = ref_store_transaction_begin(get_main_ref_store(repo), 0,
						  &err);

	repo_get_oid(repo, "HEAD", &oid);

	strbuf_addf(&suffix, "tags/v%d", version);
	create_series_ref(transaction, series, suffix.buf, &oid, NULL);

	if (ref_transaction_commit(transaction, &err))
		die("%s", err.buf);

	strbuf_addf(&latest, "refs/series/%s/LATEST", series);
	strbuf_addf(&tag, "refs/series/%s/tags/v%d", series, version);
	refs_update_symref(get_main_ref_store(repo), latest.buf, tag.buf, "series tag");

	strbuf_release(&latest);
	strbuf_release(&suffix);
	strbuf_release(&tag);
	strbuf_release(&err);

	return 0;
}

int cmd_series(int argc, const char **argv, const char *prefix,
	       struct repository *repo)
{
	parse_opt_subcommand_fn *fn = NULL;
	struct option options[] = {
		OPT_SUBCOMMAND("create", &fn, cmd_series_create),
		OPT_SUBCOMMAND("list", &fn, cmd_series_list),
		OPT_SUBCOMMAND("log", &fn, cmd_series_log),
		OPT_SUBCOMMAND("tag", &fn, cmd_series_tag),
		OPT_END()
	};

	argc = parse_options(argc, argv, prefix, options, series_usage, 0);

	return fn(argc, argv, prefix, repo);
}
