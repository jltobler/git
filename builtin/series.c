#include "builtin.h"
#include "hash.h"
#include "object-name.h"
#include "parse-options.h"
#include "refs.h"
#include "strbuf.h"

static const char *const series_usage[] = {
	"git series create <series-name>",
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

int cmd_series(int argc, const char **argv, const char *prefix,
	       struct repository *repo)
{
	parse_opt_subcommand_fn *fn = NULL;
	struct option options[] = {
		OPT_SUBCOMMAND("create", &fn, cmd_series_create),
		OPT_END()
	};

	argc = parse_options(argc, argv, prefix, options, series_usage, 0);

	return fn(argc, argv, prefix, repo);
}
