#include "builtin.h"
#include "config.h"
#include "diff.h"
#include "diffcore.h"
#include "gettext.h"
#include "hash.h"
#include "object.h"
#include "parse-options.h"
#include "revision.h"

static void diff_blobs(struct object_array_entry *old_blob,
		       struct object_array_entry *new_blob,
		       struct diff_options *opts)
{
	const unsigned mode = canon_mode(S_IFREG | 0644);
	struct object_id old_oid = old_blob->item->oid;
	struct object_id new_oid = new_blob->item->oid;
	unsigned old_mode = old_blob->mode;
	unsigned new_mode = new_blob->mode;
	char *old_path = old_blob->path;
	char *new_path = new_blob->path;
	struct diff_filespec *old, *new;

	if (old_mode == S_IFINVALID)
		old_mode = mode;

	if (new_mode == S_IFINVALID)
		new_mode = mode;

	if (!old_path)
		old_path = old_blob->name;

	if (!new_path)
		new_path = new_blob->name;

	if (!is_null_oid(&old_oid) && !is_null_oid(&new_oid) &&
	    oideq(&old_oid, &new_oid) && (old_mode == new_mode))
		return;

	if (opts->flags.reverse_diff) {
		SWAP(old_oid, new_oid);
		SWAP(old_mode, new_mode);
		SWAP(old_path, new_path);
	}

	if (opts->prefix &&
	    (strncmp(old_path, opts->prefix, opts->prefix_length) ||
	     strncmp(new_path, opts->prefix, opts->prefix_length)))
		return;

	old = alloc_filespec(old_path);
	new = alloc_filespec(new_path);

	fill_filespec(old, &old_oid, 1, old_mode);
	fill_filespec(new, &new_oid, 1, new_mode);

	diff_queue(&diff_queued_diff, old, new);
	diffcore_std(opts);
	diff_flush(opts);
}

int cmd_diff_blob(int argc, const char **argv, const char *prefix,
		  struct repository *repo)
{
	struct object_array_entry *old_blob, *new_blob;
	struct rev_info revs;
	int ret;

	const char * const usage[] = {
		N_("git diff-blob <blob> <blob>"),
		NULL
	};
	struct option options[] = {
		OPT_END()
	};

	argc = parse_options(argc, argv, prefix, options, usage,
			     PARSE_OPT_KEEP_UNKNOWN_OPT | PARSE_OPT_KEEP_ARGV0);

	repo_config(repo, git_diff_basic_config, NULL);
	prepare_repo_settings(repo);
	repo->settings.command_requires_full_index = 0;

	repo_init_revisions(repo, &revs, prefix);
	revs.abbrev = 0;
	revs.diff = 1;
	revs.disable_stdin = 1;

	prefix = precompose_argv_prefix(argc, argv, prefix);
	argc = setup_revisions(argc, argv, &revs, NULL);

	if (!revs.diffopt.output_format)
		revs.diffopt.output_format = DIFF_FORMAT_PATCH;

	switch (revs.pending.nr) {
	case 2:
		old_blob = &revs.pending.objects[0];
		new_blob = &revs.pending.objects[1];

		if (old_blob->item->type != OBJ_BLOB)
			die("object %s is not a blob", old_blob->name);

		if (new_blob->item->type != OBJ_BLOB)
			die("object %s is not a blob", new_blob->name);

		diff_blobs(old_blob, new_blob, &revs.diffopt);

		break;
	default:
		usage_with_options(usage, options);
	}

	ret = diff_result_code(&revs);
	release_revisions(&revs);

	return ret;
}
