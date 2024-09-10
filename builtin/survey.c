#include "builtin.h"
#include "config.h"
#include "parse-options.h"

static const char * const survey_usage[] = {
	N_("(EXPERIMENTAL!) git survey <options>"),
	NULL,
};

struct survey_opts {
	int verbose;
	int show_progress;
};

static struct survey_opts survey_opts = {
	.verbose = 0,
	.show_progress = -1, /* defaults to isatty(2) */
};

static struct option survey_options[] = {
	OPT__VERBOSE(&survey_opts.verbose, N_("verbose output")),
	OPT_BOOL(0, "progress", &survey_opts.show_progress, N_("show progress")),
	OPT_END(),
};

static int survey_load_config_cb(const char *var, const char *value,
				 const struct config_context *ctx, void *pvoid)
{
	if (!strcmp(var, "survey.verbose")) {
		survey_opts.verbose = git_config_bool(var, value);
		return 0;
	}
	if (!strcmp(var, "survey.progress")) {
		survey_opts.show_progress = git_config_bool(var, value);
		return 0;
	}

	return git_default_config(var, value, ctx, pvoid);
}

int cmd_survey(int argc, const char **argv, const char *prefix, struct repository *repo)
{
	show_usage_with_options_if_asked(argc, argv, survey_usage, survey_options);

	prepare_repo_settings(repo);
	repo_config(repo, survey_load_config_cb, NULL);

	argc = parse_options(argc, argv, prefix, survey_options, survey_usage, 0);

	if (survey_opts.show_progress < 0)
		survey_opts.show_progress = isatty(2);

	return 0;
}
