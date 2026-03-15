#include "builtin.h"
#include "parse-options.h"

static const char *const series_usage[] = {
	"git series",
	NULL
};

int cmd_series(int argc, const char **argv, const char *prefix,
	       struct repository *repo UNUSED)
{
	struct option options[] = {
		OPT_END()
	};

	argc = parse_options(argc, argv, prefix, options, series_usage, 0);

	return 0;
}
