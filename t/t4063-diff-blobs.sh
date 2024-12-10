#!/bin/sh

test_description='test direct comparison of blobs via git-diff and git-diff-blob'

. ./test-lib.sh

commands="diff diff-blob"

run_diff () {
	# use full-index to make it easy to match the index line
	git $1 --full-index $2 $3 >diff
}

check_index () {
	grep "^index $1\\.\\.$2" diff
}

check_mode () {
	grep "^old mode $1" diff &&
	grep "^new mode $2" diff
}

check_paths () {
	grep "^diff --git a/$1 b/$2" diff
}

test_expect_success 'create some blobs' '
	echo one >one &&
	echo two >two &&
	chmod +x two &&
	git add . &&

	# cover systems where modes are ignored
	git update-index --chmod=+x two &&

	git commit -m base &&

	sha1_one=$(git rev-parse HEAD:one) &&
	sha1_two=$(git rev-parse HEAD:two)
'

test_expect_success 'diff blob against file (git-diff)' '
	run_diff diff HEAD:one two
'
test_expect_success 'index of blob-file diff (git-diff)' '
	check_index $sha1_one $sha1_two
'
test_expect_success 'blob-file diff uses filename as paths (git-diff)' '
	check_paths one two
'
test_expect_success FILEMODE 'blob-file diff shows mode change (git-diff)' '
	check_mode 100644 100755
'

test_expect_success 'blob-file diff prefers filename to sha1 (git-diff)' '
	run_diff diff $sha1_one two &&
	check_paths two two
'

for cmd in $commands; do
	test_expect_success "diff by sha1 (git-$cmd)" '
		run_diff $cmd $sha1_one $sha1_two
	'
	test_expect_success "index of sha1 diff (git-$cmd)" '
		check_index $sha1_one $sha1_two
	'
	test_expect_success "sha1 diff uses arguments as paths (git-$cmd)" '
		check_paths $sha1_one $sha1_two
	'
	test_expect_success "sha1 diff has no mode change (git-$cmd)" '
		! grep mode diff
	'

	test_expect_success "diff by tree:path (run) (git-$cmd)" '
		run_diff $cmd HEAD:one HEAD:two
	'
	test_expect_success "index of tree:path diff (git-$cmd)" '
		check_index $sha1_one $sha1_two
	'
	test_expect_success "tree:path diff uses filenames as paths (git-$cmd)" '
		check_paths one two
	'
	test_expect_success "tree:path diff shows mode change (git-$cmd)" '
		check_mode 100644 100755
	'

	test_expect_success "diff by ranged tree:path (git-$cmd)" '
		run_diff $cmd HEAD:one..HEAD:two
	'
	test_expect_success "index of ranged tree:path diff (git-$cmd)" '
		check_index $sha1_one $sha1_two
	'
	test_expect_success "ranged tree:path diff uses filenames as paths (git-$cmd)" '
		check_paths one two
	'
	test_expect_success "ranged tree:path diff shows mode change (git-$cmd)" '
		check_mode 100644 100755
	'
done

test_done
