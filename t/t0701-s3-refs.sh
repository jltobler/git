#!/bin/sh

test_description='reference store backed by S3-compatible storage'

. ./test-lib.sh
. "$TEST_DIRECTORY"/lib-s3.sh

start_weed

SEED=1

setup_repo () {
	SUFFIX=$(test-tool genrandom "$SEED" | tr -dc 'a-z' | test_copy_bytes 10)
	SEED=$(($SEED + 1))
	S3_PREFIX="$WEED_BUCKET/repo-$SUFFIX"
	S3_URL="s3://$WEED_S3_ENDPOINT/$S3_PREFIX"

	test_when_finished "rm -rf $1" &&
	env GIT_REFERENCE_BACKEND="$S3_URL" git init "$@"
}

test_expect_success 'new repository has HEAD reference' '
	setup_repo repo --initial-branch=foobar &&
	(
		cd repo &&
		ls .git/reftable/ >tables &&
		test_line_count = 1 tables &&
		echo "refs/heads/foobar" >expect &&
		git symbolic-ref HEAD >actual &&
		test_cmp expect actual
	)
'

test_expect_success 'can write and read back a branch' '
	setup_repo repo --initial-branch=foobar &&
	(
		cd repo &&
		test_commit first &&

		# At least one reftable file must have been written locally.
		ls .git/reftable/ >tables &&
		test_line_count = 2 tables &&

		# At least one manifest must carry an "r:" entry for the reftable.
		grep -r "^r:" .git/s3-cache/manifests/ &&

		# HEAD must resolve correctly via the S3-backed ref store.
		echo refs/heads/foobar >expect &&
		git symbolic-ref HEAD >actual &&
		test_cmp expect actual
	)
'

test_expect_success 'can write and read back a branch in a bare repository' '
	setup_repo repo.git --bare &&
	(
		cd repo.git &&

		ls reftable/ >tables &&
		test_line_count = 1 tables &&

		EMPTY_TREE_OID=$(git hash-object -w --stdin -t tree </dev/null) &&
		COMMIT_OID=$(git commit-tree -m message "$EMPTY_TREE_OID") &&
		git update-ref refs/heads/branch "$COMMIT_OID" &&

		printf "%s commit\trefs/heads/branch\n" "$COMMIT_OID" >expect &&
		git refs list >actual &&
		test_cmp expect actual &&

		test_grep "^r:" s3-cache/manifests/*
	)
'

test_expect_success 'writing commits and listing them works end to end' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit first &&
		test_commit second &&
		test_commit third &&
		cat >expect <<-\EOF &&
		third
		second
		first
		EOF
		git log --format=%s >actual &&
		test_cmp expect actual
	)
'

test_expect_success 'pruned caches are re-downloaded' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit first &&

		# Record which reftable files exist after the initial write.
		ls .git/reftable/*.ref >before &&
		test_line_count -ne 0 before &&

		# Wipe both the manifest cache and the local reftable files so
		# that the next git invocation must re-sync everything from S3.
		rm -rf .git/s3-cache .git/reftable &&

		git log --format=%s >actual &&
		echo first >expect &&
		test_cmp expect actual &&

		# The reftable files must have been re-downloaded.
		ls .git/reftable/*.ref >after &&
		test_cmp before after
	)
'

test_expect_success 'MVCC via manifests' '
	setup_repo repo &&
	(
		cd repo &&

		test_commit a &&
		MANIFEST_A=$(s3_fetch_manifest_version "$S3_URL") &&
		test_commit b &&
		MANIFEST_B=$(s3_fetch_manifest_version "$S3_URL") &&
		test_commit c &&
		MANIFEST_C=$(s3_fetch_manifest_version "$S3_URL") &&

		git rev-parse HEAD~2 >expect &&
		env GIT_S3_MANIFEST="$MANIFEST_A" git rev-parse HEAD >actual &&
		test_cmp expect actual &&

		git rev-parse HEAD~1 >expect &&
		env GIT_S3_MANIFEST="$MANIFEST_B" git rev-parse HEAD >actual &&
		test_cmp expect actual &&

		git rev-parse HEAD >expect &&
		env GIT_S3_MANIFEST="$MANIFEST_C" git rev-parse HEAD >actual &&
		test_cmp expect actual
	)
'

test_expect_success 'wrong credentials are rejected' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit initial &&
		test_must_fail env S3_KEY_SECRET=wrong git log 2>err &&
		test_grep "HTTP 403" err
	)
'

test_expect_success 'malformed S3 URL missing prefix is rejected' '
	setup_repo repo &&
	(
		cd repo &&
		git config set extensions.refStorage \
			"s3://$WEED_S3_ENDPOINT/$WEED_BUCKET" &&
		test_must_fail git log 2>err &&
		test_grep "URL is missing repository prefix" err
	)
'

test_done
