#!/bin/sh

test_description='object database backed by S3-compatible storage (SeaweedFS)'

. ./test-lib.sh
. "$TEST_DIRECTORY"/lib-s3.sh

start_weed

SEED=1

setup_repo () {
	SUFFIX=$(test-tool genrandom "$SEED" | tr -dc 'a-z' | test_copy_bytes 10)
	SEED=$(($SEED + 1))
	S3_ODB_URL="s3://$WEED_S3_ENDPOINT/$WEED_BUCKET/repo-$SUFFIX"

	test_when_finished "rm -rf $1" &&
	git init "$@" &&
	git -C "$1" config set core.repositoryFormatVersion 1 &&
	git -C "$1" config set extensions.objectStorage "$S3_ODB_URL" &&
	git -C "$1" config set maintenance.auto false
}

test_expect_success 'can open an empty repository' '
	setup_repo repo &&
	(
		cd repo &&
		git status
	)
'

test_expect_success 'can write blob and read it back' '
	setup_repo repo &&
	(
		cd repo &&
		echo "hello s3" >expect &&
		oid=$(git hash-object -w --stdin <expect) &&
		git cat-file blob "$oid" >actual &&
		test_cmp expect actual &&

		ls .git/objects/s3/manifests/ >manifests &&
		test_line_count = 1 manifests &&
		test_line_count = 1 ".git/objects/s3/manifests/$(cat manifests)" &&

		ls .git/objects/s3/packs/*.pack >packs &&
		test_line_count = 1 packs &&

		find .git/objects/ -path .git/objects/s3 -prune -o -type f -print >files &&
		test_must_be_empty files
	)
'

test_expect_success 'can write blob and read it back in bare repository' '
	setup_repo repo.git --bare &&
	(
		cd repo.git &&
		echo "hello s3" >expect &&
		oid=$(git hash-object -w --stdin <expect) &&
		git cat-file blob "$oid" >actual &&
		test_cmp expect actual &&

		ls objects/s3/manifests/ >manifests &&
		test_line_count = 1 manifests &&
		test_line_count = 1 "objects/s3/manifests/$(cat manifests)" &&

		ls objects/s3/packs/*.pack >packs &&
		test_line_count = 1 packs &&

		find objects/ -path objects/s3 -prune -o -type f -print >files &&
		test_must_be_empty files
	)
'

test_expect_success 'writing commits and logging them works end to end' '
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
		echo foobar >expect &&
		oid=$(git hash-object -w --stdin <expect) &&

		find .git/objects/s3/packs -type f >before &&
		test_line_count -ne 0 before &&
		rm -r .git/objects/s3 &&

		git cat-file -p "$oid" &&
		find .git/objects/s3/packs -type f >restored &&
		test_cmp before restored
	)
'

test_expect_success 'MVCC via manifests' '
	setup_repo repo &&
	(
		cd repo &&

		echo 1 >file1 &&
		oid1=$(git hash-object -w --stdin <file1) &&
		MANIFEST=$(basename .git/objects/s3/manifests/*) &&
		echo 2 >file2 &&
		oid2=$(git hash-object -w --stdin <file2) &&

		env GIT_S3_MANIFEST="$MANIFEST" git cat-file -p "$oid1" >actual &&
		test_cmp file1 actual &&
		test_must_fail env GIT_S3_MANIFEST="$MANIFEST" git cat-file -p "$oid2" 2>err &&
		test_grep "Not a valid object name $oid" err &&

		git count-objects -v >packs &&
		test_grep "packs: 2" packs &&
		env GIT_S3_MANIFEST="$MANIFEST" git count-objects -v >packs &&
		test_grep "packs: 1" packs
	)
'

test_expect_failure 'can enumerate all objects' '
	setup_repo repo &&
	(
		cd repo &&

		echo 1 >file1 &&
		git hash-object -w --stdin >expected &&
		echo 2 >file2 &&
		git hash-object -w --stdin >>expected &&

		# The batching logic uses bitmaps and thus expects the files backend.
		git cat-file --batch --batch-all-objects >actual &&
		test_cmp expected actual
	)
'

test_expect_success 'can push changes' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit first &&
		test_commit second &&
		test_commit third &&

		git init --bare target.git &&

		# We end up using bitmaps by default, and this causes us to break.
		test_must_fail git push ./target.git HEAD:branch 2>err &&
		test_grep "trying to downcast" err &&

		# But when disabling bitmaps we can push.
		git -c pack.useBitmaps=false push ./target.git HEAD:branch &&
		git log >expect &&
		git -C target.git log branch >actual &&
		test_cmp expect actual
	)
'

test_expect_success 'wrong credentials are rejected' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit initial &&
		test_must_fail env S3_KEY_SECRET=wrong git cat-file blob HEAD 2>err &&
		test_grep "s3: HTTP 403 fetching manifest pointer" err
	)
'

test_expect_success 'malformed S3 URL missing prefix is rejected' '
	setup_repo repo &&
	(
		cd repo &&
		git config set extensions.objectStorage "s3://$WEED_S3_ENDPOINT/$WEED_BUCKET" &&
		test_must_fail git cat-file blob HEAD 2>err &&
		test_grep "URL is missing repository prefix" err
	)
'

test_done
