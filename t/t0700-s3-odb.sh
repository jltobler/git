#!/bin/sh

test_description='object database backed by S3-compatible storage'

. ./test-lib.sh
. "$TEST_DIRECTORY"/lib-s3.sh

start_s3_server

SEED=1

setup_repo () {
	SUFFIX=$(test-tool genrandom "$SEED" | tr -dc 'a-z' | test_copy_bytes 10)
	SEED=$(($SEED + 1))
	S3_URL="s3://$S3_ENDPOINT/$S3_BUCKET/repo-$SUFFIX"

	test_when_finished "rm -rf $1" &&
	git init "$@" &&
	git -C "$1" config set core.repositoryFormatVersion 1 &&
	git -C "$1" config set extensions.objectStorage "$S3_URL"
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
		oid=$(git hash-object -w expect) &&
		git cat-file blob "$oid" >actual &&
		test_cmp expect actual &&

		ls .git/s3-cache/manifests/ >manifests &&
		test_line_count = 1 manifests &&
		test_line_count = 1 ".git/s3-cache/manifests/$(cat manifests)" &&

		ls .git/s3-cache/packs/*.pack >packs &&
		test_line_count = 1 packs &&

		find .git/objects/ -path .git/s3-cache -prune -o -type f -print >files &&
		test_must_be_empty files
	)
'

test_expect_success 'can write blob and read it back in bare repository' '
	setup_repo repo.git --bare &&
	(
		cd repo.git &&
		echo "hello s3" >expect &&
		oid=$(git hash-object -w expect) &&
		git cat-file blob "$oid" >actual &&
		test_cmp expect actual &&

		ls s3-cache/manifests/ >manifests &&
		test_line_count = 1 manifests &&
		test_line_count = 1 "s3-cache/manifests/$(cat manifests)" &&

		ls s3-cache/packs/*.pack >packs &&
		test_line_count = 1 packs &&

		find objects/ -path s3-cache -prune -o -type f -print >files &&
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
		oid=$(git hash-object -w expect) &&

		find .git/s3-cache/packs -type f >before &&
		test_line_count -ne 0 before &&
		rm -r .git/s3-cache &&

		git cat-file -p "$oid" &&
		find .git/s3-cache/packs -type f >restored &&
		test_cmp before restored
	)
'

test_expect_success 'MVCC via manifests' '
	setup_repo repo &&
	(
		cd repo &&

		echo 1 >file1 &&
		oid1=$(git hash-object -w file1) &&
		MANIFEST=$(s3_fetch_manifest_version "$S3_URL") &&
		echo 2 >file2 &&
		oid2=$(git hash-object -w file2) &&

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

test_expect_success 'updating an MVCC snapshot is refused' '
	setup_repo repo &&
	(
		cd repo &&

		test_commit a &&
		MANIFEST_A=$(s3_fetch_manifest_version "$S3_URL") &&
		test_commit b &&
		MANIFEST_B=$(s3_fetch_manifest_version "$S3_URL") &&

		echo content >payload &&
		test_must_fail env GIT_S3_MANIFEST="$MANIFEST_A" \
			git hash-object -w payload 2>err &&
		test_grep "s3: conflicting write detected" err &&

		printf "%s" "$MANIFEST_B" >expect &&
		s3_fetch_manifest_version "$S3_URL" >actual &&
		test_cmp expect actual
	)
'

test_expect_failure 'can enumerate all objects' '
	setup_repo repo &&
	(
		cd repo &&

		{
			echo 1 | git hash-object -w --stdin &&
			echo 2 | git hash-object -w --stdin
		} >expected &&

		# The batching logic uses bitmaps and thus expects the files backend.
		git cat-file --batch --batch-all-objects >actual &&
		test_cmp expected actual
	)
'

test_expect_success 'can push changes into an S3-backed repo' '
	setup_repo target.git --bare &&
	test_when_finished "rm -rf source" &&
	git init source &&
	(
		cd source &&
		test_commit first &&
		test_commit second &&
		test_commit third &&

		git push --mirror ../target.git 2>err &&

		git refs list >expect &&
		git -C ../target.git refs list >actual &&
		test_cmp expect actual
	)
'

test_expect_success 'incremental pushes append packs to the manifest' '
	setup_repo target.git --bare &&
	test_when_finished "rm -rf source" &&
	git init source &&
	(
		cd source &&

		test_commit first &&
		git push --mirror ../target.git &&
		MANIFEST_FIRST=$(s3_fetch_manifest_version "$S3_URL") &&

		test_commit second &&
		git push --mirror ../target.git &&
		MANIFEST_SECOND=$(s3_fetch_manifest_version "$S3_URL") &&

		git refs list >expect &&
		git -C ../target.git refs list >actual &&
		test_cmp expect actual &&

		# Ensure that we can parse objects that should exist at a given
		# manifest state, and that we cannot parse those that do not
		# exist.
		env GIT_S3_MANIFEST="$MANIFEST_FIRST" git -C ../target.git cat-file -p first &&
		test_must_fail env GIT_S3_MANIFEST="$MANIFEST_FIRST" git -C ../target.git cat-file -p second &&
		env GIT_S3_MANIFEST="$MANIFEST_SECOND" git -C ../target.git cat-file -p first &&
		env GIT_S3_MANIFEST="$MANIFEST_SECOND" git -C ../target.git cat-file -p second
	)
'

test_expect_success 'pre-receive hook can see in-flight objects' '
	setup_repo target.git --bare &&
	test_when_finished "rm -rf source" &&

	# Install a pre-receive hook that resolves each new commit OID via
	# git cat-file. The hook runs after the pack has been written into
	# the S3 cache but before the manifest pointer in S3 has been
	# advanced; resolving the OID therefore depends on the env callback
	# exposing GIT_S3_MANIFEST so that the in-flight pack is visible.
	mkdir -p target.git/hooks &&
	write_script target.git/hooks/pre-receive <<-\EOF &&
	while read -r old new ref
	do
		git cat-file -p "$new" >/dev/null || exit 1
	done
	EOF

	git init source &&
	(
		cd source &&
		test_commit only &&
		git push ../target.git HEAD:refs/heads/branch
	)
'

test_expect_success 'geometric repacking consolidates small packs' '
	setup_repo repo &&
	(
		cd repo &&

		echo a | git hash-object -w --stdin &&
		echo b | git hash-object -w --stdin &&
		echo c | git hash-object -w --stdin &&
		echo d | git hash-object -w --stdin &&

		git count-objects -v >before &&
		test_grep "packs: 4" before &&

		git maintenance run --task=geometric-repack &&

		# All four packs should have been merged into one.
		git count-objects -v >after &&
		test_grep "packs: 1" after
	)
'

test_expect_success 'geometric repacking respects split factor' '
	setup_repo repo &&
	(
		cd repo &&

		# Write 5 objects and pack them, which should result in a
		# single pack: [5].
		echo a | git hash-object -w --stdin &&
		echo b | git hash-object -w --stdin &&
		echo c | git hash-object -w --stdin &&
		echo d | git hash-object -w --stdin &&
		echo e | git hash-object -w --stdin &&
		git count-objects -v >mid &&
		test_grep "packs: 5" mid &&
		git maintenance run --task=geometric-repack &&
		git count-objects -v >mid &&
		test_grep "packs: 1" mid &&

		# Add two more objects and pack them, which should result in
		# in two packs: [5, 1, 1] -> [5, 2].
		echo g | git hash-object -w --stdin &&
		echo h | git hash-object -w --stdin &&
		git count-objects -v >mid &&
		test_grep "packs: 3" mid &&
		git maintenance run --task=geometric-repack &&
		git count-objects -v >after &&
		test_grep "packs: 2" after &&

		# Add one more object and pack it. This should not result in a
		# change, as [5, 2, 1] is a geometric sequence already.
		echo i | git hash-object -w --stdin &&
		git count-objects -v >mid &&
		test_grep "packs: 3" mid &&
		git maintenance run --task=geometric-repack &&
		git count-objects -v >after &&
		test_grep "packs: 3" after &&

		# Add a final object and pack again. This should compress the
		# whole sequence: [5, 2, 1, 1] -> [9].
		echo j | git hash-object -w --stdin &&
		git count-objects -v >mid &&
		test_grep "packs: 4" mid &&
		git maintenance run --task=geometric-repack &&
		git count-objects -v >after &&
		test_grep "packs: 1" after
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
		git config set extensions.objectStorage "s3://$S3_ENDPOINT/$S3_BUCKET" &&
		test_must_fail git cat-file blob HEAD 2>err &&
		test_grep "URL is missing repository prefix" err
	)
'

test_done
