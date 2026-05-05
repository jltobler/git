#!/bin/sh

test_description='object database and reference store both backed by S3-compatible storage'

. ./test-lib.sh
. "$TEST_DIRECTORY"/lib-s3.sh

start_weed

SEED=1

setup_repo () {
	SUFFIX=$(test-tool genrandom "$SEED" | tr -dc 'a-z' | test_copy_bytes 10)
	SEED=$(($SEED + 1))
	S3_URL="s3://$WEED_S3_ENDPOINT/$WEED_BUCKET/repo-$SUFFIX"

	test_when_finished "rm -rf $1" &&
	env GIT_REFERENCE_BACKEND="$S3_URL" git init "$@" &&
	git -C "$1" config set core.repositoryFormatVersion 1 &&
	git -C "$1" config set extensions.objectStorage "$S3_URL"
}

test_expect_success 'can open an empty repository' '
	setup_repo repo &&
	(
		cd repo &&
		git status &&
		ls .git/reftable/ >tables &&
		test_line_count -ne 0 tables &&
		test -d .git/s3-cache/manifests
	)
'

test_expect_success 'can write a commit and read it back' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit first &&

		echo first >expect &&
		git log --format=%s >actual &&
		test_cmp expect actual &&

		# No loose objects or local packfiles must have been written;
		# everything lives in the S3 cache instead.
		find .git/objects/ -path .git/s3-cache -prune -o -type f -print >files &&
		test_must_be_empty files &&

		# At least one reftable file must have been written locally.
		ls .git/reftable/*.ref >tables &&
		test_line_count -ne 0 tables &&

		# The combined manifest must carry entries for both packs and
		# reftables, demonstrating that both backends share a manifest.
		grep -h "^p:" .git/s3-cache/manifests/* &&
		grep -h "^r:" .git/s3-cache/manifests/*
	)
'

test_expect_success 'works in a bare repository' '
	setup_repo repo.git --bare &&
	(
		cd repo.git &&

		ls reftable/ >tables &&
		test_line_count -ne 0 tables &&

		EMPTY_TREE_OID=$(git hash-object -w --stdin -t tree </dev/null) &&
		COMMIT_OID=$(git commit-tree -m message "$EMPTY_TREE_OID") &&
		git update-ref refs/heads/branch "$COMMIT_OID" &&

		printf "%s commit\trefs/heads/branch\n" "$COMMIT_OID" >expect &&
		git refs list >actual &&
		test_cmp expect actual &&

		grep -h "^p:" s3-cache/manifests/* &&
		grep -h "^r:" s3-cache/manifests/*
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
		test_commit first &&

		# Make sure both backends have populated their local
		# caches before we wipe.
		ls .git/reftable/*.ref >tables-before &&
		test_line_count -ne 0 tables-before &&
		ls .git/s3-cache/packs/*.pack >packs-before &&
		test_line_count -ne 0 packs-before &&

		# Wipe both caches so that the next git invocation must
		# re-sync everything from S3.
		rm -rf .git/s3-cache .git/reftable &&

		# The commit data must still be reachable via the freshly
		# rebuilt caches.
		echo first >expect &&
		git log --format=%s >actual &&
		test_cmp expect actual &&

		# Both subsystems must have repopulated their local
		# caches as a side effect.
		ls .git/reftable/*.ref >tables-after &&
		test_line_count -ne 0 tables-after &&
		ls .git/s3-cache/packs/*.pack >packs-after &&
		test_line_count -ne 0 packs-after
	)
'

test_expect_success 'MVCC pins both refs and objects' '
	setup_repo repo &&
	(
		cd repo &&

		test_commit a &&
		MANIFEST_A=$(s3_fetch_manifest_version "$S3_URL") &&
		test_commit b &&
		MANIFEST_B=$(s3_fetch_manifest_version "$S3_URL") &&
		test_commit c &&

		OID_A=$(git rev-parse a) &&
		OID_C=$(git rev-parse c) &&

		# Pinning to manifest A: HEAD must resolve to "a".
		echo "$OID_A" >expect &&
		env GIT_S3_MANIFEST="$MANIFEST_A" git rev-parse HEAD >actual &&
		test_cmp expect actual &&

		# Pinning to manifest B: log shows b then a, but not c.
		cat >expect <<-\EOF &&
		b
		a
		EOF
		env GIT_S3_MANIFEST="$MANIFEST_B" git log --format=%s >actual &&
		test_cmp expect actual &&

		# At manifest A, commit c is not yet visible as an object.
		test_must_fail env GIT_S3_MANIFEST="$MANIFEST_A" \
			git cat-file -p "$OID_C"
	)
'

test_expect_success 'storage handle is shared between backends' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit first &&

		# A single shared cache directory must exist; both
		# backends must populate it rather than create separate
		# trees.
		find .git -maxdepth 2 -type d -name "s3-cache*" >caches &&
		test_line_count = 1 caches &&

		test -d .git/s3-cache/packs &&
		test -d .git/s3-cache/manifests &&

		# That single cache must reflect both backends in a
		# single manifest version.
		ls .git/s3-cache/manifests >manifests &&
		test_line_count -ne 0 manifests &&
		grep -lh "^p:" .git/s3-cache/manifests/* >with-packs &&
		grep -lh "^r:" .git/s3-cache/manifests/* >with-reftables &&
		comm -12 with-packs with-reftables >shared &&
		test_line_count -ne 0 shared
	)
'

test_expect_success 'push from a dual-S3 repository' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit first &&
		test_commit second &&
		test_commit third &&

		git init --bare ../target.git &&

		# Bitmaps trigger a known downcast issue in the S3 ODB
		# source, so disable them for the push (matching t0700).
		git -c pack.useBitmaps=false push ../target.git HEAD:branch &&

		git log >expect &&
		git -C ../target.git log branch >actual &&
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

test_done
