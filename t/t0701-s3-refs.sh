#!/bin/sh

test_description='reference store backed by S3-compatible storage'

. ./test-lib.sh
. "$TEST_DIRECTORY"/lib-s3.sh

start_s3_server

SEED=1

setup_repo () {
	SUFFIX=$(test-tool genrandom "$SEED" | tr -dc 'a-z' | test_copy_bytes 10)
	SEED=$(($SEED + 1))
	S3_PREFIX="$S3_BUCKET/repo-$SUFFIX"
	S3_URL="s3://$S3_ENDPOINT/$S3_PREFIX"

	test_when_finished "rm -rf $1" &&
	env GIT_REFERENCE_BACKEND="$S3_URL" git init "$@"
}

test_expect_success 'new repository has HEAD reference' '
	setup_repo repo --initial-branch=foobar &&
	(
		cd repo &&
		ls .git/s3-cache/reftables/*.ref >tables &&
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
		ls .git/s3-cache/reftables/*.ref >tables &&
		test_line_count -ne 0 tables &&

		# All reftable files must have content-addressed names.
		for f in .git/s3-cache/reftables/*.ref
		do
			name=${f##*/} &&
			test "${#name}" = 68 &&
			expr "$name" : "[0-9a-f]\\{64\\}\\.ref\$" >/dev/null || {
				echo "non-sha-named reftable: $f" &&
				return 1
			}
		done &&

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

		ls s3-cache/reftables/*.ref >tables &&
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

test_expect_success 'can delete a ref' '
	setup_repo repo &&
	(
		cd repo &&

		test_commit initial &&
		git update-ref refs/heads/to-delete HEAD &&
		git rev-parse refs/heads/to-delete >expect_oid &&
		git rev-parse HEAD >actual_oid &&
		test_cmp expect_oid actual_oid &&

		git update-ref -d refs/heads/to-delete &&
		test_must_fail git rev-parse --verify refs/heads/to-delete &&

		# A fresh process must also see the ref as gone, proving
		# that the deletion was published to S3 and is visible
		# through the manifest.
		rm -rf .git/s3-cache .git/reftable &&
		test_must_fail git rev-parse --verify refs/heads/to-delete
	)
'

test_expect_success 'can delete a ref with old-value check' '
	setup_repo repo &&
	(
		cd repo &&

		test_commit initial &&
		OID=$(git rev-parse HEAD) &&
		git update-ref refs/heads/checked HEAD &&

		# Deletion with the wrong old value must be refused.
		test_must_fail git update-ref -d refs/heads/checked $EMPTY_BLOB &&
		git rev-parse --verify refs/heads/checked >/dev/null &&

		# Deletion with the correct old value must succeed.
		git update-ref -d refs/heads/checked "$OID" &&
		test_must_fail git rev-parse --verify refs/heads/checked
	)
'

test_expect_success 'can rename a ref' '
	setup_repo repo &&
	(
		cd repo &&

		test_commit initial &&
		git update-ref refs/heads/old-name HEAD &&
		OID=$(git rev-parse refs/heads/old-name) &&

		git branch -m old-name new-name &&

		test_must_fail git rev-parse --verify refs/heads/old-name &&
		git rev-parse --verify refs/heads/new-name >actual &&
		echo "$OID" >expect &&
		test_cmp expect actual &&

		# Verify the rename is durable across a fresh cache.
		rm -rf .git/s3-cache .git/reftable &&
		test_must_fail git rev-parse --verify refs/heads/old-name &&
		git rev-parse --verify refs/heads/new-name >actual &&
		test_cmp expect actual
	)
'

test_expect_success 'can copy a ref' '
	setup_repo repo &&
	(
		cd repo &&

		test_commit initial &&
		git update-ref refs/heads/source HEAD &&
		OID=$(git rev-parse refs/heads/source) &&

		git branch -c source destination &&

		git rev-parse --verify refs/heads/source >src &&
		git rev-parse --verify refs/heads/destination >dst &&
		echo "$OID" >expect &&
		test_cmp expect src &&
		test_cmp expect dst
	)
'

test_expect_success 'can update symbolic refs' '
	setup_repo repo --initial-branch=main &&
	(
		cd repo &&

		test_commit initial &&

		# HEAD starts as a symbolic ref to refs/heads/main.
		echo refs/heads/main >expect &&
		git symbolic-ref HEAD >actual &&
		test_cmp expect actual &&

		# Create a second branch and re-point HEAD at it.
		git update-ref refs/heads/other HEAD &&
		git symbolic-ref HEAD refs/heads/other &&

		echo refs/heads/other >expect &&
		git symbolic-ref HEAD >actual &&
		test_cmp expect actual &&

		# The symbolic ref change must be observable from a fresh
		# cache too.
		rm -rf .git/s3-cache .git/reftable &&
		git symbolic-ref HEAD >actual &&
		test_cmp expect actual
	)
'

test_expect_success 'can delete a symbolic ref' '
	setup_repo repo &&
	(
		cd repo &&

		test_commit initial &&
		git symbolic-ref refs/heads/alias refs/heads/master &&

		echo refs/heads/master >expect &&
		git symbolic-ref refs/heads/alias >actual &&
		test_cmp expect actual &&

		git symbolic-ref --delete refs/heads/alias &&
		test_must_fail git symbolic-ref refs/heads/alias
	)
'

test_expect_success 'can list refs after mixed updates' '
	setup_repo repo &&
	(
		cd repo &&

		test_commit initial &&
		OID=$(git rev-parse HEAD) &&

		git update-ref refs/heads/keep HEAD &&
		git update-ref refs/heads/temp HEAD &&
		git update-ref refs/heads/source HEAD &&
		git update-ref -d refs/heads/temp &&
		git branch -m source renamed &&
		git tag -a -m annotated annotated-tag HEAD &&

		git for-each-ref --format="%(refname)" refs/ | sort >actual &&
		cat >expect <<-\EOF &&
		refs/heads/keep
		refs/heads/master
		refs/heads/renamed
		refs/tags/annotated-tag
		refs/tags/initial
		EOF
		test_cmp expect actual
	)
'

test_expect_success 'pruned caches are re-downloaded' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit first &&

		# Wipe the entire local cache so that the next git invocation
		# must re-sync everything from S3.
		rm -rf .git/s3-cache &&

		# The commit data must still be reachable via the freshly
		# rebuilt cache.
		git log --format=%s >actual &&
		echo first >expect &&
		test_cmp expect actual &&

		# At least one reftable file must have been re-downloaded.
		ls .git/s3-cache/reftables/*.ref >after &&
		test_line_count -ne 0 after
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

test_expect_success 'updating an MVCC snapshot is refused' '
	setup_repo repo &&
	(
		cd repo &&

		test_commit a &&
		MANIFEST_A=$(s3_fetch_manifest_version "$S3_URL") &&
		test_commit b &&
		MANIFEST_B=$(s3_fetch_manifest_version "$S3_URL") &&

		test_must_fail env GIT_S3_MANIFEST="$MANIFEST_A" \
			git update-ref refs/heads/branch HEAD 2>err &&
		test_grep "s3: conflicting write detected" err &&

		printf "%s" "$MANIFEST_B" >expect &&
		s3_fetch_manifest_version "$S3_URL" >actual &&
		test_cmp expect actual
	)
'

test_expect_success 'concurrent ref update is rejected via CAS' '
	setup_repo repo &&
	(
		cd repo &&

		test_commit init &&

		# Simulate a concurrent write by overwriting the manifest
		# pointer in the reference-transaction hook.
		write_script .git/hooks/reference-transaction <<-EOF &&
		if test "\$1" = prepared
		then
			echo dummy-manifest-version |
			curl --silent --show-error --fail \
				--aws-sigv4 "aws:amz:us-east-1:s3" \
				--user "$S3_KEY_ID:$S3_KEY_SECRET" \
				--upload-file - \
				"${S3_URL#s3://}/manifest"
		fi
		EOF

		test_must_fail git update-ref refs/heads/branch HEAD 2>err &&
		test_grep "s3: conflicting write detected" err &&

		echo "dummy-manifest-version" >expect &&
		s3_fetch_manifest_version "$S3_URL" >actual &&
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

test_reftable_count () {
	MANIFEST=$(s3_fetch_manifest_version "$S3_URL") &&
	grep '^r: ' .git/s3-cache/manifests/"$MANIFEST" >tables &&
	test_line_count "$@" tables
}

# Assert that the reftable stack referenced by the current manifest forms a
# geometric progression: each table must be at least twice as large as the
# next one. The manifest lists reftables in stack order (oldest first).
test_reftable_geometric () {
	test_reftable_count = "$1" &&
	MANIFEST=$(s3_fetch_manifest_version "$S3_URL") &&
	grep '^r: ' .git/s3-cache/manifests/"$MANIFEST" \
		| sed 's/^r: //' >tables.list &&
	prev= &&
	while read -r hash
	do
		size=$(wc -c <".git/s3-cache/reftables/$hash.ref") &&
		if test -n "$prev" && test "$prev" -lt "$((size * 2))"
		then
			echo "stack not geometric: $prev followed by $size"
			return 1
		fi &&
		prev=$size || return 1
	done <tables.list
}

test_expect_success 'optimize uses geometric sequence' '
	setup_repo repo &&
	(
		cd repo &&

		git config set maintenance.auto false &&

		test_commit initial &&
		test_reftable_count = 3 &&

		for i in $(test_seq 10)
		do
			git update-ref refs/heads/a-$i HEAD || exit 1
		done &&
		test_reftable_count = 13 &&
		git refs optimize &&
		test_reftable_geometric 1 &&

		for i in $(test_seq 50)
		do
			git update-ref refs/heads/b-$i HEAD || exit 1
		done &&
		test_reftable_count = 51 &&
		git refs optimize &&
		test_reftable_geometric 1 &&

		for i in $(test_seq 5)
		do
			git update-ref refs/heads/c-$i HEAD || exit 1
		done &&
		test_reftable_count = 6 &&
		git refs optimize &&
		test_reftable_geometric 2
	)
'

test_expect_success 'reftables are content-addressed' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit first &&
		test_commit second &&

		# After several commits there must be at least one reftable
		# and every file in the cache directory must be named after
		# its sha256 content hash.
		ls .git/s3-cache/reftables/*.ref >tables &&
		test_line_count -ne 0 tables &&

		while read -r table
		do
			name=${table##*/} &&
			expected=$(test-tool sha256 <"$table") &&
			actual=${name%.ref} &&
			test "$actual" = "$expected" || {
				echo "table $table: expected $expected but named $actual" &&
				return 1
			}
		done <tables &&

		# Every "r:" entry in the manifest must be a 64-char hex
		# string with no ".ref" suffix.
		grep -h "^r: " .git/s3-cache/manifests/* | sort -u >entries &&
		test_line_count -ne 0 entries &&

		while read -r line
		do
			value=${line#r: } &&
			test "${#value}" = 64 &&
			expr "$value" : "[0-9a-f]\\{64\\}\$" >/dev/null || {
				echo "non-sha manifest entry: $line" &&
				return 1
			}
		done <entries
	)
'

test_expect_success 'malformed S3 URL missing prefix is rejected' '
	setup_repo repo &&
	(
		cd repo &&
		git config set extensions.refStorage \
			"s3://$S3_ENDPOINT/$S3_BUCKET" &&
		test_must_fail git log 2>err &&
		test_grep "URL is missing repository prefix" err
	)
'

test_done
