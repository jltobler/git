#!/bin/sh

test_description='reference store backed by the MVCC manifest store'

. ./test-lib.sh

MVCC_URL="mvcc://"

setup_repo () {
	test_when_finished "rm -rf $1" &&
	env GIT_REFERENCE_BACKEND="$MVCC_URL" git init "$@"
}

mvcc_manifest_reftables () {
	sed -n 's/^r: //p' "$1"
}

test_expect_success 'new repository has HEAD reference' '
	setup_repo repo --initial-branch=foobar &&
	(
		cd repo &&
		ls .git/mvcc-cache/reftables/*.ref >tables &&
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
		ls .git/mvcc-cache/reftables/*.ref >tables &&
		test_line_count -ne 0 tables &&

		# All reftable files must have content-addressed names.
		for f in .git/mvcc-cache/reftables/*.ref
		do
			name=${f##*/} &&
			test "${#name}" = 68 &&
			expr "$name" : "[0-9a-f]\\{64\\}\\.ref\$" >/dev/null || {
				echo "non-sha-named reftable: $f" &&
				return 1
			}
		done &&

		# At least one manifest must carry an "r:" entry for the reftable.
		grep -r "^r:" .git/mvcc-cache/manifests/ &&

		# HEAD must resolve correctly via the MVCC-backed ref store.
		echo refs/heads/foobar >expect &&
		git symbolic-ref HEAD >actual &&
		test_cmp expect actual
	)
'

test_expect_success 'can write and read back a branch in a bare repository' '
	setup_repo repo.git --bare &&
	(
		cd repo.git &&

		ls mvcc-cache/reftables/*.ref >tables &&
		test_line_count = 1 tables &&

		EMPTY_TREE_OID=$(git hash-object -w --stdin -t tree </dev/null) &&
		COMMIT_OID=$(git commit-tree -m message "$EMPTY_TREE_OID") &&
		git update-ref refs/heads/branch "$COMMIT_OID" &&

		printf "%s commit\trefs/heads/branch\n" "$COMMIT_OID" >expect &&
		git refs list >actual &&
		test_cmp expect actual &&

		test_grep "^r:" mvcc-cache/manifests/*
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

test_expect_success 'missing prefetched reftable is reported clearly' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit first &&

		# Wipe the reftable cache to simulate a prefetch failure.
		rm -f .git/mvcc-cache/reftables/*.ref &&

		test_must_fail git log 2>err &&
		test_grep "reftable" err &&
		test_grep "is not available locally" err
	)
'

test_expect_success 'MVCC via manifests' '
	setup_repo repo &&
	(
		cd repo &&

		test_commit a &&
		MANIFEST_A=$(cat .git/mvcc-cache/manifest) &&
		test_commit b &&
		MANIFEST_B=$(cat .git/mvcc-cache/manifest) &&
		test_commit c &&
		MANIFEST_C=$(cat .git/mvcc-cache/manifest) &&

		git rev-parse HEAD~2 >expect &&
		env GIT_MVCC_MANIFEST="$MANIFEST_A" git rev-parse HEAD >actual &&
		test_cmp expect actual &&

		git rev-parse HEAD~1 >expect &&
		env GIT_MVCC_MANIFEST="$MANIFEST_B" git rev-parse HEAD >actual &&
		test_cmp expect actual &&

		git rev-parse HEAD >expect &&
		env GIT_MVCC_MANIFEST="$MANIFEST_C" git rev-parse HEAD >actual &&
		test_cmp expect actual
	)
'

test_expect_success 'ref writes leave the cache pointer untouched' '
	setup_repo repo &&
	(
		cd repo &&

		test_commit base &&
		BASE_MANIFEST=$(cat .git/mvcc-cache/manifest) &&

		cp .git/mvcc-cache/manifest cache-pointer-before &&

		manifest_path=$PWD/manifest-path &&
		cp .git/mvcc-cache/manifest "$manifest_path" &&

		env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git update-ref refs/heads/staged HEAD &&

		# Cache pointer must be unchanged.
		test_cmp cache-pointer-before .git/mvcc-cache/manifest &&

		# The external pointer must advance to a new manifest
		# distinct from the base.
		STAGED_MANIFEST=$(cat "$manifest_path") &&
		test "$STAGED_MANIFEST" != "$BASE_MANIFEST" &&
		test -f ".git/mvcc-cache/manifests/$STAGED_MANIFEST" &&

		# Reading with the external pointer must see the new ref.
		env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git rev-parse refs/heads/staged >/dev/null &&

		# After publishing the new pointer into the cache, the
		# new ref must be visible without the external pointer.
		cp "$manifest_path" .git/mvcc-cache/manifest &&
		git rev-parse refs/heads/staged >/dev/null
	)
'

test_reftable_count () {
	MANIFEST=$(cat .git/mvcc-cache/manifest) &&
	grep "^r: " .git/mvcc-cache/manifests/"$MANIFEST" >tables &&
	test_line_count "$@" tables
}

# Assert that the reftable stack referenced by the current manifest forms a
# geometric progression: each table must be at least twice as large as the
# next one. The manifest lists reftables in stack order (oldest first).
test_reftable_geometric () {
	test_reftable_count = "$1" &&
	MANIFEST=$(cat .git/mvcc-cache/manifest) &&
	grep "^r: " .git/mvcc-cache/manifests/"$MANIFEST" \
		| sed "s/^r: //" >tables.list &&
	prev= &&
	while read -r hash
	do
		size=$(wc -c <".git/mvcc-cache/reftables/$hash.ref") &&
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
		ls .git/mvcc-cache/reftables/*.ref >tables &&
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
		grep -h "^r: " .git/mvcc-cache/manifests/* | sort -u >entries &&
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

test_expect_success 'staged ref write extends base manifest with new reftable' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit base &&
		BASE_MANIFEST=$(cat .git/mvcc-cache/manifest) &&

		mvcc_manifest_reftables .git/mvcc-cache/manifests/"$BASE_MANIFEST" |
			sort >base-tables &&
		test_line_count -gt 0 base-tables &&

		manifest_path=$PWD/manifest-path &&
		cp .git/mvcc-cache/manifest "$manifest_path" &&

		env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git update-ref refs/heads/staged HEAD &&

		STAGED_MANIFEST=$(cat "$manifest_path") &&
		mvcc_manifest_reftables .git/mvcc-cache/manifests/"$STAGED_MANIFEST" |
			sort >staged-tables &&

		# Every base table must still be referenced.
		comm -23 base-tables staged-tables >dropped &&
		test_must_be_empty dropped &&

		# The staged manifest must add at least one new reftable.
		comm -13 base-tables staged-tables >added &&
		test_line_count -gt 0 added
	)
'

test_expect_success 'reftable compaction under staged pointer leaves cache pointer untouched' '
	setup_repo repo &&
	(
		cd repo &&

		git config set maintenance.auto false &&

		# Create enough reftables to trigger geometric compaction.
		test_commit initial &&
		for i in $(test_seq 10)
		do
			git update-ref refs/heads/a-$i HEAD || exit 1
		done &&

		BASE_MANIFEST=$(cat .git/mvcc-cache/manifest) &&
		mvcc_manifest_reftables .git/mvcc-cache/manifests/"$BASE_MANIFEST" |
			sort >base-tables &&

		# Snapshot the cache pointer.
		cp .git/mvcc-cache/manifest cache-pointer-before &&

		manifest_path=$PWD/manifest-path &&
		cp .git/mvcc-cache/manifest "$manifest_path" &&

		env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git refs optimize &&

		# Cache pointer must be unchanged.
		test_cmp cache-pointer-before .git/mvcc-cache/manifest &&

		# The staged manifest must list strictly fewer reftables.
		STAGED_MANIFEST=$(cat "$manifest_path") &&
		mvcc_manifest_reftables .git/mvcc-cache/manifests/"$STAGED_MANIFEST" |
			sort >staged-tables &&
		test "$(wc -l <staged-tables)" -lt "$(wc -l <base-tables)" &&

		# Every newly produced reftable must live in the cache
		# (writes are content-addressed and land there directly).
		comm -13 base-tables staged-tables >only-staged &&
		test_line_count -gt 0 only-staged &&
		while read -r table
		do
			if ! test -f .git/mvcc-cache/reftables/"$table".ref
			then
				echo "missing reftable $table in cache" &&
				return 1
			fi || return 1
		done <only-staged
	)
'

test_expect_success 'ref write under GIT_MVCC_MANIFEST pin is rejected' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit base &&
		MANIFEST=$(cat .git/mvcc-cache/manifest) &&

		test_must_fail env GIT_MVCC_MANIFEST="$MANIFEST" \
			git update-ref refs/heads/blocked HEAD 2>err &&
		test_grep "cannot write while GIT_MVCC_MANIFEST is set" err
	)
'

test_expect_success 'CAS catches stale base manifest when cache pointer races' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit base &&

		write_script .git/hooks/reference-transaction <<-\EOF &&
		if test "$1" = "prepared" && test -z "$MVCC_HOOK_FIRED"
		then
			printf "%064d\n" 0 >.git/mvcc-cache/manifest
			MVCC_HOOK_FIRED=1
			export MVCC_HOOK_FIRED
		fi
		EOF

		test_must_fail git update-ref refs/heads/racing HEAD 2>err &&
		test_grep "stale base manifest" err &&
		test_grep "another writer advanced the pointer" err
	)
'

test_expect_success 'CAS catches stale base manifest when external pointer races' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit base &&

		manifest_path=$PWD/manifest-path &&
		cp .git/mvcc-cache/manifest "$manifest_path" &&

		write_script .git/hooks/reference-transaction <<-EOF &&
		if test "\$1" = "prepared" && test -z "\$MVCC_HOOK_FIRED"
		then
			printf "%064d\n" 0 >"$manifest_path"
			MVCC_HOOK_FIRED=1
			export MVCC_HOOK_FIRED
		fi
		EOF

		test_must_fail env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git update-ref refs/heads/racing HEAD 2>err &&
		test_grep "stale base manifest" err &&
		test_grep "another writer advanced the pointer" err
	)
'

test_done
