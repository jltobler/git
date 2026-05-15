#!/bin/sh

test_description='object database backed by the MVCC manifest store'

. ./test-lib.sh

MVCC_URL="mvcc://"

setup_repo () {
	test_when_finished "rm -rf $1" &&
	git init "$@" &&
	git -C "$1" config set core.repositoryFormatVersion 1 &&
	git -C "$1" config set extensions.objectStorage "$MVCC_URL"
}

mvcc_manifest_packs () {
	sed -n 's/^p: //p' "$1"
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
		echo "hello mvcc" >expect &&
		oid=$(git hash-object -w expect) &&
		git cat-file blob "$oid" >actual &&
		test_cmp expect actual &&

		ls .git/mvcc-cache/manifests/ >manifests &&
		test_line_count = 1 manifests &&
		test_line_count = 1 ".git/mvcc-cache/manifests/$(cat manifests)" &&

		ls .git/mvcc-cache/pack/*.pack >packs &&
		test_line_count = 1 packs &&

		find .git/objects/ -path .git/mvcc-cache -prune -o -type f -print >files &&
		test_must_be_empty files
	)
'

test_expect_success 'can write blob and read it back in bare repository' '
	setup_repo repo.git --bare &&
	(
		cd repo.git &&
		echo "hello mvcc" >expect &&
		oid=$(git hash-object -w expect) &&
		git cat-file blob "$oid" >actual &&
		test_cmp expect actual &&

		ls mvcc-cache/manifests/ >manifests &&
		test_line_count = 1 manifests &&
		test_line_count = 1 "mvcc-cache/manifests/$(cat manifests)" &&

		ls mvcc-cache/pack/*.pack >packs &&
		test_line_count = 1 packs &&

		find objects/ -path mvcc-cache -prune -o -type f -print >files &&
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

test_expect_success 'missing prefetched pack is reported clearly' '
	setup_repo repo &&
	(
		cd repo &&
		echo content >payload &&
		oid=$(git hash-object -w payload) &&

		# Simulate a corrupted/missing prefetch by removing the
		# pack files referenced by the current manifest.
		rm -f .git/mvcc-cache/pack/*.pack \
		      .git/mvcc-cache/pack/*.idx \
		      .git/mvcc-cache/pack/*.rev &&

		test_must_fail git cat-file -p "$oid" 2>err &&
		test_grep "is not available locally" err
	)
'

test_expect_success 'MVCC via manifests' '
	setup_repo repo &&
	(
		cd repo &&

		echo 1 >file1 &&
		oid1=$(git hash-object -w file1) &&
		MANIFEST=$(cat .git/mvcc-cache/manifest) &&
		echo 2 >file2 &&
		oid2=$(git hash-object -w file2) &&

		env GIT_MVCC_MANIFEST="$MANIFEST" git cat-file -p "$oid1" >actual &&
		test_cmp file1 actual &&
		test_must_fail env GIT_MVCC_MANIFEST="$MANIFEST" git cat-file -p "$oid2" 2>err &&
		test_grep "Not a valid object name $oid2" err &&

		git count-objects -v >packs &&
		test_grep "packs: 2" packs &&
		env GIT_MVCC_MANIFEST="$MANIFEST" git count-objects -v >packs &&
		test_grep "packs: 1" packs
	)
'

test_expect_success 'writes leave the cache pointer untouched' '
	setup_repo repo &&
	(
		cd repo &&

		# Establish a baseline manifest in the cache.
		test_commit base &&
		BASE_MANIFEST=$(cat .git/mvcc-cache/manifest) &&

		# Snapshot the cache pointer before invoking the staged write.
		cp .git/mvcc-cache/manifest cache-pointer-before &&

		# A write performed under an external manifest pointer
		# must advance only that pointer; the cache pointer must
		# remain unchanged.
		manifest_path=$PWD/manifest-path &&
		cp .git/mvcc-cache/manifest "$manifest_path" &&
		echo staged >payload &&
		oid=$(env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git hash-object -w payload) &&

		test_cmp cache-pointer-before .git/mvcc-cache/manifest &&

		# The external pointer must now name a new manifest body
		# distinct from BASE_MANIFEST, and the body must live in
		# the cache.
		STAGED_MANIFEST=$(cat "$manifest_path") &&
		test "$STAGED_MANIFEST" != "$BASE_MANIFEST" &&
		test -f ".git/mvcc-cache/manifests/$STAGED_MANIFEST" &&

		# Reading with the external pointer must see the new
		# object.
		env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git cat-file -p "$oid" >actual &&
		test_cmp payload actual &&

		# After the orchestrator publishes the new pointer into
		# the cache, the new state must be visible without the
		# external pointer.
		cp "$manifest_path" .git/mvcc-cache/manifest &&
		git cat-file -p "$oid" >actual &&
		test_cmp payload actual
	)
'

test_expect_success 'concurrent activate is mutually exclusive' '
	setup_repo repo &&
	test_when_finished "rm -f repo/.git/mvcc-cache/manifest.lock" &&
	(
		cd repo &&

		test_commit base &&

		# Hold the manifest pointer lock and verify that a
		# concurrent writer fails fast rather than racing with us.
		: >.git/mvcc-cache/manifest.lock &&
		echo payload >file &&
		test_must_fail git hash-object -w file 2>err &&
		test_grep "manifest" err
	)
'

test_expect_success 'can push changes into an MVCC-backed repo' '
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
		MANIFEST_FIRST=$(cat ../target.git/mvcc-cache/manifest) &&

		test_commit second &&
		git push --mirror ../target.git &&
		MANIFEST_SECOND=$(cat ../target.git/mvcc-cache/manifest) &&

		git refs list >expect &&
		git -C ../target.git refs list >actual &&
		test_cmp expect actual &&

		# Ensure that we can parse objects that should exist at a given
		# manifest state, and that we cannot parse those that do not
		# exist.
		env GIT_MVCC_MANIFEST="$MANIFEST_FIRST" git -C ../target.git cat-file -p first &&
		test_must_fail env GIT_MVCC_MANIFEST="$MANIFEST_FIRST" git -C ../target.git cat-file -p second &&
		env GIT_MVCC_MANIFEST="$MANIFEST_SECOND" git -C ../target.git cat-file -p first &&
		env GIT_MVCC_MANIFEST="$MANIFEST_SECOND" git -C ../target.git cat-file -p second
	)
'

test_expect_success 'pre-receive hook can see in-flight objects' '
	setup_repo target.git --bare &&
	test_when_finished "rm -rf source" &&

	# Install a pre-receive hook that resolves each new commit OID via
	# git cat-file. The hook runs after the pack has been written into
	# the MVCC cache but before the manifest pointer has been advanced;
	# resolving the OID therefore depends on the env callback exposing
	# GIT_MVCC_MANIFEST so that the in-flight pack is visible.
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

test_expect_success 'geometric repacking maintains a progression of packs' '
	setup_repo repo &&
	(
		cd repo &&

		# Geometric packing weighs packs by on-disk byte size. To
		# differentiate the resulting pack sizes we give each blob
		# multi-line content that resists trivial compression.
		blob () {
			tag=$1 &&
			for i in 1 2 3 4 5 6 7 8 9 0
			do
				printf "blob-%s line %d with distinct content\n" \
					"$tag" "$i" || return 1
			done | git hash-object -w --stdin >/dev/null
		} &&

		# Write 5 objects and pack them, which should result in a
		# single pack.
		blob a && blob b && blob c && blob d && blob e &&
		git count-objects -v >mid &&
		test_grep "packs: 5" mid &&
		git maintenance run --task=geometric-repack &&
		git count-objects -v >mid &&
		test_grep "packs: 1" mid &&

		# Add a small object. With one big pack and one tiny pack
		# the progression is satisfied (big >= 2 * tiny), so the
		# next compaction is a no-op.
		echo g | git hash-object -w --stdin &&
		git count-objects -v >mid &&
		test_grep "packs: 2" mid &&
		git maintenance run --task=geometric-repack &&
		git count-objects -v >after &&
		test_grep "packs: 2" after &&

		# Adding another small object violates the progression
		# between the two tiny packs and rolls them into one.
		echo h | git hash-object -w --stdin &&
		git count-objects -v >mid &&
		test_grep "packs: 3" mid &&
		git maintenance run --task=geometric-repack &&
		git count-objects -v >after &&
		test_grep "packs: 2" after
	)
'

test_expect_success 'staged pack write extends base manifest with new pack' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit base &&
		BASE_MANIFEST=$(cat .git/mvcc-cache/manifest) &&

		# Snapshot the base packs.
		mvcc_manifest_packs .git/mvcc-cache/manifests/"$BASE_MANIFEST" |
			sort >base-packs &&
		test_line_count -gt 0 base-packs &&

		manifest_path=$PWD/manifest-path &&
		cp .git/mvcc-cache/manifest "$manifest_path" &&
		echo extra >payload &&
		env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git hash-object -w payload &&

		STAGED_MANIFEST=$(cat "$manifest_path") &&
		mvcc_manifest_packs .git/mvcc-cache/manifests/"$STAGED_MANIFEST" |
			sort >staged-packs &&

		# The staged manifest must be a strict superset of the base.
		comm -23 base-packs staged-packs >only-in-base &&
		test_must_be_empty only-in-base &&
		comm -13 base-packs staged-packs >only-in-staged &&
		test_line_count = 1 only-in-staged
	)
'

test_expect_success 'staged write makes new pack readable in the same process' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit base &&
		BASE_MANIFEST=$(cat .git/mvcc-cache/manifest) &&
		mvcc_manifest_packs .git/mvcc-cache/manifests/"$BASE_MANIFEST" |
			sort >base-packs &&

		manifest_path=$PWD/manifest-path &&
		cp .git/mvcc-cache/manifest "$manifest_path" &&

		# A new object written under the external pointer must be
		# readable in the same invocation.
		echo staged-only >payload &&
		oid=$(env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git hash-object -w payload) &&
		env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git cat-file -p "$oid" >actual &&
		test_cmp payload actual &&

		# The new pack must be referenced by the staged manifest
		# but not by the cache manifest.
		STAGED_MANIFEST=$(cat "$manifest_path") &&
		mvcc_manifest_packs .git/mvcc-cache/manifests/"$STAGED_MANIFEST" |
			sort >staged-packs &&
		comm -13 base-packs staged-packs >only-staged &&
		test_line_count = 1 only-staged
	)
'

test_expect_success 'an existing external pointer is treated as a continuation' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit base &&

		manifest_path=$PWD/manifest-path &&
		cp .git/mvcc-cache/manifest "$manifest_path" &&

		echo first >payload1 &&
		oid1=$(env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git hash-object -w payload1) &&
		AFTER_FIRST=$(cat "$manifest_path") &&
		mvcc_manifest_packs .git/mvcc-cache/manifests/"$AFTER_FIRST" |
			sort >after-first-packs &&

		# Run a second Git invocation against the same external
		# pointer; it must see the first invocation`s state and
		# build on top of it.
		env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git cat-file -p "$oid1" >actual &&
		test_cmp payload1 actual &&

		echo second >payload2 &&
		oid2=$(env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git hash-object -w payload2) &&
		AFTER_SECOND=$(cat "$manifest_path") &&
		test "$AFTER_FIRST" != "$AFTER_SECOND" &&

		mvcc_manifest_packs .git/mvcc-cache/manifests/"$AFTER_SECOND" |
			sort >after-second-packs &&

		# Every pack present after the first write must still be in
		# the manifest after the second write.
		comm -23 after-first-packs after-second-packs >dropped &&
		test_must_be_empty dropped &&
		comm -13 after-first-packs after-second-packs >added &&
		test_line_count = 1 added &&

		# Both objects must remain visible to the staged view.
		env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git cat-file -p "$oid1" >actual &&
		test_cmp payload1 actual &&
		env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git cat-file -p "$oid2" >actual &&
		test_cmp payload2 actual
	)
'

test_expect_success 'geometric repack under staged pointer leaves cache pointer untouched' '
	setup_repo repo &&
	(
		cd repo &&

		# Seed several packs in the cache so that repack has work
		# to do.
		echo a | git hash-object -w --stdin &&
		echo b | git hash-object -w --stdin &&
		echo c | git hash-object -w --stdin &&
		echo d | git hash-object -w --stdin &&

		BASE_MANIFEST=$(cat .git/mvcc-cache/manifest) &&
		mvcc_manifest_packs .git/mvcc-cache/manifests/"$BASE_MANIFEST" |
			sort >base-packs &&
		cp .git/mvcc-cache/manifest cache-pointer-before &&

		manifest_path=$PWD/manifest-path &&
		cp .git/mvcc-cache/manifest "$manifest_path" &&

		env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git maintenance run --task=geometric-repack &&

		# Cache pointer must be unchanged.
		test_cmp cache-pointer-before .git/mvcc-cache/manifest &&

		# The staged manifest must list strictly fewer packs (the
		# rolled-up merged pack replaces several base packs).
		STAGED_MANIFEST=$(cat "$manifest_path") &&
		mvcc_manifest_packs .git/mvcc-cache/manifests/"$STAGED_MANIFEST" |
			sort >staged-packs &&
		test "$(wc -l <staged-packs)" -lt "$(wc -l <base-packs)" &&

		# The new merged pack must live in the cache (writes are
		# content-addressed and land there directly) and be
		# referenced only by the staged manifest.
		comm -13 base-packs staged-packs >only-staged &&
		test_line_count = 1 only-staged &&
		merged=$(cat only-staged) &&
		test -f .git/mvcc-cache/pack/"$merged".pack
	)
'

test_expect_success 'pointer referencing missing manifest body fails clearly' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit base &&

		# Overwrite the pointer with a hash that does not name any
		# manifest body in the cache.
		BOGUS=0000000000000000000000000000000000000000000000000000000000000000 &&
		printf "%s\n" "$BOGUS" >.git/mvcc-cache/manifest &&

		test_must_fail git log 2>err &&
		test_grep "manifest body" err &&
		test_grep "not available locally" err
	)
'

test_expect_success 'malformed manifest body line is rejected' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit base &&

		# Forge a manifest body with an unknown directive.
		printf "x: garbage\n" >.git/mvcc-cache/manifests/bogus.body &&
		body_hash=$(test-tool sha256 <.git/mvcc-cache/manifests/bogus.body) &&
		mv .git/mvcc-cache/manifests/bogus.body \
			".git/mvcc-cache/manifests/$body_hash" &&
		printf "%s\n" "$body_hash" >.git/mvcc-cache/manifest &&

		test_must_fail git log 2>err &&
		test_grep "malformed manifest" err
	)
'

test_expect_success 'manifest pointer file without trailing newline still resolves' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit base &&

		# Rewrite the pointer to omit the trailing newline.
		printf "%s" "$(cat .git/mvcc-cache/manifest)" >.git/mvcc-cache/manifest &&

		git log --format=%s >actual &&
		echo base >expect &&
		test_cmp expect actual
	)
'

test_expect_success 'empty manifest activation has sha256("") version hash' '
	setup_repo repo &&
	(
		cd repo &&

		# Force the MVCC storage to materialise its cache layout.
		git status >/dev/null &&

		# An empty manifest body has the SHA-256 of the empty
		# string as its content-addressed name.
		EMPTY_HASH=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 &&

		: >.git/mvcc-cache/manifests/"$EMPTY_HASH" &&
		printf "%s\n" "$EMPTY_HASH" >.git/mvcc-cache/manifest &&

		# The repository must be openable and contain zero objects.
		git count-objects -v >actual &&
		test_grep "packs: 0" actual
	)
'

test_expect_success 'unknown object storage schema is rejected with a clear error' '
	setup_repo repo &&
	(
		cd repo &&
		# Replace the schema with one that has no corresponding
		# ODB source factory; this must surface a clear error.
		git config set extensions.objectStorage "bogus-scheme://" &&
		test_must_fail git status 2>err &&
		test_grep "unknown object database source schema" err
	)
'

test_expect_success 'concurrent activate is mutually exclusive with external pointer' '
	setup_repo repo &&
	manifest_path=$PWD/manifest-path-lock &&
	test_when_finished "rm -f $manifest_path $manifest_path.lock" &&
	(
		cd repo &&
		# Establish a base manifest in the cache so we have a hash
		# to seed the external pointer with.
		test_commit base &&
		cp .git/mvcc-cache/manifest "$manifest_path" &&
		: >"$manifest_path.lock" &&
		echo payload >file &&
		test_must_fail env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git hash-object -w file 2>err &&
		test_grep "manifest" err
	)
'

test_expect_success 'all cache subdirectories are created at init time' '
	setup_repo repo &&
	(
		cd repo &&
		# Force initialisation of the MVCC storage by issuing any
		# command that touches the ODB.
		git status >/dev/null &&

		for sub in pack reftables manifests
		do
			test -d .git/mvcc-cache/$sub ||
			{ echo "missing cache subdir $sub" && return 1; }
		done &&

		# Subdirs must initially be empty (no writes performed yet).
		find .git/mvcc-cache/pack -type f >files &&
		test_must_be_empty files
	)
'

test_expect_success 'every p: entry has co-resident .pack, .idx, .rev files' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit one &&
		test_commit two &&

		MANIFEST=$(cat .git/mvcc-cache/manifest) &&
		mvcc_manifest_packs .git/mvcc-cache/manifests/"$MANIFEST" >packs &&
		test_line_count -gt 0 packs &&

		while read -r hash
		do
			for ext in pack idx rev
			do
				test -f .git/mvcc-cache/pack/"$hash.$ext" ||
				{ echo "missing $hash.$ext" && return 1; }
			done
		done <packs
	)
'

test_expect_success 'no tmp_* tempfiles remain after successful writes' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit one &&
		test_commit two &&
		git maintenance run --task=geometric-repack &&

		for sub in pack reftables manifests
		do
			find .git/mvcc-cache/$sub -name "tmp_*" >stragglers &&
			if test -s stragglers
			then
				echo "tempfile leftovers in $sub:" &&
				cat stragglers &&
				return 1
			fi
		done
	)
'

test_expect_success 'write under GIT_MVCC_MANIFEST pin is rejected' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit base &&
		MANIFEST=$(cat .git/mvcc-cache/manifest) &&

		echo payload >file &&
		test_must_fail env GIT_MVCC_MANIFEST="$MANIFEST" \
			git hash-object -w file 2>err &&
		test_grep "cannot write while GIT_MVCC_MANIFEST is set" err
	)
'

test_expect_success 'missing external manifest pointer file is rejected' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit base &&

		manifest_path=$PWD/manifest-path-missing &&

		echo payload >file &&
		test_must_fail env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git hash-object -w file 2>err &&
		test_grep "does not exist" err
	)
'

test_expect_success 'empty external manifest pointer file is rejected' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit base &&

		manifest_path=$PWD/manifest-path-empty &&
		: >"$manifest_path" &&

		echo payload >file &&
		test_must_fail env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git hash-object -w file 2>err &&
		test_grep "empty" err
	)
'

test_expect_success 'pin overrides external pointer for reads, writes still rejected' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit base &&
		BASE_MANIFEST=$(cat .git/mvcc-cache/manifest) &&

		# Build a fresh external pointer file that names the same
		# base manifest as the cache; then have a staged write
		# evolve it (without any pin).
		manifest_path=$PWD/manifest-path &&
		cp .git/mvcc-cache/manifest "$manifest_path" &&

		echo newer >payload &&
		newer_oid=$(env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git hash-object -w payload) &&
		STAGED_MANIFEST=$(cat "$manifest_path") &&
		test "$STAGED_MANIFEST" != "$BASE_MANIFEST" &&

		# With both env vars set and the pin pointing at the base
		# manifest, reads must see the pinned (older) view: the
		# new object exists only in the staged manifest, so the
		# pinned view cannot resolve it.
		test_must_fail env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			GIT_MVCC_MANIFEST="$BASE_MANIFEST" \
			git cat-file -p "$newer_oid" 2>err &&

		# Likewise, writes under the pin must be rejected even
		# though the external pointer is set.
		echo extra >another &&
		test_must_fail env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			GIT_MVCC_MANIFEST="$BASE_MANIFEST" \
			git hash-object -w another 2>err &&
		test_grep "cannot write while GIT_MVCC_MANIFEST is set" err
	)
'

test_done
