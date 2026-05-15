#!/bin/sh

test_description='object database and reference store both backed by the MVCC manifest store'

. ./test-lib.sh

MVCC_URL="mvcc://"

setup_repo () {
	test_when_finished "rm -rf $1" &&
	env GIT_REFERENCE_BACKEND="$MVCC_URL" git init "$@" &&
	git -C "$1" config set core.repositoryFormatVersion 1 &&
	git -C "$1" config set extensions.objectStorage "$MVCC_URL"
}

mvcc_manifest_reftables () {
	sed -n 's/^r: //p' "$1"
}

test_expect_success 'can open an empty repository' '
	setup_repo repo &&
	(
		cd repo &&
		git status &&
		ls .git/mvcc-cache/reftables/*.ref >tables &&
		test_line_count -ne 0 tables &&
		test -d .git/mvcc-cache/manifests
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
		# everything lives in the MVCC cache instead.
		find .git/objects/ -path .git/mvcc-cache -prune -o -type f -print >files &&
		test_must_be_empty files &&

		# At least one reftable file must have been written locally.
		ls .git/mvcc-cache/reftables/*.ref >tables &&
		test_line_count -ne 0 tables &&

		# The combined manifest must carry entries for both packs and
		# reftables, demonstrating that both backends share a manifest.
		grep -h "^p:" .git/mvcc-cache/manifests/* &&
		grep -h "^r:" .git/mvcc-cache/manifests/*
	)
'

test_expect_success 'works in a bare repository' '
	setup_repo repo.git --bare &&
	(
		cd repo.git &&

		ls mvcc-cache/reftables/*.ref >tables &&
		test_line_count -ne 0 tables &&

		EMPTY_TREE_OID=$(git hash-object -w --stdin -t tree </dev/null) &&
		COMMIT_OID=$(git commit-tree -m message "$EMPTY_TREE_OID") &&
		git update-ref refs/heads/branch "$COMMIT_OID" &&

		printf "%s commit\trefs/heads/branch\n" "$COMMIT_OID" >expect &&
		git refs list >actual &&
		test_cmp expect actual &&

		grep -h "^p:" mvcc-cache/manifests/* &&
		grep -h "^r:" mvcc-cache/manifests/*
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

test_expect_success 'MVCC pins both refs and objects' '
	setup_repo repo &&
	(
		cd repo &&

		test_commit a &&
		MANIFEST_A=$(cat .git/mvcc-cache/manifest) &&
		test_commit b &&
		MANIFEST_B=$(cat .git/mvcc-cache/manifest) &&
		test_commit c &&

		OID_A=$(git rev-parse a) &&
		OID_C=$(git rev-parse c) &&

		# Pinning to manifest A: HEAD must resolve to "a".
		echo "$OID_A" >expect &&
		env GIT_MVCC_MANIFEST="$MANIFEST_A" git rev-parse HEAD >actual &&
		test_cmp expect actual &&

		# Pinning to manifest B: log shows b then a, but not c.
		cat >expect <<-\EOF &&
		b
		a
		EOF
		env GIT_MVCC_MANIFEST="$MANIFEST_B" git log --format=%s >actual &&
		test_cmp expect actual &&

		# At manifest A, commit c is not yet visible as an object.
		test_must_fail env GIT_MVCC_MANIFEST="$MANIFEST_A" \
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
		find .git -maxdepth 2 -type d -name "mvcc-cache*" >caches &&
		test_line_count = 1 caches &&

		test -d .git/mvcc-cache/pack &&
		test -d .git/mvcc-cache/manifests &&

		# That single cache must reflect both backends in a
		# single manifest version.
		ls .git/mvcc-cache/manifests >manifests &&
		test_line_count -ne 0 manifests &&
		grep -lh "^p:" .git/mvcc-cache/manifests/* >with-packs &&
		grep -lh "^r:" .git/mvcc-cache/manifests/* >with-reftables &&
		comm -12 with-packs with-reftables >shared &&
		test_line_count -ne 0 shared
	)
'

test_expect_success 'differing URL payloads still share one storage handle' '
	test_when_finished "rm -rf repo" &&
	env GIT_REFERENCE_BACKEND="mvcc://refs-payload" git init repo &&
	git -C repo config set core.repositoryFormatVersion 1 &&
	git -C repo config set extensions.objectStorage "mvcc://objects-payload" &&
	(
		cd repo &&

		# Both backends are configured with different URL payloads,
		# but the storage handle is keyed by the common directory.
		# A single commit must therefore produce one manifest body
		# carrying both `p:` and `r:` entries (proving both backends
		# activated the same manifest), rather than two divergent
		# manifests.
		test_commit only &&

		ls .git/mvcc-cache/manifests >manifests &&
		test_line_count -ne 0 manifests &&

		MANIFEST=$(cat .git/mvcc-cache/manifest) &&
		grep "^p:" .git/mvcc-cache/manifests/"$MANIFEST" &&
		grep "^r:" .git/mvcc-cache/manifests/"$MANIFEST"
	)
'

test_expect_success 'combined staged write captures both refs and packs' '
	setup_repo repo &&
	(
		cd repo &&

		test_commit base &&
		BASE_MANIFEST=$(cat .git/mvcc-cache/manifest) &&

		cp .git/mvcc-cache/manifest cache-pointer-before &&

		manifest_path=$PWD/manifest-path &&
		cp .git/mvcc-cache/manifest "$manifest_path" &&

		# Perform a single commit with an external pointer. This
		# exercises both subsystems through the same MVCC handle.
		env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git -c user.email=t@example.com \
			    -c user.name=tester \
			    commit --allow-empty -m staged &&

		# Cache pointer untouched.
		test_cmp cache-pointer-before .git/mvcc-cache/manifest &&

		# A single new manifest body must cover both backends.
		STAGED_MANIFEST=$(cat "$manifest_path") &&
		test "$STAGED_MANIFEST" != "$BASE_MANIFEST" &&
		test -f ".git/mvcc-cache/manifests/$STAGED_MANIFEST" &&
		grep "^p:" ".git/mvcc-cache/manifests/$STAGED_MANIFEST" &&
		grep "^r:" ".git/mvcc-cache/manifests/$STAGED_MANIFEST"
	)
'

test_expect_success 'push from a dual-MVCC repository' '
	test_when_finished "rm -rf target.git" &&
	git init --bare target.git &&

	setup_repo repo &&
	(
		cd repo &&
		test_commit first &&
		test_commit second &&
		test_commit third &&

		# Bitmaps trigger a known downcast issue in the MVCC ODB
		# source, so disable them for the push (matching t0700).
		git -c pack.useBitmaps=false push ../target.git HEAD:branch &&

		git log >expect &&
		git -C ../target.git log branch >actual &&
		test_cmp expect actual
	)
'

test_expect_success 'incremental pushes append refs and packs to the manifest' '
	setup_repo target.git --bare &&
	test_when_finished "rm -rf source" &&
	git init source &&
	(
		cd source &&

		test_commit first &&
		git push --mirror ../target.git &&
		git refs list >expect-first &&
		MANIFEST_FIRST=$(cat ../target.git/mvcc-cache/manifest) &&

		test_commit second &&
		git push --mirror ../target.git &&
		git refs list >expect-second &&
		MANIFEST_SECOND=$(cat ../target.git/mvcc-cache/manifest) &&

		git refs list >expect &&
		git -C ../target.git refs list >actual &&
		test_cmp expect actual &&

		env GIT_MVCC_MANIFEST="$MANIFEST_FIRST" git -C ../target.git refs list >actual &&
		test_cmp expect-first actual &&
		env GIT_MVCC_MANIFEST="$MANIFEST_SECOND" git -C ../target.git refs list >actual &&
		test_cmp expect-second actual
	)
'

test_expect_success 'sequential staged writes within one process accumulate' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit base &&
		BASE_MANIFEST=$(cat .git/mvcc-cache/manifest) &&
		BASE_HEAD=$(git rev-parse HEAD) &&

		cat .git/mvcc-cache/manifests/mvcc-cache/manifests/"$BASE_MANIFEST" |
			sort >base-packs &&

		manifest_path=$PWD/manifest-path &&
		cp .git/mvcc-cache/manifest "$manifest_path" &&

		# A single `git commit` exercises both the ODB write path
		# (new pack) and the refs write path (new reftable), each
		# of which calls mvcc_storage_activate_manifest. The
		# second write must see the first one`s state via the
		# in-process manifest cache.
		env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git -c user.email=t@example.com \
			    -c user.name=tester \
			    commit --allow-empty -m staged &&

		STAGED_MANIFEST=$(cat "$manifest_path") &&
		test "$STAGED_MANIFEST" != "$BASE_MANIFEST" &&

		# Packs are never compacted as a side effect of `git commit`,
		# so every base pack must still be referenced and the staged
		# manifest must list at least one new pack.
		cat .git/mvcc-cache/manifests/"$STAGED_MANIFEST" |
			sort >staged-packs &&
		comm -23 base-packs staged-packs >dropped-packs &&
		test_must_be_empty dropped-packs &&
		test "$(wc -l <staged-packs)" -gt "$(wc -l <base-packs)" &&

		# The reftable subsystem must have produced output: the
		# staged manifest must reference at least one reftable.
		mvcc_manifest_reftables .git/mvcc-cache/manifests/"$STAGED_MANIFEST" |
			sort >staged-reftables &&
		test_line_count -gt 0 staged-reftables &&

		# The new commit must be observable inside the staged
		# session, proving both subsystems chained correctly.
		env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git rev-parse HEAD >new-head &&
		test "$(cat new-head)" != "$BASE_HEAD"
	)
'

test_expect_success 'GIT_MVCC_MANIFEST overrides a newer external pointer' '
	setup_repo repo &&
	(
		cd repo &&

		test_commit base &&
		BASE_MANIFEST=$(cat .git/mvcc-cache/manifest) &&
		BASE_OID=$(git rev-parse HEAD) &&

		# Make a staged write that produces a new (newer) manifest
		# referenced via the external pointer file.
		manifest_path=$PWD/manifest-path &&
		cp .git/mvcc-cache/manifest "$manifest_path" &&
		env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git -c user.email=t@example.com \
			    -c user.name=tester \
			    commit --allow-empty -m newer &&
		NEW_MANIFEST=$(cat "$manifest_path") &&
		test "$NEW_MANIFEST" != "$BASE_MANIFEST" &&

		# Without pinning, reads under the external pointer see
		# the new commit.
		env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git rev-parse HEAD >actual-newer &&
		test "$(cat actual-newer)" != "$BASE_OID" &&

		# Pinning to the base manifest must override the external
		# pointer and yield the base view.
		env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
		    GIT_MVCC_MANIFEST="$BASE_MANIFEST" \
			git rev-parse HEAD >actual-pinned &&
		echo "$BASE_OID" >expect-pinned &&
		test_cmp expect-pinned actual-pinned
	)
'

test_expect_success 'CAS catches stale base manifest during a commit in cache mode' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit base &&

		# A reference-transaction hook that clobbers the cache
		# pointer in the prepared phase, simulating a concurrent
		# writer that advanced the pointer between manifest
		# resolution and activation.
		write_script .git/hooks/reference-transaction <<-\EOF &&
		if test "$1" = "prepared"
		then
			printf "%064d\n" 0 >.git/mvcc-cache/manifest
		fi
		EOF

		test_must_fail git -c user.email=t@example.com \
				   -c user.name=tester \
				   commit --allow-empty -m racing 2>err &&
		test_grep "stale base manifest" err &&
		test_grep "another writer advanced the pointer" err
	)
'

test_expect_success 'CAS catches stale base manifest during a commit with external pointer' '
	setup_repo repo &&
	(
		cd repo &&
		test_commit base &&

		manifest_path=$PWD/manifest-path &&
		cp .git/mvcc-cache/manifest "$manifest_path" &&

		write_script .git/hooks/reference-transaction <<-EOF &&
		if test "\$1" = "prepared"
		then
			printf "%064d\n" 0 >"$manifest_path"
		fi
		EOF

		test_must_fail env GIT_MVCC_MANIFEST_PATH="$manifest_path" \
			git -c user.email=t@example.com \
			    -c user.name=tester \
			    commit --allow-empty -m racing 2>err &&
		test_grep "stale base manifest" err &&
		test_grep "another writer advanced the pointer" err
	)
'

test_done
