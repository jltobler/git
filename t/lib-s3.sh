# Shell library to run an S3-compatible server using SeaweedFS in tests.

if ! weed version >/dev/null 2>&1
then
	skip_all='skipping s3 tests; weed not available'
	test_done
fi

if ! test_have_prereq CURL; then
	skip_all='skipping s3 tests; curl not available'
	test_done
fi

port=${this_test#${this_test%%[1-9]*}}
S3_PORT=$(($port + 1000))
S3_MASTER_PORT=$(($port + 1010))
S3_VOLUME_PORT=$(($port + 1020))
S3_FILER_PORT=$(($port + 1030))

S3_DIR="$TRASH_DIRECTORY/weed-server"
S3_ENDPOINT="http://127.0.0.1:$S3_PORT"
S3_BUCKET="git-test-bucket"

# Fixed credentials for the test instance.
S3_ACCESS_KEY="git-test-key-id"
S3_SECRET_KEY="git-test-secret-key"

registered_stop_s3_atexit_handler=

start_s3_server () {
	if test -n "$S3_SERVER_PID"
	then
		die "start_s3_server already called"
	fi

	if test -z "$registered_stop_s3_atexit_handler"
	then
		test_atexit stop_s3_server
		registered_stop_s3_atexit_handler=AlreadyDone
	fi

	mkdir -p "$S3_DIR/data" "$S3_DIR/logs" &&

	cat >"$S3_DIR/s3.json" <<-EOF
	{
	  "identities": [{
	    "name": "git-test",
	    "credentials": [{"accessKey": "$S3_ACCESS_KEY", "secretKey": "$S3_SECRET_KEY"}],
	    "actions": ["Admin", "Read", "Write", "List", "Tagging"]
	  }]
	}
	EOF

	say >&3 "Starting SeaweedFS (S3 on port $S3_PORT) ..."
	weed server \
		-ip=127.0.0.1 \
		-dir="$S3_DIR/data" \
		-s3 \
		-s3.iam=false \
		-s3.config="$S3_DIR/s3.json" \
		-s3.port="$S3_PORT" \
		-s3.port.grpc=0 \
		-s3.port.iceberg=0 \
		-filer.port="$S3_FILER_PORT" \
		-filer.port.grpc=0 \
		-master.port="$S3_MASTER_PORT" \
		-master.volumeSizeLimitMB=5 \
		-master.electionTimeout=1ms \
		-master.defaultReplication=000 \
		-volume.port="$S3_VOLUME_PORT" \
		-volume.preStopSeconds=0 \
		>"$S3_DIR/logs/weed.log" 2>&1 &
	S3_SERVER_PID=$!

	i=30
	ready=
	while test $i -gt 0
	do
		if curl --silent --max-time 1 "$S3_ENDPOINT/status" >/dev/null 2>&1
		then
			ready=true
			break
		fi
		kill -0 "$S3_SERVER_PID" 2>/dev/null || break
		sleep 1
		i=$((i - 1))
	done

	if test -z "$ready"
	then
		cat "$S3_DIR/logs/weed.log" >&4
		kill "$S3_SERVER_PID" 2>/dev/null
		wait "$S3_SERVER_PID"
		S3_SERVER_PID=
		error "SeaweedFS failed to start"
		return 1
	fi

	# Export credentials so the S3 ODB backend picks them up automatically.
	S3_KEY_ID="$S3_ACCESS_KEY"
	S3_KEY_SECRET="$S3_SECRET_KEY"
	export S3_KEY_ID S3_KEY_SECRET
}

stop_s3_server () {
	if test -z "$S3_SERVER_PID"
	then
		return
	fi
	say >&3 "Stopping SeaweedFS ..."
	kill -9 "$S3_SERVER_PID"
	wait "$S3_SERVER_PID" >&3 2>&4
	S3_SERVER_PID=
}

# Fetch the latest manifest version for a given repository directly from S3 via
# curl, bypassing any local cache. This is useful in tests that need to verify
# what is actually persisted in object storage independent of local filesystem
# state.
s3_fetch_manifest_version () {
	curl --silent --show-error --fail \
		--aws-sigv4 "aws:amz:us-east-1:s3" \
		--user "$S3_KEY_ID:$S3_KEY_SECRET" \
		"${1#s3://}/manifest"
}

# Overwrite the manifest pointer at <prefix>/manifest directly via curl,
# bypassing the in-process s3 storage cache. The body is read from stdin and
# becomes the new manifest version hash. Useful for tests that need to simulate
# an out-of-band concurrent writer advancing the pointer.
s3_put_manifest_version () {
	curl --silent --show-error --fail \
		--aws-sigv4 "aws:amz:us-east-1:s3" \
		--user "$S3_KEY_ID:$S3_KEY_SECRET" \
		--upload-file - \
		"${1#s3://}/manifest"
}
