# Shell library to run an S3-compatible server using SeaweedFS in tests.

if ! weed version >/dev/null 2>&1
then
	skip_all='skipping s3 tests; weed not available'
	test_done
fi

test_set_port WEED_S3_PORT
WEED_DIR="$TRASH_DIRECTORY/weed-server"
WEED_S3_ENDPOINT="http://127.0.0.1:$WEED_S3_PORT"
WEED_BUCKET="git-test-bucket"

# Fixed credentials for the test instance.
WEED_ACCESS_KEY="git-test-key-id"
WEED_SECRET_KEY="git-test-secret-key"

registered_stop_weed_atexit_handler=

start_weed () {
	if test -n "$WEED_PID"
	then
		die "start_weed already called"
	fi

	if test -z "$registered_stop_weed_atexit_handler"
	then
		test_atexit 'stop_weed'
		registered_stop_weed_atexit_handler=AlreadyDone
	fi

	mkdir -p "$WEED_DIR/data" "$WEED_DIR/logs" &&

	cat >"$WEED_DIR/s3.json" <<-EOF
	{
	  "identities": [{
	    "name": "git-test",
	    "credentials": [{"accessKey": "$WEED_ACCESS_KEY", "secretKey": "$WEED_SECRET_KEY"}],
	    "actions": ["Admin", "Read", "Write", "List", "Tagging"]
	  }]
	}
	EOF

	say >&3 "Starting SeaweedFS (S3 on port $WEED_S3_PORT) ..."
	weed server \
		-ip.bind=0.0.0.0 \
		-dir="$WEED_DIR/data" \
		-s3 \
		-s3.port="$WEED_S3_PORT" \
		-s3.config="$WEED_DIR/s3.json" \
		-master.volumeSizeLimitMB=100 \
		>"$WEED_DIR/logs/weed.log" 2>&1 &
	WEED_PID=$!

	i=30
	ready=
	while test $i -gt 0
	do
		if curl --silent --max-time 1 "$WEED_S3_ENDPOINT/status" >/dev/null 2>&1
		then
			ready=true
			break
		fi
		kill -0 "$WEED_PID" 2>/dev/null || break
		sleep 1
		i=$((i - 1))
	done

	if test -z "$ready"
	then
		cat "$WEED_DIR/logs/weed.log" >&4
		kill "$WEED_PID" 2>/dev/null
		wait "$WEED_PID"
		WEED_PID=
		test_skip_or_die GIT_TEST_WEED "SeaweedFS failed to start"
	fi

	# Export credentials so the S3 ODB backend picks them up automatically.
	S3_KEY_ID="$WEED_ACCESS_KEY"
	S3_KEY_SECRET="$WEED_SECRET_KEY"
	export S3_KEY_ID S3_KEY_SECRET
}

stop_weed () {
	if test -z "$WEED_PID"
	then
		return
	fi
	say >&3 "Stopping SeaweedFS ..."
	kill "$WEED_PID"
	wait "$WEED_PID" >&3 2>&4
	WEED_PID=
}
