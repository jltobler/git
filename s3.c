#include "git-compat-util.h"
#include "gettext.h"
#include "hash.h"
#include "hex.h"
#include "repository.h"
#include "strbuf.h"
#include "string-list.h"
#include "s3.h"
#include "tempfile.h"
#include "thread-utils.h"
#include "trace2.h"
#include "wrapper.h"

#include <curl/curl.h>
#include <curl/easy.h>

static void s3_etag_release(struct s3_etag *etag)
{
	FREE_AND_NULL(etag->tag);
}

static void s3_etag_copy(const struct s3_etag *from, struct s3_etag *to)
{
	s3_etag_release(to);
	to->tag = xstrdup_or_null(from->tag);
}

static void sha256_to_hex(const unsigned char *digest, char *out)
{
	hash_to_hex_algop_r(out, digest, &hash_algos[GIT_HASH_SHA256]);
}

static void sha256_buf_hex(const void *buf, size_t len, char out[GIT_SHA256_HEXSZ + 1])
{
	git_SHA256_CTX ctx;
	unsigned char digest[GIT_SHA256_RAWSZ];
	git_SHA256_Init(&ctx);
	git_SHA256_Update(&ctx, buf, len);
	git_SHA256_Final(digest, &ctx);
	sha256_to_hex(digest, out);
}

int s3_sha256_file_hex(const char *path, char out[GIT_SHA256_HEXSZ + 1])
{
	unsigned char digest[GIT_SHA256_RAWSZ];
	git_SHA256_CTX ctx;
	char buf[65536];
	ssize_t bytes_read;
	int fd;

	fd = open(path, O_RDONLY);
	if (fd < 0)
		return error_errno("s3: cannot open '%s' for hashing", path);

	git_SHA256_Init(&ctx);
	while ((bytes_read = xread(fd, buf, sizeof(buf))) > 0)
		git_SHA256_Update(&ctx, buf, bytes_read);
	close(fd);

	if (bytes_read < 0)
		return error_errno("s3: read error hashing '%s'", path);

	git_SHA256_Final(digest, &ctx);
	sha256_to_hex(digest, out);
	return 0;
}

static void hmac_sha256(const void *key, size_t key_len,
			const void *data, size_t data_len,
			unsigned char out[GIT_SHA256_RAWSZ])
{
	git_SHA256_CTX ctx;
	unsigned char k_pad[GIT_SHA256_BLKSZ];
	unsigned char inner[GIT_SHA256_RAWSZ];
	int i;

	/* Keys longer than the block size are hashed to GIT_SHA256_RAWSZ. */
	if (key_len > GIT_SHA256_BLKSZ) {
		git_SHA256_Init(&ctx);
		git_SHA256_Update(&ctx, key, key_len);
		git_SHA256_Final(k_pad, &ctx);
		key_len = GIT_SHA256_RAWSZ;
	} else {
		memcpy(k_pad, key, key_len);
	}
	memset(k_pad + key_len, 0, GIT_SHA256_BLKSZ - key_len);

	/* Inner: SHA256((key XOR ipad) || data) */
	for (i = 0; i < GIT_SHA256_BLKSZ; i++)
		k_pad[i] ^= 0x36;
	git_SHA256_Init(&ctx);
	git_SHA256_Update(&ctx, k_pad, GIT_SHA256_BLKSZ);
	git_SHA256_Update(&ctx, data, data_len);
	git_SHA256_Final(inner, &ctx);

	/* Outer: SHA256((key XOR opad) || inner) */
	for (i = 0; i < GIT_SHA256_BLKSZ; i++)
		k_pad[i] ^= (0x36 ^ 0x5c);
	git_SHA256_Init(&ctx);
	git_SHA256_Update(&ctx, k_pad, GIT_SHA256_BLKSZ);
	git_SHA256_Update(&ctx, inner, GIT_SHA256_RAWSZ);
	git_SHA256_Final(out, &ctx);
}

/*
 * Derive the four-stage SigV4 signing key:
 *   kDate    = HMAC-SHA256("AWS4" + secret_key, date_str)
 *   kRegion  = HMAC-SHA256(kDate, region)
 *   kService = HMAC-SHA256(kRegion, "s3")
 *   kSigning = HMAC-SHA256(kService, "aws4_request")
 */
static void sigv4_derive_key(const char *secret_key,
			     const char *date_str, /* "YYYYMMDD" */
			     const char *region,
			     unsigned char key_out[GIT_SHA256_RAWSZ])
{
	char seed[128];
	unsigned char k[GIT_SHA256_RAWSZ];

	xsnprintf(seed, sizeof(seed), "AWS4%s", secret_key);
	hmac_sha256(seed, strlen(seed), date_str, strlen(date_str), k);
	hmac_sha256(k, GIT_SHA256_RAWSZ, region, strlen(region), k);
	hmac_sha256(k, GIT_SHA256_RAWSZ, "s3", 2, k);
	hmac_sha256(k, GIT_SHA256_RAWSZ, "aws4_request", 12, key_out);
}

/*
 * AWS Signature Version 4
 *
 * Reference: https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html
 *
 * Build a curl_slist of headers and (via *auth_header_out) the Authorization
 * header string for a single S3 request.
 *
 * method         - "GET" or "PUT"
 * key            - bucket-relative S3 key, used to derive the canonical
 *                  URI path (`/<bucket>/<key>`) for both signing and
 *                  the Host: header.
 * payload_sha256 - lowercase hex SHA-256 of the request body
 * if_match       - optional CAS precondition for PUTs. When set with a
 *                  non-NULL `tag` the request is signed with
 *                  `If-Match: <tag>`. When set with a NULL `tag` the
 *                  request is signed with `If-None-Match: *` (used for
 *                  first-write PUTs).
 * content_length - body size in bytes; ignored for GET (pass 0)
 *
 * Returns a newly allocated curl_slist that the caller must free with
 * curl_slist_free_all().  Also allocates *auth_header_out which the caller
 * must free(); it is already appended to the returned slist.
 */
static struct curl_slist *sigv4_build_headers(struct s3_storage *storage,
					      const char *method,
					      const char *key,
					      const char *payload_sha256,
					      const struct s3_etag *if_match,
					      curl_off_t content_length)
{
	struct strbuf canonical_hdrs = STRBUF_INIT;
	struct strbuf string_to_sign = STRBUF_INIT;
	struct strbuf canonical_req = STRBUF_INIT;
	struct strbuf signed_hdrs = STRBUF_INIT;
	char canonical_req_hash[GIT_SHA256_HEXSZ + 1];
	int is_put = !strcmp(method, "PUT");
	unsigned char signing_key[GIT_SHA256_RAWSZ];
	unsigned char sig_raw[GIT_SHA256_RAWSZ];
	struct strbuf auth = STRBUF_INIT;
	struct strbuf hdr = STRBUF_INIT;
	struct curl_slist *hdrs = NULL;
	char *path;
	char sig_hex[GIT_SHA256_HEXSZ + 1];
	char date_time[17];
	char date_only[9];
	struct tm gmt;
	time_t now;

	path = xstrfmt("/%s/%s", storage->bucket, key);

	now = time(NULL);
	gmtime_r(&now, &gmt);
	strftime(date_time, sizeof(date_time), "%Y%m%dT%H%M%SZ", &gmt);
	strftime(date_only, sizeof(date_only), "%Y%m%d", &gmt);

	/*
	 * Step 1: assemble the canonical request.
	 *
	 * CanonicalRequest =
	 *   Method '\n'
	 *   CanonicalURI '\n'
	 *   CanonicalQueryString '\n'
	 *   CanonicalHeaders '\n'
	 *   SignedHeaders '\n'
	 *   HexEncode(Hash(RequestPayload))
	 */
	if (is_put) {
		strbuf_addf(&canonical_hdrs,
			    "content-length:%" PRIdMAX "\n",
			    (intmax_t)content_length);
		strbuf_addstr(&signed_hdrs, "content-length;");
	}

	strbuf_addf(&canonical_hdrs, "host:%s\n", storage->host);
	strbuf_addstr(&signed_hdrs, "host;");

	if (if_match) {
		if (if_match->tag) {
			strbuf_addf(&canonical_hdrs, "if-match:%s\n", if_match->tag);
			strbuf_addstr(&signed_hdrs, "if-match;");
		} else {
			strbuf_addstr(&canonical_hdrs, "if-none-match:*\n");
			strbuf_addstr(&signed_hdrs, "if-none-match;");
		}
	}

	strbuf_addf(&canonical_hdrs, "x-amz-content-sha256:%s\n",
		    payload_sha256);
	strbuf_addf(&canonical_hdrs, "x-amz-date:%s\n", date_time);
	strbuf_addstr(&signed_hdrs, "x-amz-content-sha256;x-amz-date");

	strbuf_addf(&canonical_req,
		    "%s\n"
		    "%s\n"
		    "\n"
		    "%s\n"
		    "%s\n"
		    "%s",
		    method,
		    path,
		    canonical_hdrs.buf,
		    signed_hdrs.buf,
		    payload_sha256);

	/*
	 * Step 2: sign the request.
	 *
	 * StringToSign =
	 *   "AWS4-HMAC-SHA256" '\n'
	 *   Timestamp '\n'
	 *   CredentialScope '\n'
	 *   HexEncode(Hash(CanonicalRequest))
	 */
	sha256_buf_hex(canonical_req.buf, canonical_req.len,
		       canonical_req_hash);
	strbuf_addf(&string_to_sign,
		    "AWS4-HMAC-SHA256\n"
		    "%s\n"
		    "%s/%s/s3/aws4_request\n"
		    "%s",
		    date_time,
		    date_only, storage->region,
		    canonical_req_hash);
	sigv4_derive_key(storage->secret_access_key, date_only,
			 storage->region, signing_key);
	hmac_sha256(signing_key, GIT_SHA256_RAWSZ,
		    string_to_sign.buf, string_to_sign.len,
		    sig_raw);
	sha256_to_hex(sig_raw, sig_hex);

	/* Step 3: authorization header that includes the signed request. */
	strbuf_addf(&auth,
		    "Authorization: AWS4-HMAC-SHA256 "
		    "Credential=%s/%s/%s/s3/aws4_request, "
		    "SignedHeaders=%s, "
		    "Signature=%s",
		    storage->access_key_id, date_only, storage->region,
		    signed_hdrs.buf, sig_hex);
	strbuf_release(&signed_hdrs);

	/* Assemble the curl header list. */
	strbuf_addf(&hdr, "Host: %s", storage->host);
	hdrs = curl_slist_append(hdrs, hdr.buf);

	strbuf_reset(&hdr);
	strbuf_addf(&hdr, "x-amz-date: %s", date_time);
	hdrs = curl_slist_append(hdrs, hdr.buf);

	strbuf_reset(&hdr);
	strbuf_addf(&hdr, "x-amz-content-sha256: %s", payload_sha256);
	hdrs = curl_slist_append(hdrs, hdr.buf);

	if (is_put) {
		strbuf_reset(&hdr);
		strbuf_addf(&hdr, "Content-Length: %" PRIdMAX, (intmax_t)content_length);
		hdrs = curl_slist_append(hdrs, hdr.buf);
	}

	if (if_match) {
		strbuf_reset(&hdr);
		if (if_match->tag)
			strbuf_addf(&hdr, "If-Match: %s", if_match->tag);
		else
			strbuf_addstr(&hdr, "If-None-Match: *");
		hdrs = curl_slist_append(hdrs, hdr.buf);
	}

	hdrs = curl_slist_append(hdrs, auth.buf);

	strbuf_release(&string_to_sign);
	strbuf_release(&canonical_req);
	strbuf_release(&canonical_hdrs);
	strbuf_release(&auth);
	strbuf_release(&hdr);
	free(path);
	return hdrs;
}

/*
 * Build an S3 key by joining the source's prefix with a suffix. Returns a
 * heap-allocated string the caller must free().
 */
static char *s3_key(const struct s3_storage *storage, const char *suffix)
{
	return xstrfmt("%s/%s", storage->prefix, suffix);
}

static char *s3_url(const struct s3_storage *storage, const char *key)
{
	return xstrfmt("%s/%s/%s", storage->endpoint_url, storage->bucket, key);
}

/*
 * Lock callbacks for the shared CURLSH handle. curl invokes these around its
 * internal accesses to the shared connection / DNS / TLS-session caches.
 *
 * The data and access parameters are unused: we use a single coarse mutex per
 * storage which is sufficient because the share is only briefly contended
 * during cache lookups.
 */
static void s3_share_lock(CURL *curl UNUSED, curl_lock_data data UNUSED,
			  curl_lock_access access UNUSED, void *userdata)
{
	pthread_mutex_t *mutex = userdata;
	pthread_mutex_lock(mutex);
}

static void s3_share_unlock(CURL *curl UNUSED, curl_lock_data data UNUSED,
			    void *userdata)
{
	pthread_mutex_t *mutex = userdata;
	pthread_mutex_unlock(mutex);
}

/*
 * Lazily create and return the CURLSH for this storage. The share caches
 * connections, DNS results and TLS sessions across all curl easy handles
 * created against this storage so that small follow-up requests (like the
 * manifest pointer fetch) skip the TCP + TLS handshake entirely.
 *
 * The mutex protecting the share is created the first time we get here
 * (rather than in s3_storage_get) so we can use a recursive mutex without
 * having to teach the storage init path about pthread attributes. libcurl
 * may call our LOCKFUNC re-entrantly with different CURL_LOCK_DATA values
 * while it already holds the lock for another -- a non-recursive mutex
 * deadlocks the (single-threaded) process in that case.
 */
static CURLSH *s3_get_share(struct s3_storage *storage)
{
	if (!storage->curl_share) {
		CURLSH *share = curl_share_init();
		if (!share)
			die("s3: curl_share_init failed");

		curl_share_setopt(share, CURLSHOPT_LOCKFUNC, s3_share_lock);
		curl_share_setopt(share, CURLSHOPT_UNLOCKFUNC, s3_share_unlock);
		curl_share_setopt(share, CURLSHOPT_USERDATA,
				  &storage->curl_share_mutex);
		curl_share_setopt(share, CURLSHOPT_SHARE,
				  CURL_LOCK_DATA_CONNECT);
		curl_share_setopt(share, CURLSHOPT_SHARE, CURL_LOCK_DATA_DNS);
		curl_share_setopt(share, CURLSHOPT_SHARE,
				  CURL_LOCK_DATA_SSL_SESSION);

		storage->curl_share = share;
	}
	return storage->curl_share;
}

/*
 * Apply settings common to every S3 request: attach the shared connection
 * cache, enable TCP keepalive so connections stay healthy across longer
 * idle periods, and request HTTP/3 with graceful fallback so we get the
 * best protocol the network will allow.
 *
 * CURL_HTTP_VERSION_3 in current libcurl negotiates downwards through
 * HTTP/2 to HTTP/1.1 if QUIC is unreachable, so it is safe to set
 * unconditionally for HTTPS endpoints. Servers that support HTTP/3
 * (e.g. GCS) save us roughly one round trip per cold connection by
 * merging the TCP and TLS handshakes into a single QUIC handshake.
 *
 * libcurl's Alt-Svc cache is also enabled and persisted at
 * <cache>/altsvc.txt so that protocol-version selection across processes
 * benefits from what previous invocations have already learned about the
 * endpoint.
 */
static void s3_curl_setup_common(struct s3_storage *storage, CURL *curl)
{
	char *altsvc_path;

	curl_easy_setopt(curl, CURLOPT_SHARE, s3_get_share(storage));
	curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
	curl_easy_setopt(curl, CURLOPT_HTTP_VERSION,
			 (long)CURL_HTTP_VERSION_3);

	altsvc_path = xstrfmt("%s/altsvc.txt", storage->cache_dir);
	curl_easy_setopt(curl, CURLOPT_ALTSVC_CTRL,
			 (long)(CURLALTSVC_H1 | CURLALTSVC_H2 |
				CURLALTSVC_H3));
	curl_easy_setopt(curl, CURLOPT_ALTSVC, altsvc_path);
	free(altsvc_path);

	if (getenv("GIT_S3_CURL_VERBOSE"))
		curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
}

/*
 * Execute the curl request and return 0 on HTTP 2xx, -(http_code) on any
 * HTTP error response, or -1 on a transport-level curl failure.
 */
static int s3_perform(CURL *curl, const char *desc)
{
	CURLcode cc;
	long http_code = 0;

	cc = curl_easy_perform(curl);
	if (cc != CURLE_OK)
		return error("s3: curl error for %s: %s", desc, curl_easy_strerror(cc));

	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
	if (http_code < 200 || http_code >= 300)
		return -(int)http_code;

	return 0;
}

static size_t s3_write_cb(char *ptr, size_t size, size_t nmemb,
			  void *userdata)
{
	struct strbuf *buf = userdata;
	size_t n = size * nmemb;
	strbuf_add(buf, ptr, n);
	return n;
}

/*
 * Header callback used to extract the ETag from a response. The ETag
 * uniquely identifies the version of an object on the server. We pass
 * it back as If-Match for compare-and-swap writes of the manifest
 * pointer.
 */
static size_t s3_etag_header_cb(char *buffer, size_t size, size_t nitems,
				void *userdata)
{
	struct s3_etag *etag = userdata;
	size_t n = size * nitems;
	const char *prefix = "ETag:";
	size_t prefix_len = strlen(prefix);
	const char *p, *end;

	if (n < prefix_len || strncasecmp(buffer, prefix, prefix_len))
		return n;

	p = buffer + prefix_len;
	end = buffer + n;
	while (p < end && (*p == ' ' || *p == '\t'))
		p++;

	/* Strip trailing CRLF / whitespace. */
	while (end > p && (end[-1] == '\r' || end[-1] == '\n' ||
			   end[-1] == ' ' || end[-1] == '\t'))
		end--;

	s3_etag_release(etag);
	etag->tag = xstrndup(p, end - p);
	return n;
}

/* SHA-256 of the empty string (used as the payload hash for GET requests). */
#define SHA256_EMPTY "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

static int s3_download_buffer(struct s3_storage *storage, const char *key,
			      struct strbuf *out, struct s3_etag *etag_out)
{
	struct curl_slist *hdrs;
	CURL *curl;
	char *url;
	int ret;

	trace2_region_enter_printf("s3", "download-buffer", storage->repo,
				   "key:%s", key);

	url = s3_url(storage, key);

	hdrs = sigv4_build_headers(storage, "GET", key, SHA256_EMPTY,
				   NULL, 0);

	curl = curl_easy_init();
	s3_curl_setup_common(storage, curl);
	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, s3_write_cb);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, out);
	/* We want to check the status code ourselves. */
	curl_easy_setopt(curl, CURLOPT_FAILONERROR, 0L);
	if (etag_out) {
		curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, s3_etag_header_cb);
		curl_easy_setopt(curl, CURLOPT_HEADERDATA, etag_out);
	}

	ret = s3_perform(curl, key);

	trace2_region_leave("s3", "download-buffer", storage->repo);
	curl_easy_cleanup(curl);
	curl_slist_free_all(hdrs);
	free(url);
	return ret;
}

void s3_transfers_append(struct s3_transfers *transfers,
			 const char *local_path)
{
	ALLOC_GROW(transfers->keys, transfers->items_nr + 1,
		   transfers->items_alloc);
	/*
	 * keys and local_paths grow in lockstep so a single items_alloc
	 * covers both. ALLOC_GROW above may have reallocated keys; mirror
	 * the new capacity into local_paths via REALLOC_ARRAY which sizes
	 * to items_alloc unconditionally.
	 */
	REALLOC_ARRAY(transfers->local_paths, transfers->items_alloc);

	transfers->keys[transfers->items_nr] =
		s3_key(transfers->storage,
		       local_path + strlen(transfers->storage->cache_dir) + 1);
	transfers->local_paths[transfers->items_nr] = xstrdup(local_path);
	transfers->items_nr++;
}

void s3_transfers_release(struct s3_transfers *transfers)
{
	for (size_t i = 0; i < transfers->items_nr; i++) {
		free(transfers->keys[i]);
		free(transfers->local_paths[i]);
	}
	free(transfers->keys);
	free(transfers->local_paths);
	memset(transfers, 0, sizeof(*transfers));
}

/*
 * Per-file state for a download in flight inside s3_storage_download_files().
 *
 * The struct is heap-allocated once per transfer; pointers stored on
 * the curl easy handle (CURLOPT_PRIVATE) reference these stable
 * addresses, which is why we don't embed the struct directly in a
 * growable array.
 */
struct s3_download_request {
	CURL *curl;
	struct tempfile *tempfile;
	const char *key;        /* aliased into the caller's transfer */
	const char *local_path; /* aliased into the caller's transfer */
	struct curl_slist *hdrs;
	int started;
};

static void s3_download_request_free(struct s3_download_request *req)
{
	if (!req)
		return;
	if (req->curl)
		curl_easy_cleanup(req->curl);
	if (req->hdrs)
		curl_slist_free_all(req->hdrs);
	delete_tempfile(&req->tempfile);
	free(req);
}

/*
 * Prepare a single download request: allocate the per-request state,
 * open a tempfile next to the destination, and configure a curl easy
 * handle for the GET. Always returns a non-NULL request; on failure the
 * returned request has its curl handle left NULL (and an error already
 * reported) so the caller can detect failure and skip it when feeding
 * the curl_multi.
 */
static struct s3_download_request *s3_download_prepare(struct s3_storage *storage,
						       const char *key,
						       const char *local_path)
{
	struct s3_download_request *req;
	struct strbuf template = STRBUF_INIT;
	char *url = NULL;
	FILE *fp;

	CALLOC_ARRAY(req, 1);
	req->key = key;
	req->local_path = local_path;

	strbuf_addf(&template, "%s.XXXXXX", local_path);
	req->tempfile = xmks_tempfile(template.buf);
	strbuf_release(&template);

	fp = fdopen_tempfile(req->tempfile, "w");
	if (!fp) {
		error_errno("s3: cannot open '%s' for writing", local_path);
		goto out;
	}

	url = s3_url(storage, key);

	req->hdrs = sigv4_build_headers(storage, "GET", key, SHA256_EMPTY,
					NULL, 0);

	req->curl = curl_easy_init();
	if (!req->curl)
		die("s3: curl_easy_init failed");
	s3_curl_setup_common(storage, req->curl);
	curl_easy_setopt(req->curl, CURLOPT_URL, url);
	curl_easy_setopt(req->curl, CURLOPT_HTTPHEADER, req->hdrs);
	curl_easy_setopt(req->curl, CURLOPT_WRITEDATA, fp);
	curl_easy_setopt(req->curl, CURLOPT_PRIVATE, req);
	/*
	 * Tell libcurl to wait for an existing connection to finish
	 * negotiating ALPN before opening a new one. Combined with the
	 * HTTP/3 / HTTP/2 negotiation in s3_curl_setup_common this is
	 * what actually enables multiplexing across the easy handles in
	 * this call: the first transfer establishes the connection, the
	 * rest pile onto it as additional streams. Has no effect against
	 * HTTP/1.1 endpoints.
	 */
	curl_easy_setopt(req->curl, CURLOPT_PIPEWAIT, 1L);

out:
	free(url);
	return req;
}

int s3_storage_download_files(struct s3_storage *storage,
			      const struct s3_transfers *transfers)
{
	struct s3_download_request **requests = NULL;
	CURLM *curlm = NULL;
	size_t nr = transfers->items_nr;
	int still_running = 0;
	int ret = 0;
	size_t i;

	if (!nr)
		return 0;

	trace2_region_enter_printf("s3", "download", storage->repo,
				   "n:%"PRIuMAX, (uintmax_t)nr);

	CALLOC_ARRAY(requests, nr);
	for (i = 0; i < nr; i++) {
		requests[i] = s3_download_prepare(storage,
						  transfers->keys[i],
						  transfers->local_paths[i]);
		if (!requests[i]->curl)
			ret = -1;
	}

	curlm = curl_multi_init();
	if (!curlm)
		die("s3: curl_multi_init failed");
	curl_multi_setopt(curlm, CURLMOPT_MAX_HOST_CONNECTIONS, 8L);

	for (i = 0; i < nr; i++) {
		struct s3_download_request *req = requests[i];
		CURLMcode mc;

		if (!req->curl)
			continue;

		mc = curl_multi_add_handle(curlm, req->curl);
		if (mc != CURLM_OK) {
			error("s3: curl_multi_add_handle failed for '%s': %s",
			      req->key, curl_multi_strerror(mc));
			ret = -1;
			continue;
		}
		req->started = 1;
	}

	do {
		CURLMcode mc;
		CURLMsg *msg;
		int msgs_left = 0;

		mc = curl_multi_perform(curlm, &still_running);
		if (mc != CURLM_OK) {
			ret = error("s3: curl_multi_perform: %s",
				    curl_multi_strerror(mc));
			break;
		}

		while ((msg = curl_multi_info_read(curlm, &msgs_left))) {
			struct s3_download_request *req = NULL;
			long http_code = 0;

			if (msg->msg != CURLMSG_DONE)
				continue;

			curl_easy_getinfo(msg->easy_handle, CURLINFO_PRIVATE,
					  (char **)&req);
			if (!req)
				continue;

			if (msg->data.result != CURLE_OK) {
				error("s3: curl error downloading '%s': %s",
				      req->key,
				      curl_easy_strerror(msg->data.result));
				ret = -1;
				continue;
			}

			curl_easy_getinfo(msg->easy_handle,
					  CURLINFO_RESPONSE_CODE, &http_code);
			if (http_code < 200 || http_code >= 300) {
				error("s3: HTTP %ld downloading '%s'",
				      http_code, req->key);
				ret = -1;
				continue;
			}

			/*
			 * Commit the tempfile to its final destination only
			 * after a clean response. The fd is owned by the
			 * tempfile machinery and is closed by rename_tempfile.
			 */
			if (rename_tempfile(&req->tempfile,
					    (char *)req->local_path) < 0) {
				error_errno("s3: failed committing tempfile '%s'",
					    req->local_path);
				ret = -1;
			}
		}

		if (still_running) {
			mc = curl_multi_poll(curlm, NULL, 0, 1000, NULL);
			if (mc != CURLM_OK) {
				ret = error("s3: curl_multi_poll: %s",
					    curl_multi_strerror(mc));
				break;
			}
		}
	} while (still_running);

	for (i = 0; i < nr; i++) {
		struct s3_download_request *req = requests[i];
		if (req->started)
			curl_multi_remove_handle(curlm, req->curl);
		s3_download_request_free(req);
	}
	free(requests);
	curl_multi_cleanup(curlm);

	trace2_region_leave("s3", "download", storage->repo);
	return ret;
}

struct s3_read_ctx {
	const unsigned char *data;
	size_t remaining;
};

static size_t s3_read_cb(char *dest, size_t size, size_t nmemb, void *payload)
{
	struct s3_read_ctx *ctx = payload;
	size_t n;

	n = size * nmemb;
	if (n > ctx->remaining)
		n = ctx->remaining;

	memcpy(dest, ctx->data, n);
	ctx->data += n;
	ctx->remaining -= n;

	return n;
}

static int s3_upload_buffer(struct s3_storage *storage, const char *key,
			    const unsigned char *data, size_t data_len,
			    const struct s3_etag *if_match,
			    struct s3_etag *etag_out)
{
	char payload_sha256[GIT_SHA256_HEXSZ + 1];
	struct s3_read_ctx ctx = {
		.data = data,
		.remaining = data_len,
	};
	struct curl_slist *hdrs;
	CURL *curl;
	char *url;
	int ret;

	trace2_region_enter_printf("s3", "upload-buffer", storage->repo,
				   "key:%s", key);

	sha256_buf_hex(data, data_len, payload_sha256);

	url = s3_url(storage, key);

	hdrs = sigv4_build_headers(storage, "PUT", key, payload_sha256,
				   if_match, (curl_off_t)data_len);

	curl = curl_easy_init();
	s3_curl_setup_common(storage, curl);
	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
	curl_easy_setopt(curl, CURLOPT_READDATA, &ctx);
	curl_easy_setopt(curl, CURLOPT_READFUNCTION, s3_read_cb);
	curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)data_len);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);
	if (etag_out) {
		curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, s3_etag_header_cb);
		curl_easy_setopt(curl, CURLOPT_HEADERDATA, etag_out);
	}

	ret = s3_perform(curl, key);
	if (ret == -412) {
		error("s3: conflicting write detected");
	} else if (ret < 0 && ret != -1) {
		error("s3: HTTP %d uploading '%s'", -ret, key);
	}

	trace2_region_leave("s3", "upload-buffer", storage->repo);

	curl_slist_free_all(hdrs);
	curl_easy_cleanup(curl);
	free(url);
	return ret ? -1 : 0;
}

/*
 * Per-file state for an upload in flight inside s3_storage_upload_files().
 *
 * Heap-allocated once per transfer; pointers stored on the curl easy
 * handle (CURLOPT_PRIVATE) reference these stable addresses.
 */
struct s3_upload_request {
	CURL *curl;
	FILE *fp;
	const char *key;        /* aliased into the caller's transfer */
	struct curl_slist *hdrs;
	int started;
};

static void s3_upload_request_free(struct s3_upload_request *req)
{
	if (!req)
		return;
	if (req->curl)
		curl_easy_cleanup(req->curl);
	if (req->hdrs)
		curl_slist_free_all(req->hdrs);
	if (req->fp)
		fclose(req->fp);
	free(req);
}

/*
 * Prepare a single upload request: hash the file (for SigV4), open it
 * for reading, and configure a curl easy handle for the PUT. Always
 * returns a non-NULL request; on failure the returned request has its
 * curl handle left NULL (and an error already reported) so the caller
 * can detect failure and skip it when feeding the curl_multi.
 *
 * Hashing and stat happen synchronously here because they are CPU-bound
 * and cheap relative to network RTTs. If we ever observe SHA-256
 * hashing as a bottleneck for very large uploads we can hoist it into a
 * worker.
 */
static struct s3_upload_request *s3_upload_prepare(struct s3_storage *storage,
						   const char *key,
						   const char *local_path)
{
	char payload_sha256[GIT_SHA256_HEXSZ + 1];
	struct s3_upload_request *req;
	struct stat st;
	char *url = NULL;

	CALLOC_ARRAY(req, 1);
	req->key = key;

	if (s3_sha256_file_hex(local_path, payload_sha256) < 0)
		goto out;

	if (stat(local_path, &st) < 0) {
		error_errno("s3: cannot stat '%s'", local_path);
		goto out;
	}

	req->fp = fopen(local_path, "rb");
	if (!req->fp) {
		error_errno("s3: cannot open '%s' for reading", local_path);
		goto out;
	}

	url = s3_url(storage, key);

	req->hdrs = sigv4_build_headers(storage, "PUT", key, payload_sha256,
					NULL, (curl_off_t)st.st_size);

	req->curl = curl_easy_init();
	if (!req->curl)
		die("s3: curl_easy_init failed");
	s3_curl_setup_common(storage, req->curl);
	curl_easy_setopt(req->curl, CURLOPT_URL, url);
	curl_easy_setopt(req->curl, CURLOPT_UPLOAD, 1L);
	curl_easy_setopt(req->curl, CURLOPT_READDATA, req->fp);
	curl_easy_setopt(req->curl, CURLOPT_INFILESIZE_LARGE,
			 (curl_off_t)st.st_size);
	curl_easy_setopt(req->curl, CURLOPT_HTTPHEADER, req->hdrs);
	curl_easy_setopt(req->curl, CURLOPT_PRIVATE, req);
	/* See s3_download_prepare for why this is required. */
	curl_easy_setopt(req->curl, CURLOPT_PIPEWAIT, 1L);

out:
	free(url);
	return req;
}

static int s3_storage_upload_files(struct s3_storage *storage,
				   const struct s3_transfers *transfers)
{
	struct s3_upload_request **requests = NULL;
	CURLM *curlm = NULL;
	size_t nr = transfers->items_nr;
	int still_running = 0;
	int ret = 0;
	size_t i;

	if (!nr)
		return 0;

	trace2_region_enter_printf("s3", "upload", storage->repo,
				   "n:%"PRIuMAX, (uintmax_t)nr);

	CALLOC_ARRAY(requests, nr);
	for (i = 0; i < nr; i++) {
		requests[i] = s3_upload_prepare(storage,
						transfers->keys[i],
						transfers->local_paths[i]);
		if (!requests[i]->curl)
			ret = -1;
	}

	curlm = curl_multi_init();
	if (!curlm)
		die("s3: curl_multi_init failed");
	/*
	 * Allow generous concurrency. curl will negotiate the actual stream
	 * count with the server (HTTP/2 SETTINGS_MAX_CONCURRENT_STREAMS) or
	 * cap us at CURLMOPT_MAX_HOST_CONNECTIONS for HTTP/1.1.
	 */
	curl_multi_setopt(curlm, CURLMOPT_MAX_HOST_CONNECTIONS, 8L);

	for (i = 0; i < nr; i++) {
		struct s3_upload_request *req = requests[i];
		CURLMcode mc;

		if (!req->curl)
			continue; /* failed during prepare */

		mc = curl_multi_add_handle(curlm, req->curl);
		if (mc != CURLM_OK) {
			error("s3: curl_multi_add_handle failed for '%s': %s",
			      req->key, curl_multi_strerror(mc));
			ret = -1;
			continue;
		}
		req->started = 1;
	}

	do {
		CURLMcode mc;
		CURLMsg *msg;
		int msgs_left = 0;

		mc = curl_multi_perform(curlm, &still_running);
		if (mc != CURLM_OK) {
			ret = error("s3: curl_multi_perform: %s",
				    curl_multi_strerror(mc));
			break;
		}

		while ((msg = curl_multi_info_read(curlm, &msgs_left))) {
			struct s3_upload_request *req = NULL;
			long http_code = 0;

			if (msg->msg != CURLMSG_DONE)
				continue;

			curl_easy_getinfo(msg->easy_handle, CURLINFO_PRIVATE,
					  (char **)&req);
			if (!req)
				continue;

			if (msg->data.result != CURLE_OK) {
				error("s3: curl error uploading '%s': %s",
				      req->key,
				      curl_easy_strerror(msg->data.result));
				ret = -1;
				continue;
			}

			curl_easy_getinfo(msg->easy_handle,
					  CURLINFO_RESPONSE_CODE, &http_code);
			if (http_code < 200 || http_code >= 300) {
				error("s3: HTTP %ld uploading '%s'",
				      http_code, req->key);
				ret = -1;
			}
		}

		if (still_running) {
			mc = curl_multi_poll(curlm, NULL, 0, 1000, NULL);
			if (mc != CURLM_OK) {
				ret = error("s3: curl_multi_poll: %s",
					    curl_multi_strerror(mc));
				break;
			}
		}
	} while (still_running);

	for (i = 0; i < nr; i++) {
		struct s3_upload_request *req = requests[i];
		if (req->started)
			curl_multi_remove_handle(curlm, req->curl);
		s3_upload_request_free(req);
	}
	free(requests);
	curl_multi_cleanup(curlm);

	trace2_region_leave("s3", "upload", storage->repo);
	return ret;
}

/*
 * Resolve which manifest version to read. If GIT_S3_MANIFEST is set its value
 * is used directly as the version hash, pinning this process to that exact
 * snapshot. Otherwise the mutable pointer at <prefix>/manifest is fetched.
 */
static char *s3_resolve_manifest_version(struct s3_storage *storage,
					 struct s3_etag *etag)
{
	const char *pinned = getenv("GIT_S3_MANIFEST");
	struct strbuf buf = STRBUF_INIT;
	char *manifest = NULL;
	char *key = NULL;
	int ret;

	if (pinned)
		return xstrdup(pinned);

	key = s3_key(storage, "manifest");
	ret = s3_download_buffer(storage, key, &buf, etag);
	if (ret == -404) {
		/*
		 * There is no manifest, so the repository in question does
		 * not exist or has no objects yet.
		 *
		 * TODO: we shoudln't rely on absence, but should instead rely
		 * on there being an empty manifest that is created by
		 * `create_on_disk()`.
		 */
		manifest = xstrdup("");
		goto out;
	}
	if (ret < 0) {
		if (ret != -1)
			error("s3: HTTP %d fetching manifest pointer", -ret);
		goto out;
	}

	manifest = strbuf_detach(&buf, NULL);

out:
	strbuf_release(&buf);
	free(key);

	return manifest;
}

int s3_storage_write_manifest(struct s3_storage *storage,
			      const struct s3_manifest *manifest,
			      struct strbuf *manifest_path)
{
	struct strbuf content = STRBUF_INIT;
	struct strbuf path = STRBUF_INIT;
	char version_hex[GIT_SHA256_HEXSZ + 1];
	struct tempfile *tempfile = NULL;
	int ret = 0;

	for (size_t i = 0; i < manifest->packs.nr; i++)
		strbuf_addf(&content, "p: %s\n", manifest->packs.items[i].string);

	sha256_buf_hex(content.buf, content.len, version_hex);

	strbuf_addf(&path, "%s/manifests/tmp_manifest_XXXXXX", storage->cache_dir);
	tempfile = xmks_tempfile(path.buf);

	if (write_in_full(get_tempfile_fd(tempfile), content.buf, content.len) < 0) {
		ret = error_errno("s3-storage: cannot write manifest temp file");
		goto out;
	}

	strbuf_reset(&path);
	strbuf_addf(&path, "%s/manifests/%s", storage->cache_dir, version_hex);

	if (rename_tempfile(&tempfile, path.buf) < 0) {
		ret = error_errno("s3-storage: cannot move manifest tempfile into place");
		goto out;
	}

	strbuf_swap(manifest_path, &path);

out:
	delete_tempfile(&tempfile);
	strbuf_release(&content);
	strbuf_release(&path);

	return ret;
}

int s3_transfers_publish(struct s3_storage *storage,
			 const struct s3_manifest *manifest,
			 struct s3_transfers *transfers)
{
	struct s3_etag new_etag = S3_ETAG_INIT;
	struct strbuf path = STRBUF_INIT;
	const char *new_manifest_version;
	char *key = NULL;
	int ret;

	ret = s3_storage_write_manifest(storage, manifest, &path);
	if (ret < 0)
		goto out;

	s3_transfers_append(transfers, path.buf);
	new_manifest_version = path.buf + path.len - GIT_SHA256_HEXSZ;

	ret = s3_storage_upload_files(storage, transfers);
	if (ret < 0) {
		ret = error("s3: failed uploading objects");
		goto out;
	}

	/*
	 * Advance the mutable manifest pointer with a conditional PUT. When
	 * we already have an ETag for the previously observed pointer value
	 * we use If-Match so we do not clobber another writer's update.
	 * Otherwise this is the very first publish for the prefix and we use
	 * If-None-Match: * so we do not overwrite a manifest a concurrent
	 * first-writer may have just created.
	 *
	 * Capture the response ETag so that subsequent updates within this
	 * process can chain CAS without needing to re-read <prefix>/manifest
	 * from S3.
	 */
	key = s3_key(storage, "manifest");
	ret = s3_upload_buffer(storage, key,
			       (unsigned char *)new_manifest_version,
			       strlen(new_manifest_version),
			       &manifest->etag, &new_etag);
	if (ret < 0)
		goto out;

	/*
	 * Update the cached manifest. We copy the caller's manifest contents
	 * (packs/reftables) and then replace the just-copied (stale) ETag
	 * with the fresh one returned by S3 for the pointer object.
	 */
	s3_manifest_release(&storage->manifest);
	s3_manifest_copy(manifest, &storage->manifest);
	s3_etag_release(&storage->manifest.etag);
	SWAP(storage->manifest.etag, new_etag);

out:
	s3_etag_release(&new_etag);
	strbuf_release(&path);
	free(key);
	return ret;
}

const struct s3_manifest *s3_storage_get_manifest(struct s3_storage *storage)
{
	struct s3_manifest manifest = S3_MANIFEST_INIT;
	struct string_list lines = STRING_LIST_INIT_DUP;
	struct strbuf content = STRBUF_INIT;
	char *path = NULL, *version = NULL;
	int ret;

	if (storage->manifest_initialized)
		return &storage->manifest;

	/*
	 * Resolve the current manifest version pointer and capture its
	 * ETag. The ETag is preserved on storage->manifest so that we can
	 * use it as the If-Match precondition when we later publish a
	 * compare-and-swap update of the pointer.
	 */
	version = s3_resolve_manifest_version(storage, &manifest.etag);
	if (!version)
		die("s3-storage: failed fetching manifest version");
	if (!*version) {
		/*
		 * TODO: eventually we should make it a hard error if there's
		 * no manifest yet, as this indicates that something may be
		 * wrong.
		 */
		storage->manifest = manifest;
		storage->manifest_initialized = true;
		goto out;
	}

	/*
	 * Verify whether we already have the manifest available locally. If
	 * not, fetch it from S3.
	 */
	path = xstrfmt("%s/manifests/%s", storage->cache_dir, version);
	if (access(path, R_OK) < 0) {
		struct s3_transfers transfers = S3_TRANSFERS_INIT(storage);

		if (errno != ENOENT)
			die_errno("s3: failed statting manifest '%s'", path);

		s3_transfers_append(&transfers, path);

		ret = s3_storage_download_files(storage, &transfers);
		if (ret < 0)
			die("s3-storage: failed fetching manifest");

		s3_transfers_release(&transfers);
	}

	if (strbuf_read_file(&content, path, 0) < 0)
		die_errno("s3-storage: failed reading manifest '%s'", path);

	strbuf_trim(&content);
	string_list_split(&lines, content.buf, "\n", -1);

	for (size_t i = 0; i < lines.nr; i++) {
		struct string_list_item *item = &lines.items[i];
		if (item->string[0] == 'p')
			string_list_append(&manifest.packs, item->string + 3);
	}

	storage->manifest = manifest;
	storage->manifest_initialized = true;

out:
	string_list_clear(&lines, 0);
	strbuf_release(&content);
	free(version);
	free(path);
	return &storage->manifest;
}

static char *region_from_host(const char *host)
{
	const char *s3dot = strstr(host, "s3.");
	const char *amazonaws = strstr(host, ".amazonaws.com");
	if (!amazonaws || !s3dot || s3dot >= amazonaws)
		return xstrdup("us-east-1");
	return xstrndup(s3dot + 3, amazonaws - (s3dot + 3));
}

static struct strmap storages_by_url = STRMAP_INIT;

struct s3_storage *s3_storage_get(struct repository *repo,
				  const char *url)
{
	struct s3_manifest empty_manifest = S3_MANIFEST_INIT;
	const char *after_scheme, *host_end, *slash;
	struct strbuf path = STRBUF_INIT;
	struct s3_storage *storage;

	storage = strmap_get(&storages_by_url, url);
	if (storage) {
		storage->refcount++;
		return storage;
	}

	after_scheme = strstr(url, "://");
	if (!after_scheme)
		die(_("s3-storage: malformed URL '%s': missing scheme"), url);
	after_scheme += 3;

	host_end = strchr(after_scheme, '/');
	if (!host_end)
		die(_("s3-storage: URL must include a bucket name: '%s'"), url);

	slash = strchr(host_end + 1, '/');
	if (!slash)
		die(_("s3-storage: URL is missing repository prefix: '%s'"), url);

	CALLOC_ARRAY(storage, 1);
	storage->repo = repo;
	storage->refcount++;
	storage->url = xstrdup(url);
	storage->endpoint_url = xstrndup(url, host_end - url);
	storage->host = xstrndup(after_scheme, host_end - after_scheme);
	storage->region = region_from_host(after_scheme);
	storage->bucket = xstrndup(host_end + 1, slash - (host_end + 1));
	storage->prefix = xstrdup(slash + 1);
	storage->cache_dir = xstrfmt("%s/s3-cache", repo->commondir);
	storage->manifest = empty_manifest;
	if (init_recursive_mutex(&storage->curl_share_mutex))
		die("s3: failed initializing curl share mutex");

	if (mkdir(storage->cache_dir, 0777) < 0 && errno != EEXIST)
		die_errno("s3-storage: cannot create cache dir '%s'", storage->cache_dir);

	strbuf_addf(&path, "%s/packs", storage->cache_dir);
	if (mkdir(path.buf, 0777) < 0 && errno != EEXIST)
		die_errno("s3-storage: cannot create '%s'", path.buf);
	strbuf_reset(&path);
	strbuf_addf(&path, "%s/manifests", storage->cache_dir);
	if (mkdir(path.buf, 0777) < 0 && errno != EEXIST)
		die_errno("s3-storage: cannot create '%s'", path.buf);
	strbuf_release(&path);

	storage->access_key_id = xstrdup_or_null(getenv("S3_KEY_ID"));
	storage->secret_access_key = xstrdup_or_null(getenv("S3_KEY_SECRET"));
	if (!storage->access_key_id || !storage->secret_access_key)
		die("s3-storage: no credentials found; set S3_KEY_ID and S3_KEY_SECRET");

	if (strmap_put(&storages_by_url, url, storage))
		BUG("s3 storage already cached");

	return storage;
}

void s3_storage_release(struct s3_storage *storage)
{
	if (!storage)
		return;
	if (storage->refcount-- > 1)
		return;

	strmap_remove(&storages_by_url, storage->url, 0);

	if (storage->curl_share)
		curl_share_cleanup(storage->curl_share);
	pthread_mutex_destroy(&storage->curl_share_mutex);

	s3_manifest_release(&storage->manifest);
	free(storage->url);
	free(storage->endpoint_url);
	free(storage->host);
	free(storage->region);
	free(storage->bucket);
	free(storage->prefix);
	free(storage->cache_dir);
	free(storage->access_key_id);
	free(storage->secret_access_key);
	free(storage);
}

void s3_manifest_release(struct s3_manifest *manifest)
{
	s3_etag_release(&manifest->etag);
	string_list_clear(&manifest->packs, 0);
}

void s3_manifest_copy(const struct s3_manifest *from,
		      struct s3_manifest *to)
{
	struct string_list_item *item;
	s3_etag_copy(&from->etag, &to->etag);
	for_each_string_list_item(item, &from->packs)
		string_list_append(&to->packs, item->string);
}
