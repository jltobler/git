#define USE_THE_REPOSITORY_VARIABLE

#include "git-compat-util.h"
#include "csum-file.h"
#include "environment.h"
#include "gettext.h"
#include "git-zlib.h"
#include "hash.h"
#include "hex.h"
#include "object-file.h"
#include "odb/source.h"
#include "odb/source-packed.h"
#include "odb/source-s3.h"
#include "odb/streaming.h"
#include "odb/transaction.h"
#include "pack.h"
#include "packfile.h"
#include "path.h"
#include "repository.h"
#include "strbuf.h"
#include "string-list.h"
#include "tempfile.h"
#include "wrapper.h"
#include "write-or-die.h"

#include <curl/curl.h>
#include <curl/easy.h>

static void sha256_to_hex(const unsigned char *digest, char *out)
{
	hash_to_hex_algop_r(out, digest, &hash_algos[GIT_HASH_SHA256]);
}

static void sha256_buf_hex(const void *buf, size_t len,
			   char out[GIT_SHA256_HEXSZ + 1])
{
	git_SHA256_CTX ctx;
	unsigned char digest[GIT_SHA256_RAWSZ];
	git_SHA256_Init(&ctx);
	git_SHA256_Update(&ctx, buf, len);
	git_SHA256_Final(digest, &ctx);
	sha256_to_hex(digest, out);
}

static int sha256_file_hex(const char *path, char out[GIT_SHA256_HEXSZ + 1])
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
			     const char *date_str,  /* "YYYYMMDD" */
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
 * host           - authority of the endpoint URL, e.g. "s3.us-east-1.amazonaws.com"
 * path           - URI path component, e.g. "/pack/pack-abc.pack"
 * payload_sha256 - lowercase hex SHA-256 of the request body
 * content_length - body size in bytes; ignored for GET (pass 0)
 *
 * Returns a newly allocated curl_slist that the caller must free with
 * curl_slist_free_all().  Also allocates *auth_header_out which the caller
 * must free(); it is already appended to the returned slist.
 */
static struct curl_slist *sigv4_build_headers(struct odb_source_s3 *s3,
					      const char *method,
					      const char *host,
					      const char *path,
					      const char *payload_sha256,
					      curl_off_t content_length,
					      char **auth_header_out)
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
	char sig_hex[GIT_SHA256_HEXSZ + 1];
	char date_time[17];
	char date_only[9];
	struct tm gmt;
	time_t now;

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
			    "content-length:%"PRIdMAX"\n",
			    (intmax_t)content_length);
	}
	strbuf_addf(&canonical_hdrs, "host:%s\n", host);
	strbuf_addf(&canonical_hdrs, "x-amz-content-sha256:%s\n",
		    payload_sha256);
	strbuf_addf(&canonical_hdrs, "x-amz-date:%s\n", date_time);

	if (is_put)
		strbuf_addstr(&signed_hdrs, "content-length;");
	strbuf_addstr(&signed_hdrs, "host;x-amz-content-sha256;x-amz-date");

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
		    date_only, s3->region,
		    canonical_req_hash);
	sigv4_derive_key(s3->secret_access_key, date_only,
			 s3->region, signing_key);
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
		    s3->access_key_id, date_only, s3->region,
		    signed_hdrs.buf, sig_hex);
	strbuf_release(&signed_hdrs);

	*auth_header_out = strbuf_detach(&auth, NULL);

	/* Assemble the curl header list. */
	strbuf_addf(&hdr, "Host: %s", host);
	hdrs = curl_slist_append(hdrs, hdr.buf);

	strbuf_reset(&hdr);
	strbuf_addf(&hdr, "x-amz-date: %s", date_time);
	hdrs = curl_slist_append(hdrs, hdr.buf);

	strbuf_reset(&hdr);
	strbuf_addf(&hdr, "x-amz-content-sha256: %s", payload_sha256);
	hdrs = curl_slist_append(hdrs, hdr.buf);

	if (is_put) {
		strbuf_reset(&hdr);
		strbuf_addf(&hdr, "Content-Length: %"PRIdMAX, (intmax_t)content_length);
		hdrs = curl_slist_append(hdrs, hdr.buf);
	}

	hdrs = curl_slist_append(hdrs, *auth_header_out);

	strbuf_release(&string_to_sign);
	strbuf_release(&canonical_req);
	strbuf_release(&canonical_hdrs);
	strbuf_release(&hdr);
	return hdrs;
}

/* Build an S3 key by joining the source's prefix with a suffix. */
static const char *s3_key(const struct odb_source_s3 *s3, struct strbuf *buf, const char *suffix)
{
	strbuf_reset(buf);
	strbuf_addf(buf, "%s/%s", s3->prefix, suffix);
	return buf->buf;
}

/*
 * Return the HTTP Host value and canonical-URI path used for both the actual
 * request and SigV4 signing.  We always use path-style addressing:
 *
 *   host = "<host>[:<port>]"    (authority from endpoint_url)
 *   path = "/<bucket>/<key>"
 *
 * Both returned strings are heap-allocated; the caller must free() them.
 */
static void s3_request_parts(const struct odb_source_s3 *s3, const char *key,
			     char **host_out, char **path_out)
{
	const char *authority = strstr(s3->endpoint_url, "://");
	*host_out = xstrdup(authority ? authority + 3 : s3->endpoint_url);
	*path_out = xstrfmt("/%s/%s", s3->bucket, key);
}

static char *s3_url(const struct odb_source_s3 *s3, const char *key)
{
	return xstrfmt("%s/%s/%s", s3->endpoint_url, s3->bucket, key);
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

/* SHA-256 of the empty string (used as the payload hash for GET requests). */
#define SHA256_EMPTY "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

static int s3_get_to_buf(struct odb_source_s3 *s3, const char *key,
			 struct strbuf *out)
{
	char *url, *host, *path, *auth_hdr;
	struct curl_slist *hdrs;
	CURL *curl;
	int ret;

	s3_request_parts(s3, key, &host, &path);
	url = s3_url(s3, key);

	hdrs = sigv4_build_headers(s3, "GET", host, path, SHA256_EMPTY,
				   0, &auth_hdr);

	curl = curl_easy_init();
	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, s3_write_cb);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, out);
	/* We want to check the status code ourselves. */
	curl_easy_setopt(curl, CURLOPT_FAILONERROR, 0L);

	ret = s3_perform(curl, key);

	curl_easy_cleanup(curl);
	curl_slist_free_all(hdrs);
	free(auth_hdr);
	free(host);
	free(path);
	free(url);
	return ret;
}

static int s3_get_to_file(struct odb_source_s3 *s3, const char *key,
			  const char *local_path)
{
	char *url = NULL, *host = NULL, *path = NULL, *auth_hdr = NULL;
	struct strbuf template = STRBUF_INIT;
	struct curl_slist *hdrs = NULL;
	struct tempfile *tempfile;
	CURL *curl = NULL;
	FILE *fp;
	int ret;

	strbuf_addf(&template, "%s.XXXXXX", local_path);
	tempfile = xmks_tempfile(template.buf);

	fp = fdopen_tempfile(tempfile, "w");
	if (!fp) {
		ret = error_errno("s3: cannot open '%s' for writing",
				  local_path);
		goto out;
	}

	s3_request_parts(s3, key, &host, &path);
	url = s3_url(s3, key);

	hdrs = sigv4_build_headers(s3, "GET", host, path, SHA256_EMPTY,
				   0, &auth_hdr);

	curl = curl_easy_init();
	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, fp);

	ret = s3_perform(curl, key);
	if (ret < 0 && ret != -1) {
		ret = error("s3: HTTP %d downloading '%s'", -ret, key);
		goto out;
	}

	if (rename_tempfile(&tempfile, local_path) < 0) {
		ret = error_errno("failed committing tempfile '%s'", local_path);
		goto out;
	}

out:
	delete_tempfile(&tempfile);
	curl_easy_cleanup(curl);
	curl_slist_free_all(hdrs);
	strbuf_release(&template);
	free(auth_hdr);
	free(host);
	free(path);
	free(url);
	return ret ? -1 : 0;
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

static int s3_put_from_buffer(struct odb_source_s3 *s3, const char *key,
			      const unsigned char *data, size_t data_len)
{
	char payload_sha256[GIT_SHA256_HEXSZ + 1];
	char *url, *path, *host, *auth_hdr;
	struct s3_read_ctx ctx = {
		.data = data,
		.remaining = data_len,
	};
	struct curl_slist *hdrs;
	CURL *curl;
	int ret;

	sha256_buf_hex(data, data_len, payload_sha256);

	s3_request_parts(s3, key, &host, &path);
	url = s3_url(s3, key);

	hdrs = sigv4_build_headers(s3, "PUT", host, path, payload_sha256,
				   (curl_off_t)data_len, &auth_hdr);

	curl = curl_easy_init();
	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
	curl_easy_setopt(curl, CURLOPT_READDATA, &ctx);
	curl_easy_setopt(curl, CURLOPT_READFUNCTION, s3_read_cb);
	curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t) data_len);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);

	ret = s3_perform(curl, key);
	if (ret < 0 && ret != -1)
		error("s3: HTTP %d uploading '%s'", -ret, key);

	curl_easy_cleanup(curl);
	curl_slist_free_all(hdrs);
	free(auth_hdr);
	free(host);
	free(path);
	free(url);
	return ret ? -1 : 0;
}

/*
 * Upload the local file at `local_path` to the S3 object at `key`.
 * Returns 0 on success, -1 on error.
 */
static int s3_put_from_file(struct odb_source_s3 *s3, const char *key,
			    const char *local_path)
{
	char *url = NULL, *path = NULL, *host = NULL, *auth_hdr = NULL;
	char payload_sha256[GIT_SHA256_HEXSZ + 1];
	struct curl_slist *hdrs = NULL;
	CURL *curl = NULL;
	FILE *fp = NULL;
	struct stat st;
	int ret;

	ret = sha256_file_hex(local_path, payload_sha256);
	if (ret < 0)
		goto out;

	if (stat(local_path, &st) < 0) {
		ret = error_errno("s3: cannot stat '%s'", local_path);
		goto out;
	}

	fp = fopen(local_path, "rb");
	if (!fp) {
		ret = error_errno("s3: cannot open '%s' for reading",
				  local_path);
		goto out;
	}

	s3_request_parts(s3, key, &host, &path);
	url = s3_url(s3, key);

	hdrs = sigv4_build_headers(s3, "PUT", host, path, payload_sha256,
				   (curl_off_t)st.st_size, &auth_hdr);

	curl = curl_easy_init();
	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
	curl_easy_setopt(curl, CURLOPT_READDATA, fp);
	curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE,
			 (curl_off_t)st.st_size);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);

	ret = s3_perform(curl, key);
	if (ret < 0 && ret != -1) {
		ret = error("s3: HTTP %d uploading '%s'", -ret, key);
		goto out;
	}

out:
	curl_easy_cleanup(curl);
	curl_slist_free_all(hdrs);
	fclose(fp);
	free(auth_hdr);
	free(host);
	free(path);
	free(url);
	return ret ? -1 : 0;
}

/*
 * Resolve which manifest version to read. If GIT_S3_MANIFEST is set its value
 * is used directly as the version hash, pinning this process to that exact
 * snapshot. Otherwise the mutable pointer at <prefix>/manifest is fetched.
 */
static char *s3_resolve_manifest_version(struct odb_source_s3 *s3)
{
	const char *pinned = getenv("GIT_S3_MANIFEST");
	struct strbuf buf = STRBUF_INIT, key = STRBUF_INIT;
	char *manifest = NULL;
	int ret;

	if (pinned)
		return xstrdup(pinned);

	ret = s3_get_to_buf(s3, s3_key(s3, &key, "manifest"), &buf);
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
	strbuf_release(&key);
	return manifest;
}

static int s3_fetch_manifest(struct odb_source_s3 *s3,
			     struct string_list *out)
{
	struct strbuf manifest = STRBUF_INIT;
	struct strbuf key = STRBUF_INIT;
	char *path = NULL, *version = NULL;
	int ret;

	version = s3_resolve_manifest_version(s3);
	if (!version) {
		ret = -1;
		goto out;
	}
	if (!*version) {
		/* No manifest exists yet, start with an empty pack list. */
		ret = 0;
		goto out;
	}

	/*
	 * Verify whether we already have the manifest available locally. If
	 * not, fetch it from S3.
	 */
	path = xstrfmt("%s/manifests/%s", s3->cache_dir, version);
	if (access(path, R_OK) < 0) {
		if (errno != ENOENT) {
			ret = error_errno("s3: failed statting manifest '%s'", path);
			goto out;
		}

		ret = s3_get_to_file(s3, s3_key(s3, &key, path + strlen(s3->cache_dir) + 1), path);
		if (ret < 0)
			goto out;
	}

	if (strbuf_read_file(&manifest, path, 0) < 0) {
		ret = error_errno("failed reading manifest '%s'", path);
		goto out;
	}

	strbuf_trim(&manifest);
	string_list_split(out, manifest.buf, "\n", -1);

	ret = 0;

out:
	strbuf_release(&manifest);
	strbuf_release(&key);
	free(version);
	free(path);
	return ret;
}

/*
 * Fetch the manifest, download any packs that are not yet in the local cache,
 * and load them into the embedded odb_source_packed.
 */
static void odb_source_s3_prepare(struct odb_source_s3 *s3)
{
	struct string_list manifest = STRING_LIST_INIT_DUP;
	struct strbuf key = STRBUF_INIT;

	if (s3->initialized)
		return;

	if (s3_fetch_manifest(s3, &manifest) < 0)
		die("s3: failed to fetch manifest");

	for (size_t i = 0; i < manifest.nr; i++) {
		const char *hash = manifest.items[i].string;
		char *pack_path, *idx_path;

		pack_path = xstrfmt("%s/packs/%s.pack", s3->cache_dir, hash);
		if (access(pack_path, R_OK) < 0) {
			const char *suffix = pack_path + strlen(s3->cache_dir) + 1;
			if (s3_get_to_file(s3, s3_key(s3, &key, suffix), pack_path) < 0)
				die("failed downloading pack '%s'", key.buf);
		}

		idx_path = xstrfmt("%s/packs/%s.idx",  s3->cache_dir, hash);
		if (access(idx_path, R_OK) < 0) {
			const char *suffix = idx_path + strlen(s3->cache_dir) + 1;
			if (s3_get_to_file(s3, s3_key(s3, &key, suffix), idx_path)  < 0)
				die("failed downloading index '%s'", key.buf);
		}

		/* Register the pack with the embedded packed store. */
		if (!packfile_store_load_pack(s3->packed, idx_path, 1))
			die("failed to load pack '%s'", hash);

		free(pack_path);
		free(idx_path);
	}

	s3->initialized = true;

	string_list_clear(&manifest, 0);
	strbuf_release(&key);
}

static void odb_source_s3_free(struct odb_source *source)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);

	odb_source_free(&s3->packed->base);
	free(s3->bucket);
	free(s3->prefix);
	free(s3->cache_dir);
	free(s3->access_key_id);
	free(s3->secret_access_key);
	free(s3->region);
	free(s3->endpoint_url);
	odb_source_release(source);
	free(s3);
}

static void odb_source_s3_close(struct odb_source *source)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	odb_source_close(&s3->packed->base);
}

static void odb_source_s3_reprepare(struct odb_source *source)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);

	/*
	 * Drop the initialized flag so that the next access re-fetches the
	 * manifest. The embedded packed store keeps its existing packs; new
	 * ones discovered in the refreshed manifest are added incrementally.
	 */
	s3->initialized = false;
	odb_source_s3_prepare(s3);
}

static int odb_source_s3_read_object_info(struct odb_source *source,
					  const struct object_id *oid,
					  struct object_info *oi,
					  enum object_info_flags flags)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	odb_source_s3_prepare(s3);

	/* Strip `OBJECT_INFO_SECOND_READ` so that we don't end up repreparing. */
	return odb_source_read_object_info(&s3->packed->base, oid, oi,
					   flags & ~OBJECT_INFO_SECOND_READ);
}

static int odb_source_s3_read_object_stream(struct odb_read_stream **out,
					    struct odb_source *source,
					    const struct object_id *oid)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	odb_source_s3_prepare(s3);
	return odb_source_read_object_stream(out, &s3->packed->base, oid);
}

static int odb_source_s3_for_each_object(
	struct odb_source *source,
	const struct object_info *request,
	odb_for_each_object_cb cb,
	void *cb_data,
	const struct odb_for_each_object_options *opts)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	odb_source_s3_prepare(s3);
	return odb_source_for_each_object(&s3->packed->base, request,
					  cb, cb_data, opts);
}

static int odb_source_s3_count_objects(struct odb_source *source,
				       enum odb_count_objects_flags flags,
				       unsigned long *out)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	odb_source_s3_prepare(s3);
	return odb_source_count_objects(&s3->packed->base, flags, out);
}

static int odb_source_s3_find_abbrev_len(struct odb_source *source,
					 const struct object_id *oid,
					 unsigned min_len,
					 unsigned *out)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	odb_source_s3_prepare(s3);
	return odb_source_find_abbrev_len(&s3->packed->base, oid,
					  min_len, out);
}

static int odb_source_s3_freshen_object(struct odb_source *source,
					const struct object_id *oid,
					const time_t *mtime)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	odb_source_s3_prepare(s3);
	return odb_source_freshen_object(&s3->packed->base, oid, mtime);
}

struct s3_pending_object {
	struct object_id oid;
	enum object_type type;
	unsigned char *data;
	unsigned long len;
};

static int write_pending_to_pack(struct odb_source_s3 *s3,
				 struct s3_pending_object *objects,
				 size_t objects_nr,
				 char **pack_path_out,
				 char **idx_path_out)
{
	struct repository *repo = s3->base.odb->repo;
	const struct git_hash_algo *algo = repo->hash_algo;
	char *pack_tmp_path = NULL, *pack_path = NULL, *idx_path = NULL;
	unsigned char pack_hash[GIT_MAX_RAWSZ];
	struct pack_idx_entry **idx_entries;
	struct pack_idx_option idx_opts;
	struct hashfile *f = NULL;
	struct tempfile *tempfile;
	int ret;

	pack_tmp_path = xstrfmt("%s/packs/tmp_pack_XXXXXX", s3->cache_dir);
	tempfile = xmks_tempfile(pack_tmp_path);
	f = hashfd(algo, get_tempfile_fd(tempfile), pack_tmp_path);

	CALLOC_ARRAY(idx_entries, objects_nr);
	for (size_t i = 0; i < objects_nr; i++)
		CALLOC_ARRAY(idx_entries[i], 1);

	write_pack_header(f, objects_nr);

	for (size_t i = 0; i < objects_nr; i++) {
		struct s3_pending_object *obj = &objects[i];
		unsigned long compressed_bound;
		unsigned char *compressed;
		unsigned char hdr[10];
		git_zstream zs;
		int hdr_len;

		idx_entries[i]->offset = hashfile_total(f);
		oidcpy(&idx_entries[i]->oid, &obj->oid);

		hdr_len = encode_in_pack_object_header(hdr, sizeof(hdr),
						       obj->type, obj->len);
		crc32_begin(f);
		hashwrite(f, hdr, hdr_len);

		git_deflate_init(&zs, zlib_compression_level);
		compressed_bound = git_deflate_bound(&zs, obj->len);
		compressed = xmalloc(compressed_bound);

		zs.next_in = obj->data;
		zs.avail_in = obj->len;
		zs.next_out = compressed;
		zs.avail_out = compressed_bound;

		if (git_deflate(&zs, Z_FINISH) != Z_STREAM_END) {
			git_deflate_end(&zs);
			free(compressed);
			ret = error("s3: deflate failed for object %s",
				    oid_to_hex(&obj->oid));
			goto out;
		}

		hashwrite(f, compressed, zs.total_out);
		idx_entries[i]->crc32 = crc32_end(f);

		git_deflate_end(&zs);
		free(compressed);
	}

	/*
	 * Finalise the pack file. This appends the pack's trailing hash,
	 * writes it into pack_hash[], and closes the file descriptor.
	 */
	finalize_hashfile(f, pack_hash, FSYNC_COMPONENT_PACK,
			  CSUM_HASH_IN_STREAM | CSUM_FSYNC);
	f = NULL;

	pack_path = xstrfmt("%s/packs/%s.pack", s3->cache_dir,
			    hash_to_hex_algop(pack_hash, algo));
	if (rename_tempfile(&tempfile, pack_path) < 0) {
		ret = error_errno("s3: cannot move packfile into place '%s'", pack_path);
		goto out;
	}

	reset_pack_idx_option(&idx_opts);
	idx_path = xstrfmt("%s/packs/%s.idx", s3->cache_dir,
			   hash_to_hex_algop(pack_hash, algo));
	write_idx_file(repo, idx_path, idx_entries, objects_nr,
		       &idx_opts, pack_hash);

	*pack_path_out = pack_path;
	pack_path = NULL;
	*idx_path_out = idx_path;
	idx_path = NULL;

	ret = 0;

out:
	for (size_t i = 0; i < objects_nr; i++)
		free(idx_entries[i]);
	delete_tempfile(&tempfile);
	free(pack_tmp_path);
	free(idx_entries);
	free(pack_path);
	free(idx_path);
	if (f)
		free_hashfile(f);
	return ret;
}

/*
 * Write a new manifest version to S3 and advance the mutable pointer.
 *
 * The pack-hash list is serialised and content-addressed: the file is uploaded
 * to <prefix>/manifests/<sha256-of-content>, then the pointer at
 * <prefix>/manifest is updated to hold that hash.
 */
static int s3_write_manifest(struct odb_source_s3 *s3,
			     const struct string_list *list)
{
	struct strbuf content = STRBUF_INIT;
	struct strbuf path = STRBUF_INIT;
	struct strbuf key = STRBUF_INIT;
	char version_hex[GIT_SHA256_HEXSZ + 1];
	struct tempfile *tempfile = NULL;
	int ret;

	/* Serialise the pack-hash list. */
	for (size_t i = 0; i < list->nr; i++)
		strbuf_addf(&content, "%s\n", list->items[i].string);

	sha256_buf_hex(content.buf, content.len, version_hex);

	strbuf_addf(&path, "%s/manifests/tmp_manifest_XXXXXX", s3->cache_dir);
	tempfile = xmks_tempfile(path.buf);

	if (write_in_full(get_tempfile_fd(tempfile), content.buf, content.len) < 0) {
		ret = error_errno("s3: cannot write manifest temp file");
		goto out;
	}

	strbuf_reset(&path);
	strbuf_addf(&path, "%s/manifests/%s", s3->cache_dir, version_hex);

	if (rename_tempfile(&tempfile, path.buf) < 0) {
		ret = error_errno("cannot move tempfile into place");
		goto out;
	}

	/* Store the hashed path of the manifest. */
	s3_key(s3, &key, path.buf + strlen(s3->cache_dir) + 1);
	ret = s3_put_from_file(s3, key.buf, path.buf);
	if (ret < 0)
		goto out;

	/*
	 * Update the manifest pointer.
	 *
	 * TODO: we should use locking semantics on the local filesystem and
	 * add a compare-and-swap on the object storage.
	 */
	ret = s3_put_from_buffer(s3, s3_key(s3, &key, "manifest"),
				 (unsigned char *)version_hex, strlen(version_hex));

out:
	delete_tempfile(&tempfile);
	strbuf_release(&content);
	strbuf_release(&path);
	strbuf_release(&key);
	return ret;
}

static int write_objects(struct odb_source_s3 *s3,
			 struct s3_pending_object *objects,
			 size_t objects_nr)
{
	struct string_list manifest = STRING_LIST_INIT_DUP;
	char *pack_path = NULL, *idx_path = NULL;
	const char *pack_basename, *idx_basename, *hash_end;
	struct strbuf key = STRBUF_INIT;
	int ret = 0;

	if (!objects_nr)
		return 0;

	if (write_pending_to_pack(s3, objects, objects_nr,
				  &pack_path, &idx_path) < 0) {
		ret = -1;
		goto out;
	}

	/* Upload the .pack and .idx to S3. */
	pack_basename = strrchr(pack_path, '/') + 1;
	s3_key(s3, &key, pack_path + strlen(s3->cache_dir) + 1);
	if (s3_put_from_file(s3, key.buf, pack_path) < 0) {
		ret = error("failed uploading pack '%s'", pack_basename);
		goto out;
	}

	idx_basename = strrchr(idx_path, '/') + 1;
	s3_key(s3, &key, idx_path + strlen(s3->cache_dir) + 1);
	if (s3_put_from_file(s3, key.buf, idx_path) < 0) {
		ret = error("failed uploading idx '%s'", idx_basename);
		goto out;
	}

	/* Fetch the current manifest, append the hash, re-upload. */
	if (s3_fetch_manifest(s3, &manifest) < 0) {
		ret = -1;
		goto out;
	}

	hash_end = strrchr(pack_basename, '.');
	string_list_append_nodup(&manifest, xstrndup(pack_basename, hash_end - pack_basename));

	if (s3_write_manifest(s3, &manifest) < 0) {
		ret = -1;
		goto out;
	}

	if (!packfile_store_load_pack(s3->packed, idx_path, 1))
		die("s3: failed to activate newly written pack '%s'", pack_basename);

out:
	strbuf_release(&key);
	string_list_clear(&manifest, 0);
	free(pack_path);
	free(idx_path);
	return ret;
}

static int odb_source_s3_write_object(struct odb_source *source,
				      const void *buf,
				      unsigned long len,
				      enum object_type type,
				      const struct object_id *oid,
				      const struct object_id *compat_oid UNUSED,
				      const time_t *mtime UNUSED,
				      enum odb_write_object_flags flags UNUSED)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	struct s3_pending_object object = {
		.type = type,
		.len = len,
		.data = (unsigned char *) buf,
	};

	oidcpy(&object.oid, oid);

	return write_objects(s3, &object, 1);
}

static int read_write_stream(struct odb_write_stream *stream,
			     size_t len, unsigned char **out)
{
	unsigned char buf[16384];
	unsigned char *data;
	size_t total_read = 0;
	int ret;

	CALLOC_ARRAY(data, len);
	while (!stream->is_finished) {
		ssize_t bytes_read = odb_write_stream_read(stream, buf, sizeof(buf));
		if (bytes_read < 0) {
			ret = error("s3: stream read error");
			goto out;
		}

		if (total_read + (size_t)bytes_read > len) {
			ret = error("s3: stream yielded more bytes than expected");
			goto out;
		}

		memcpy(data + total_read, buf, bytes_read);
		total_read += bytes_read;
	}

	if (total_read != len) {
		ret = error("s3: stream yielded fewer bytes than expected (%"PRIuMAX" of %"PRIuMAX")",
			    (uintmax_t)total_read, (uintmax_t)len);
		goto out;
	}

	*out = data;
	data = NULL;
	ret = 0;

out:
	free(data);
	return ret;
}

static int odb_source_s3_write_object_stream(struct odb_source *source,
					     struct odb_write_stream *stream,
					     size_t len,
					     struct object_id *oid)
{
	struct object_id computed_oid;
	unsigned char *data = NULL;
	int ret;

	ret = read_write_stream(stream, len, &data);
	if (ret)
		goto out;

	hash_object_file(source->odb->repo->hash_algo, data, len,
			 OBJ_BLOB, &computed_oid);
	oidcpy(oid, &computed_oid);
	ret = odb_source_s3_write_object(source, data, len, OBJ_BLOB, oid,
					 NULL, NULL, 0);

out:
	free(data);
	return ret;
}

struct odb_transaction_s3 {
	struct odb_transaction base;
	struct odb_source_s3 *s3;
	struct s3_pending_object *objects;
	size_t objects_nr, objects_alloc;
};

static void odb_transaction_s3_commit(struct odb_transaction *base)
{
	struct odb_transaction_s3 *tx =
		container_of(base, struct odb_transaction_s3, base);

	if (write_objects(tx->s3, tx->objects, tx->objects_nr) < 0)
		die("s3: failed to flush objects on transaction commit");

	for (size_t i = 0; i < tx->objects_nr; i++)
		free(tx->objects[i].data);
	free(tx->objects);
}

static int odb_transaction_s3_write_object_stream(struct odb_transaction *base,
						  struct odb_write_stream *stream,
						  size_t len,
						  struct object_id *oid)
{
	struct odb_transaction_s3 *tx = container_of(base, struct odb_transaction_s3, base);
	unsigned char *data;
	int ret;

	ret = read_write_stream(stream, len, &data);
	if (ret)
		return ret;

	ALLOC_GROW(tx->objects, tx->objects_nr, tx->objects_alloc);
	tx->objects[tx->objects_nr].type = OBJ_BLOB;
	tx->objects[tx->objects_nr].data = data;
	tx->objects[tx->objects_nr].len = len;
	hash_object_file(tx->s3->base.odb->repo->hash_algo, data, len,
			 OBJ_BLOB, &tx->objects[tx->objects_nr].oid);
	oidcpy(oid, &tx->objects[tx->objects_nr].oid);

	tx->objects_nr++;

	return 0;
}

static int odb_source_s3_begin_transaction(struct odb_source *source,
					   struct odb_transaction **out)
{
	struct odb_transaction_s3 *tx;

	CALLOC_ARRAY(tx, 1);
	tx->s3 = odb_source_s3_downcast(source);
	tx->base.source = source;
	tx->base.commit = odb_transaction_s3_commit;
	tx->base.write_object_stream = odb_transaction_s3_write_object_stream;

	*out = &tx->base;
	return 0;
}

static int odb_source_s3_read_alternates(struct odb_source *source UNUSED,
					 struct strvec *out UNUSED)
{
	return 0;
}

static int odb_source_s3_write_alternate(struct odb_source *source UNUSED,
					 const char *alternate UNUSED)
{
	return error("s3 source: alternates are not supported");
}

static int odb_source_s3_get_packs(struct odb_source *source,
				   struct packfile_list_entry **out)
{
	struct odb_source_s3 *s3 = odb_source_s3_downcast(source);
	odb_source_s3_prepare(s3);
	return odb_source_get_packs(&s3->packed->base, out);
}

static char *region_from_host(const char *host)
{
	const char *s3dot = strstr(host, "s3.");
	const char *amazonaws = strstr(host, ".amazonaws.com");
	const char *colon = strchr(host, '/');
	if (!amazonaws || colon <= amazonaws || !s3dot || s3dot >= amazonaws)
		return xstrdup("us-east-1");
	return xstrndup(s3dot + 3, amazonaws - (s3dot + 3));
}

static void s3_parse_url(const char *url,
			 char **endpoint_url_out,
			 char **bucket_out, char **prefix_out,
			 char **region_out)
{
	const char *after_scheme;
	const char *host_end;
	const char *path;
	const char *slash;

	after_scheme = strstr(url, "://");
	if (!after_scheme)
		die(_("s3: malformed URL '%s': missing scheme"), url);
	after_scheme += 3;

	host_end = strchr(after_scheme, '/');
	if (!host_end)
		die(_("s3: URL must include a bucket name: '%s'"), url);

	*endpoint_url_out = xstrndup(url, host_end - url);
	*region_out = region_from_host(after_scheme);

	/* Split the path "/bucket[/prefix]" into its components. */
	path = host_end + 1;  /* skip the leading '/' */
	slash = strchr(path, '/');
	if (!slash)
		die("URL is missing repository prefix");

	*bucket_out = xstrndup(path, slash - path);
	*prefix_out = xstrdup(slash + 1);
}

struct odb_source_s3 *odb_source_s3_new(struct object_database *odb,
					const char *payload,
					bool local)
{
	struct strbuf path = STRBUF_INIT;
	struct odb_source_s3 *s3;

	CALLOC_ARRAY(s3, 1);
	s3->cache_dir = xstrfmt("%s/objects/s3", odb->repo->commondir);
	s3_parse_url(payload, &s3->endpoint_url, &s3->bucket, &s3->prefix, &s3->region);

	safe_create_dir(odb->repo, s3->cache_dir, 0);
	strbuf_addf(&path, "%s/packs", s3->cache_dir);
	safe_create_dir(odb->repo, path.buf, 0);
	strbuf_reset(&path);
	strbuf_addf(&path, "%s/manifests", s3->cache_dir);
	safe_create_dir(odb->repo, path.buf, 0);

	s3->access_key_id = xstrdup_or_null(getenv("S3_KEY_ID"));
	s3->secret_access_key = xstrdup_or_null(getenv("S3_KEY_SECRET"));
	if (!s3->access_key_id || !s3->secret_access_key)
		die("s3: no credentials found; set S3_KEY_ID and S3_KEY_SECRET");

	/*
	 * Create the embedded packed store and inhibit its automatic
	 * directory scan. We will populate it manually from the manifest.
	 */
	s3->packed = odb_source_packed_new(odb, s3->cache_dir, local);
	s3->packed->initialized = true;

	odb_source_init(&s3->base, odb, ODB_SOURCE_S3, s3->cache_dir, local);
	s3->base.free = odb_source_s3_free;
	s3->base.close = odb_source_s3_close;
	s3->base.reprepare = odb_source_s3_reprepare;
	s3->base.read_object_info = odb_source_s3_read_object_info;
	s3->base.read_object_stream = odb_source_s3_read_object_stream;
	s3->base.for_each_object = odb_source_s3_for_each_object;
	s3->base.count_objects = odb_source_s3_count_objects;
	s3->base.find_abbrev_len = odb_source_s3_find_abbrev_len;
	s3->base.freshen_object = odb_source_s3_freshen_object;
	s3->base.write_object = odb_source_s3_write_object;
	s3->base.write_object_stream = odb_source_s3_write_object_stream;
	s3->base.begin_transaction = odb_source_s3_begin_transaction;
	s3->base.read_alternates = odb_source_s3_read_alternates;
	s3->base.write_alternate = odb_source_s3_write_alternate;
	s3->base.get_packs = odb_source_s3_get_packs;

	strbuf_release(&path);
	return s3;
}
