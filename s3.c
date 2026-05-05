#include "git-compat-util.h"
#include "gettext.h"
#include "hash.h"
#include "hex.h"
#include "repository.h"
#include "strbuf.h"
#include "string-list.h"
#include "s3.h"
#include "tempfile.h"
#include "wrapper.h"

#include <curl/curl.h>
#include <curl/easy.h>

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
 * host           - authority of the endpoint URL, e.g. "s3.us-east-1.amazonaws.com"
 * path           - URI path component, e.g. "/pack/pack-abc.pack"
 * payload_sha256 - lowercase hex SHA-256 of the request body
 * content_length - body size in bytes; ignored for GET (pass 0)
 *
 * Returns a newly allocated curl_slist that the caller must free with
 * curl_slist_free_all().  Also allocates *auth_header_out which the caller
 * must free(); it is already appended to the returned slist.
 */
static struct curl_slist *sigv4_build_headers(struct s3_storage *storage,
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
	if (is_put)
		strbuf_addf(&canonical_hdrs,
			    "content-length:%" PRIdMAX "\n",
			    (intmax_t)content_length);
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
		strbuf_addf(&hdr, "Content-Length: %" PRIdMAX, (intmax_t)content_length);
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
const char *s3_key(const struct s3_storage *storage, struct strbuf *buf, const char *suffix)
{
	strbuf_reset(buf);
	strbuf_addf(buf, "%s/%s", storage->prefix, suffix);
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
static void s3_request_parts(const struct s3_storage *storage, const char *key,
			     char **host_out, char **path_out)
{
	const char *authority = strstr(storage->endpoint_url, "://");
	*host_out = xstrdup(authority ? authority + 3 : storage->endpoint_url);
	*path_out = xstrfmt("/%s/%s", storage->bucket, key);
}

static char *s3_url(const struct s3_storage *storage, const char *key)
{
	return xstrfmt("%s/%s/%s", storage->endpoint_url, storage->bucket, key);
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

static int s3_get_to_buf(struct s3_storage *storage, const char *key,
			 struct strbuf *out)
{
	char *url, *host, *path, *auth_hdr;
	struct curl_slist *hdrs;
	CURL *curl;
	int ret;

	s3_request_parts(storage, key, &host, &path);
	url = s3_url(storage, key);

	hdrs = sigv4_build_headers(storage, "GET", host, path, SHA256_EMPTY,
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

int s3_get_to_file(struct s3_storage *storage, const char *key,
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

	s3_request_parts(storage, key, &host, &path);
	url = s3_url(storage, key);

	hdrs = sigv4_build_headers(storage, "GET", host, path, SHA256_EMPTY,
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

int s3_put_from_buffer(struct s3_storage *storage, const char *key,
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

	s3_request_parts(storage, key, &host, &path);
	url = s3_url(storage, key);

	hdrs = sigv4_build_headers(storage, "PUT", host, path, payload_sha256,
				   (curl_off_t)data_len, &auth_hdr);

	curl = curl_easy_init();
	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
	curl_easy_setopt(curl, CURLOPT_READDATA, &ctx);
	curl_easy_setopt(curl, CURLOPT_READFUNCTION, s3_read_cb);
	curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)data_len);
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
int s3_put_from_file(struct s3_storage *storage, const char *key,
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

	s3_request_parts(storage, key, &host, &path);
	url = s3_url(storage, key);

	hdrs = sigv4_build_headers(storage, "PUT", host, path, payload_sha256,
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
static char *s3_resolve_manifest_version(struct s3_storage *storage)
{
	const char *pinned = getenv("GIT_S3_MANIFEST");
	struct strbuf buf = STRBUF_INIT, key = STRBUF_INIT;
	char *manifest = NULL;
	int ret;

	if (pinned)
		return xstrdup(pinned);

	ret = s3_get_to_buf(storage, s3_key(storage, &key, "manifest"), &buf);
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

/*
 * Write a new manifest version to S3 and advance the mutable pointer.
 *
 * The pack-hash list is serialised and content-addressed: the file is uploaded
 * to <prefix>/manifests/<sha256-of-content>, then the pointer at
 * <prefix>/manifest is updated to hold that hash.
 */
int s3_storage_update_manifest(struct s3_storage *storage,
			       const struct s3_manifest *manifest)
{
	struct strbuf content = STRBUF_INIT;
	struct strbuf path = STRBUF_INIT;
	struct strbuf key = STRBUF_INIT;
	char version_hex[GIT_SHA256_HEXSZ + 1];
	struct tempfile *tempfile = NULL;
	int ret;

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

	s3_key(storage, &key, path.buf + strlen(storage->cache_dir) + 1);
	ret = s3_put_from_file(storage, key.buf, path.buf);
	if (ret < 0)
		goto out;

	/*
	 * TODO: we should use locking semantics on the local filesystem and
	 * add a compare-and-swap on the object storage.
	 */
	ret = s3_put_from_buffer(storage, s3_key(storage, &key, "manifest"),
				 (unsigned char *)version_hex, strlen(version_hex));

	/* Update the cached manifest. */
	s3_manifest_release(&storage->manifest);
	s3_manifest_copy(manifest, &storage->manifest);

out:
	delete_tempfile(&tempfile);
	strbuf_release(&content);
	strbuf_release(&path);
	strbuf_release(&key);
	return ret;
}

const struct s3_manifest *s3_storage_get_manifest(struct s3_storage *storage)
{
	struct s3_manifest manifest = S3_MANIFEST_INIT;
	struct string_list lines = STRING_LIST_INIT_DUP;
	struct strbuf content = STRBUF_INIT;
	struct strbuf key = STRBUF_INIT;
	char *path = NULL, *version = NULL;
	int ret;

	if (storage->manifest_initialized)
		return &storage->manifest;

	version = s3_resolve_manifest_version(storage);
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
		if (errno != ENOENT)
			die_errno("s3: failed statting manifest '%s'", path);

		ret = s3_get_to_file(storage, s3_key(storage, &key, path + strlen(storage->cache_dir) + 1), path);
		if (ret < 0)
			die("s3-storage: failed fetching manifest");
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
	strbuf_release(&key);
	free(version);
	free(path);
	return &storage->manifest;
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
	storage->refcount++;
	storage->url = xstrdup(url);
	storage->endpoint_url = xstrndup(url, host_end - url);
	storage->region = region_from_host(after_scheme);
	storage->bucket = xstrndup(host_end + 1, slash - (host_end + 1));
	storage->prefix = xstrdup(slash + 1);
	storage->cache_dir = xstrfmt("%s/s3-cache", repo->commondir);
	storage->manifest = empty_manifest;

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

	s3_manifest_release(&storage->manifest);
	free(storage->endpoint_url);
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
	string_list_clear(&manifest->packs, 0);
}

void s3_manifest_copy(const struct s3_manifest *from,
		      struct s3_manifest *to)
{
	struct string_list_item *item;
	for_each_string_list_item(item, &from->packs)
		string_list_append(&to->packs, item->string);
}
