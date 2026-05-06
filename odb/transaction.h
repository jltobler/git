#ifndef ODB_TRANSACTION_H
#define ODB_TRANSACTION_H

#include "git-compat-util.h"
#include "gettext.h"
#include "odb.h"

/*
 * Options for `odb_transaction_write_pack()`.
 */
struct odb_transaction_write_pack_opts {
	const char *fsck_msg_types;
	const char *shallow_file;
	const char *error_msg;
	unsigned unpack_limit;
	off_t max_pack_size;
	int fsck_objects;
	int reject_thin;
	int err_fd;
	int quiet;

	/*
	 * To prevent races with concurrent repacks, the "files" backend creates
	 * a lockfile that remains after the ODB transaction is committed. This
	 * lockfile is expected to be removed only after the references are
	 * updated.
	 */
	struct tempfile *pack_lockfile;
};

/*
 * A transaction may be started for an object database prior to writing new
 * objects via odb_transaction_begin(). These objects are not committed until
 * odb_transaction_commit() is invoked. Only a single transaction may be pending
 * at a time.
 *
 * Each ODB source is expected to implement its own transaction handling.
 */
struct odb_transaction {
	/* The ODB source the transaction is opened against. */
	struct odb_source *source;

	/* The ODB source specific callback invoked to commit a transaction. */
	int (*commit)(struct odb_transaction *transaction);

	/*
	 * This callback is expected to write the given object stream into
	 * the ODB transaction. Note that for now, only blobs support streaming.
	 *
	 * The resulting object ID shall be written into the out pointer. The
	 * callback is expected to return 0 on success, a negative error code
	 * otherwise.
	 */
	int (*write_object_stream)(struct odb_transaction *transaction,
				   struct odb_write_stream *stream, size_t len,
				   struct object_id *oid);

	int (*write_pack)(struct odb_transaction *transaction, int pack_fd,
			  struct odb_transaction_write_pack_opts *opts);

	const char **(*env)(struct odb_transaction *transaction);
};

enum odb_transaction_flags {
	ODB_TRANSACTION_RECEIVE = (1 << 0),
};

/*
 * Starts an ODB transaction. Subsequent objects are written to the transaction
 * and not committed until odb_transaction_commit() is invoked on the
 * transaction. If the ODB already has a pending transaction, NULL is returned.
 */
int odb_transaction_begin(struct object_database *odb,
			  struct odb_transaction **out,
			  enum odb_transaction_flags flags);

static inline void odb_transaction_begin_or_die(struct object_database *odb,
						struct odb_transaction **out,
						enum odb_transaction_flags flags)
{
	if (odb_transaction_begin(odb, out, flags))
		die(_("failed to start ODB transaction"));
}

/*
 * Commits an ODB transaction making the written objects visible. If the
 * specified transaction is NULL, the function is a no-op.
 */
int odb_transaction_commit(struct odb_transaction *transaction);

/*
 * Writes the object in the provided stream into the transaction. The resulting
 * object ID is written into the out pointer. Returns 0 on success, a negative
 * error code otherwise.
 */
int odb_transaction_write_object_stream(struct odb_transaction *transaction,
					struct odb_write_stream *stream,
					size_t len, struct object_id *oid);

/*
 * Writes the objects contained in the provided packfile via fd into the
 * transaction. Returns 0 on success, a negative error code otherwise.
 */
int odb_transaction_write_pack(struct odb_transaction *transaction, int pack_fd,
			       struct odb_transaction_write_pack_opts *opts);

const char **odb_transaction_env(struct odb_transaction *transaction);

#endif
