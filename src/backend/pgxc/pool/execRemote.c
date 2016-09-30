/*-------------------------------------------------------------------------
 *
 * execRemote.c
 *
 *	  Functions to execute commands on remote Datanodes
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/pgxc/pool/execRemote.c
 *
 *-------------------------------------------------------------------------
 */

#include <time.h>
#include "postgres.h"
#include "access/twophase.h"
#include "access/gtm.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/relscan.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#include "executor/executor.h"
#include "gtm/gtm_c.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgxc/execRemote.h"
#include "tcop/tcopprot.h"
#include "executor/nodeSubplan.h"
#include "nodes/nodeFuncs.h"
#include "pgstat.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/var.h"
#include "pgxc/copyops.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolmgr.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/tuplesort.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "pgxc/xc_maintenance_mode.h"

/*
 * We do not want it too long, when query is terminating abnormally we just
 * want to read in already available data, if datanode connection will reach a
 * consistent state after that, we will go normal clean up procedure: send down
 * ABORT etc., if data node is not responding we will signal pooler to drop
 * the connection.
 * It is better to drop and recreate datanode connection then wait for several
 * seconds while it being cleaned up when, for example, cancelling query.
 */
#define END_QUERY_TIMEOUT	1000

typedef struct
{
	xact_callback function;
	void *fparams;
} abort_callback_type;

/*
 * Buffer size does not affect performance significantly, just do not allow
 * connection buffer grows infinitely
 */
#define COPY_BUFFER_SIZE 8192
#define PRIMARY_NODE_WRITEAHEAD 1024 * 1024

/*
 * Flag to track if a temporary object is accessed by the current transaction
 */
static bool temp_object_included = false;
static abort_callback_type dbcleanup_info = { NULL, NULL };

static int	pgxc_node_begin(int conn_count, PGXCNodeHandle ** connections,
				GlobalTransactionId gxid, bool need_tran_block,
				bool readOnly, char node_type);

static PGXCNodeAllHandles *get_exec_connections(RemoteQueryState *planstate,
					 ExecNodes *exec_nodes,
					 RemoteQueryExecType exec_type,
					 bool is_global_session);


static bool pgxc_start_command_on_connection(PGXCNodeHandle *connection,
					RemoteQueryState *remotestate, Snapshot snapshot);

static void pgxc_node_remote_count(int *dnCount, int dnNodeIds[],
		int *coordCount, int coordNodeIds[]);
static char *pgxc_node_remote_prepare(char *prepareGID, bool localNode);
static bool pgxc_node_remote_finish(char *prepareGID, bool commit,
						char *nodestring, GlobalTransactionId gxid,
						GlobalTransactionId prepare_gxid);
static void pgxc_node_remote_commit(void);
static void pgxc_node_remote_abort(void);
static void pgxc_connections_cleanup(ResponseCombiner *combiner);

static void pgxc_node_report_error(ResponseCombiner *combiner);

#define REMOVE_CURR_CONN(combiner) \
	if ((combiner)->current_conn < --((combiner)->conn_count)) \
	{ \
		(combiner)->connections[(combiner)->current_conn] = \
				(combiner)->connections[(combiner)->conn_count]; \
	} \
	else \
		(combiner)->current_conn = 0

#define MAX_STATEMENTS_PER_TRAN 10

/* Variables to collect statistics */
static int	total_transactions = 0;
static int	total_statements = 0;
static int	total_autocommit = 0;
static int	nonautocommit_2pc = 0;
static int	autocommit_2pc = 0;
static int	current_tran_statements = 0;
static int *statements_per_transaction = NULL;
static int *nodes_per_transaction = NULL;

/*
 * statistics collection: count a statement
 */
static void
stat_statement()
{
	total_statements++;
	current_tran_statements++;
}

/*
 * To collect statistics: count a transaction
 */
static void
stat_transaction(int node_count)
{
	total_transactions++;

	if (!statements_per_transaction)
	{
		statements_per_transaction = (int *) malloc((MAX_STATEMENTS_PER_TRAN + 1) * sizeof(int));
		memset(statements_per_transaction, 0, (MAX_STATEMENTS_PER_TRAN + 1) * sizeof(int));
	}
	if (current_tran_statements > MAX_STATEMENTS_PER_TRAN)
		statements_per_transaction[MAX_STATEMENTS_PER_TRAN]++;
	else
		statements_per_transaction[current_tran_statements]++;
	current_tran_statements = 0;
	if (node_count > 0 && node_count <= NumDataNodes)
	{
		if (!nodes_per_transaction)
		{
			nodes_per_transaction = (int *) malloc(NumDataNodes * sizeof(int));
			memset(nodes_per_transaction, 0, NumDataNodes * sizeof(int));
		}
		nodes_per_transaction[node_count - 1]++;
	}
}


/*
 * Output collected statistics to the log
 */
static void
stat_log()
{
	elog(DEBUG1, "Total Transactions: %d Total Statements: %d", total_transactions, total_statements);
	elog(DEBUG1, "Autocommit: %d 2PC for Autocommit: %d 2PC for non-Autocommit: %d",
		 total_autocommit, autocommit_2pc, nonautocommit_2pc);
	if (total_transactions)
	{
		if (statements_per_transaction)
		{
			int			i;

			for (i = 0; i < MAX_STATEMENTS_PER_TRAN; i++)
				elog(DEBUG1, "%d Statements per Transaction: %d (%d%%)",
					 i, statements_per_transaction[i], statements_per_transaction[i] * 100 / total_transactions);
		}
		elog(DEBUG1, "%d+ Statements per Transaction: %d (%d%%)",
			 MAX_STATEMENTS_PER_TRAN, statements_per_transaction[MAX_STATEMENTS_PER_TRAN], statements_per_transaction[MAX_STATEMENTS_PER_TRAN] * 100 / total_transactions);
		if (nodes_per_transaction)
		{
			int			i;

			for (i = 0; i < NumDataNodes; i++)
				elog(DEBUG1, "%d Nodes per Transaction: %d (%d%%)",
					 i + 1, nodes_per_transaction[i], nodes_per_transaction[i] * 100 / total_transactions);
		}
	}
}


/*
 * Create a structure to store parameters needed to combine responses from
 * multiple connections as well as state information
 */
void
InitResponseCombiner(ResponseCombiner *combiner, int node_count,
					   CombineType combine_type)
{
	combiner->node_count = node_count;
	combiner->connections = NULL;
	combiner->conn_count = 0;
	combiner->combine_type = combine_type;
	combiner->command_complete_count = 0;
	combiner->request_type = REQUEST_TYPE_NOT_DEFINED;
	combiner->description_count = 0;
	combiner->copy_in_count = 0;
	combiner->copy_out_count = 0;
	combiner->copy_file = NULL;
	combiner->errorMessage = NULL;
	combiner->errorDetail = NULL;
	combiner->errorHint = NULL;
	combiner->tuple_desc = NULL;
	combiner->probing_primary = false;
	combiner->returning_node = InvalidOid;
	combiner->currentRow = NULL;
	combiner->rowBuffer = NIL;
	combiner->tapenodes = NULL;
	combiner->merge_sort = false;
	combiner->extended_query = false;
	combiner->tapemarks = NULL;
	combiner->tuplesortstate = NULL;
	combiner->cursor = NULL;
	combiner->update_cursor = NULL;
	combiner->cursor_count = 0;
	combiner->cursor_connections = NULL;
	combiner->remoteCopyType = REMOTE_COPY_NONE;
}


/*
 * Parse out row count from the command status response and convert it to integer
 */
static int
parse_row_count(const char *message, size_t len, uint64 *rowcount)
{
	int			digits = 0;
	int			pos;

	*rowcount = 0;
	/* skip \0 string terminator */
	for (pos = 0; pos < len - 1; pos++)
	{
		if (message[pos] >= '0' && message[pos] <= '9')
		{
			*rowcount = *rowcount * 10 + message[pos] - '0';
			digits++;
		}
		else
		{
			*rowcount = 0;
			digits = 0;
		}
	}
	return digits;
}

/*
 * Convert RowDescription message to a TupleDesc
 */
static TupleDesc
create_tuple_desc(char *msg_body, size_t len)
{
	TupleDesc 	result;
	int 		i, nattr;
	uint16		n16;

	/* get number of attributes */
	memcpy(&n16, msg_body, 2);
	nattr = ntohs(n16);
	msg_body += 2;

	result = CreateTemplateTupleDesc(nattr, false);

	/* decode attributes */
	for (i = 1; i <= nattr; i++)
	{
		AttrNumber	attnum;
		char		*attname;
		char		*typname;
		Oid 		oidtypeid;
		int32 		typemode, typmod;

		attnum = (AttrNumber) i;

		/* attribute name */
		attname = msg_body;
		msg_body += strlen(attname) + 1;

		/* type name */
		typname = msg_body;
		msg_body += strlen(typname) + 1;

		/* table OID, ignored */
		msg_body += 4;

		/* column no, ignored */
		msg_body += 2;

		/* data type OID, ignored */
		msg_body += 4;

		/* type len, ignored */
		msg_body += 2;

		/* type mod */
		memcpy(&typemode, msg_body, 4);
		typmod = ntohl(typemode);
		msg_body += 4;

		/* PGXCTODO text/binary flag? */
		msg_body += 2;

		/* Get the OID type and mode type from typename */
		parseTypeString(typname, &oidtypeid, NULL, false);

		TupleDescInitEntry(result, attnum, attname, oidtypeid, typmod, 0);
	}
	return result;
}

/*
 * Handle CopyOutCommandComplete ('c') message from a Datanode connection
 */
static void
HandleCopyOutComplete(ResponseCombiner *combiner)
{
	if (combiner->request_type == REQUEST_TYPE_ERROR)
		return;
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_OUT;
	if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'c' message, current request type %d", combiner->request_type)));
	/* Just do nothing, close message is managed by the Coordinator */
	combiner->copy_out_count++;
}

/*
 * Handle CommandComplete ('C') message from a Datanode connection
 */
static void
HandleCommandComplete(ResponseCombiner *combiner, char *msg_body, size_t len, PGXCNodeHandle *conn)
{
	int 			digits = 0;
	EState		   *estate = combiner->ss.ps.state;

	/*
	 * If we did not receive description we are having rowcount or OK response
	 */
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COMMAND;
	/* Extract rowcount */
	if (combiner->combine_type != COMBINE_TYPE_NONE && estate)
	{
		uint64	rowcount;
		digits = parse_row_count(msg_body, len, &rowcount);
		if (digits > 0)
		{
			/* Replicated write, make sure they are the same */
			if (combiner->combine_type == COMBINE_TYPE_SAME)
			{
				if (combiner->command_complete_count)
				{
					/*
					 * Replicated command may succeed on on node and fail on
					 * another. The example is if distributed table referenced
					 * by a foreign key constraint defined on a partitioned
					 * table. If command deletes rows from the replicated table
					 * they may be referenced on one Datanode but not on other.
					 * So, replicated command on each Datanode either affects
					 * proper number of rows, or returns error. Here if
					 * combiner got an error already, we allow to report it,
					 * not the scaring data corruption message.
					 */
					if (combiner->errorMessage == NULL && rowcount != estate->es_processed)
						/* There is a consistency issue in the database with the replicated table */
						ereport(ERROR,
								(errcode(ERRCODE_DATA_CORRUPTED),
								 errmsg("Write to replicated table returned different results from the Datanodes")));
				}
				else
					/* first result */
					estate->es_processed = rowcount;
			}
			else
				estate->es_processed += rowcount;
		}
		else
			combiner->combine_type = COMBINE_TYPE_NONE;
	}

	/* If response checking is enable only then do further processing */
	if (conn->ck_resp_rollback)
	{
		if (strcmp(msg_body, "ROLLBACK") == 0)
		{
			/*
			 * Subsequent clean up routine will be checking this flag
			 * to determine nodes where to send ROLLBACK PREPARED.
			 * On current node PREPARE has failed and the two-phase record
			 * does not exist, so clean this flag as if PREPARE was not sent
			 * to that node and avoid erroneous command.
			 */
			conn->ck_resp_rollback = false;
			/*
			 * Set the error, if none, to force throwing.
			 * If there is error already, it will be thrown anyway, do not add
			 * this potentially confusing message
			 */
			if (combiner->errorMessage == NULL)
			{
				MemoryContext oldcontext = MemoryContextSwitchTo(ErrorContext);
				combiner->errorMessage =
								pstrdup("unexpected ROLLBACK from remote node");
				MemoryContextSwitchTo(oldcontext);
				/*
				 * ERRMSG_PRODUCER_ERROR
				 * Messages with this code are replaced by others, if they are
				 * received, so if node will send relevant error message that
				 * one will be replaced.
				 */
				combiner->errorCode[0] = 'X';
				combiner->errorCode[1] = 'X';
				combiner->errorCode[2] = '0';
				combiner->errorCode[3] = '1';
				combiner->errorCode[4] = '0';
			}
		}
	}
	combiner->command_complete_count++;
}

/*
 * Handle RowDescription ('T') message from a Datanode connection
 */
static bool
HandleRowDescription(ResponseCombiner *combiner, char *msg_body, size_t len)
{
	if (combiner->request_type == REQUEST_TYPE_ERROR)
		return false;
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_QUERY;
	if (combiner->request_type != REQUEST_TYPE_QUERY)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'T' message, current request type %d", combiner->request_type)));
	}
	/* Increment counter and check if it was first */
	if (combiner->description_count++ == 0)
	{
		combiner->tuple_desc = create_tuple_desc(msg_body, len);
		return true;
	}
	return false;
}


/*
 * Handle CopyInResponse ('G') message from a Datanode connection
 */
static void
HandleCopyIn(ResponseCombiner *combiner)
{
	if (combiner->request_type == REQUEST_TYPE_ERROR)
		return;
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_IN;
	if (combiner->request_type != REQUEST_TYPE_COPY_IN)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'G' message, current request type %d", combiner->request_type)));
	}
	/*
	 * The normal PG code will output an G message when it runs in the
	 * Coordinator, so do not proxy message here, just count it.
	 */
	combiner->copy_in_count++;
}

/*
 * Handle CopyOutResponse ('H') message from a Datanode connection
 */
static void
HandleCopyOut(ResponseCombiner *combiner)
{
	if (combiner->request_type == REQUEST_TYPE_ERROR)
		return;
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_OUT;
	if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'H' message, current request type %d", combiner->request_type)));
	}
	/*
	 * The normal PG code will output an H message when it runs in the
	 * Coordinator, so do not proxy message here, just count it.
	 */
	combiner->copy_out_count++;
}

/*
 * Handle CopyOutDataRow ('d') message from a Datanode connection
 */
static void
HandleCopyDataRow(ResponseCombiner *combiner, char *msg_body, size_t len)
{
	if (combiner->request_type == REQUEST_TYPE_ERROR)
		return;
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		combiner->request_type = REQUEST_TYPE_COPY_OUT;

	/* Inconsistent responses */
	if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the Datanodes for 'd' message, current request type %d", combiner->request_type)));

	/* count the row */
	combiner->processed++;

	/* Output remote COPY operation to correct location */
	switch (combiner->remoteCopyType)
	{
		case REMOTE_COPY_FILE:
			/* Write data directly to file */
			fwrite(msg_body, 1, len, combiner->copy_file);
			break;
		case REMOTE_COPY_STDOUT:
			/* Send back data to client */
			pq_putmessage('d', msg_body, len);
			break;
		case REMOTE_COPY_TUPLESTORE:
			/*
			 * Do not store trailing \n character.
			 * When tuplestore data are loaded to a table it automatically
			 * inserts line ends.
			 */
			tuplestore_putmessage(combiner->tuplestorestate, len-1, msg_body);
			break;
		case REMOTE_COPY_NONE:
		default:
			Assert(0); /* Should not happen */
	}
}

/*
 * Handle DataRow ('D') message from a Datanode connection
 * The function returns true if data row is accepted and successfully stored
 * within the combiner.
 */
static bool
HandleDataRow(ResponseCombiner *combiner, char *msg_body, size_t len, Oid node)
{
	/* We expect previous message is consumed */
	Assert(combiner->currentRow == NULL);

	if (combiner->request_type == REQUEST_TYPE_ERROR)
		return false;

	if (combiner->request_type != REQUEST_TYPE_QUERY)
	{
		/* Inconsistent responses */
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes for 'D' message, current request type %d", combiner->request_type)));
	}

	/*
	 * If we got an error already ignore incoming data rows from other nodes
	 * Still we want to continue reading until get CommandComplete
	 */
	if (combiner->errorMessage)
		return false;

	/*
	 * Replicated INSERT/UPDATE/DELETE with RETURNING: receive only tuples
	 * from one node, skip others as duplicates
	 */
	if (combiner->combine_type == COMBINE_TYPE_SAME)
	{
		/* Do not return rows when probing primary, instead return when doing
		 * first normal node. Just save some CPU and traffic in case if
		 * probing fails.
		 */
		if (combiner->probing_primary)
			return false;
		if (OidIsValid(combiner->returning_node))
		{
			if (combiner->returning_node != node)
				return false;
		}
		else
			combiner->returning_node = node;
	}

	/*
	 * We are copying message because it points into connection buffer, and
	 * will be overwritten on next socket read
	 */
	combiner->currentRow = (RemoteDataRow) palloc(sizeof(RemoteDataRowData) + len);
	memcpy(combiner->currentRow->msg, msg_body, len);
	combiner->currentRow->msglen = len;
	combiner->currentRow->msgnode = node;

	return true;
}

/*
 * Handle ErrorResponse ('E') message from a Datanode connection
 */
static void
HandleError(ResponseCombiner *combiner, char *msg_body, size_t len, PGXCNodeHandle *conn)
{
	/* parse error message */
	char *code = NULL;
	char *message = NULL;
	char *detail = NULL;
	char *hint = NULL;
	int   offset = 0;

	/*
	 * Scan until point to terminating \0
	 */
	while (offset + 1 < len)
	{
		/* pointer to the field message */
		char *str = msg_body + offset + 1;

		switch (msg_body[offset])
		{
			case 'C':	/* code */
				code = str;
				break;
			case 'M':	/* message */
				message = str;
				break;
			case 'D':	/* details */
				detail = str;
				break;

			case 'H':	/* hint */
				hint = str;
				break;

			/* Fields not yet in use */
			case 'S':	/* severity */
			case 'R':	/* routine */
			case 'P':	/* position string */
			case 'p':	/* position int */
			case 'q':	/* int query */
			case 'W':	/* where */
			case 'F':	/* file */
			case 'L':	/* line */
			default:
				break;
		}

		/* code, message and \0 */
		offset += strlen(str) + 2;
	}

	/*
	 * We may have special handling for some errors, default handling is to
	 * throw out error with the same message. We can not ereport immediately
	 * because we should read from this and other connections until
	 * ReadyForQuery is received, so we just store the error message.
	 * If multiple connections return errors only first one is reported.
	 *
	 * The producer error may be hiding primary error, so if previously received
	 * error is a producer error allow it to be overwritten.
	 */
	if (combiner->errorMessage == NULL ||
			MAKE_SQLSTATE(combiner->errorCode[0], combiner->errorCode[1],
						  combiner->errorCode[2], combiner->errorCode[3],
						  combiner->errorCode[4]) == ERRCODE_PRODUCER_ERROR)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(ErrorContext);
		combiner->errorMessage = pstrdup(message);
		/* Error Code is exactly 5 significant bytes */
		if (code)
			memcpy(combiner->errorCode, code, 5);
		if (detail)
			combiner->errorDetail = pstrdup(detail);
		if (hint)
			combiner->errorHint = pstrdup(hint);
		MemoryContextSwitchTo(oldcontext);
	}

	/*
	 * If the PREPARE TRANSACTION command fails for whatever reason, we don't
	 * want to send down ROLLBACK PREPARED to this node. Otherwise, it may end
	 * up rolling back an unrelated prepared transaction with the same GID as
	 * used by this transaction
	 */
	if (conn->ck_resp_rollback)
		conn->ck_resp_rollback = false;

	/*
	 * If Datanode have sent ErrorResponse it will never send CommandComplete.
	 * Increment the counter to prevent endless waiting for it.
	 */
	combiner->command_complete_count++;
}

/*
 * HandleCmdComplete -
 *	combine deparsed sql statements execution results
 *
 * Input parameters:
 *	commandType is dml command type
 *	combineTag is used to combine the completion result
 *	msg_body is execution result needed to combine
 *	len is msg_body size
 */
void
HandleCmdComplete(CmdType commandType, CombineTag *combine,
						const char *msg_body, size_t len)
{
	int	digits = 0;
	uint64	originrowcount = 0;
	uint64	rowcount = 0;
	uint64	total = 0;

	if (msg_body == NULL)
		return;

	/* if there's nothing in combine, just copy the msg_body */
	if (strlen(combine->data) == 0)
	{
		strcpy(combine->data, msg_body);
		combine->cmdType = commandType;
		return;
	}
	else
	{
		/* commandType is conflict */
		if (combine->cmdType != commandType)
			return;

		/* get the processed row number from msg_body */
		digits = parse_row_count(msg_body, len + 1, &rowcount);
		elog(DEBUG1, "digits is %d\n", digits);
		Assert(digits >= 0);

		/* no need to combine */
		if (digits == 0)
			return;

		/* combine the processed row number */
		parse_row_count(combine->data, strlen(combine->data) + 1, &originrowcount);
		elog(DEBUG1, "originrowcount is %lu, rowcount is %lu\n", originrowcount, rowcount);
		total = originrowcount + rowcount;

	}

	/* output command completion tag */
	switch (commandType)
	{
		case CMD_SELECT:
			strcpy(combine->data, "SELECT");
			break;
		case CMD_INSERT:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
			   "INSERT %u %lu", 0, total);
			break;
		case CMD_UPDATE:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
					 "UPDATE %lu", total);
			break;
		case CMD_DELETE:
			snprintf(combine->data, COMPLETION_TAG_BUFSIZE,
					 "DELETE %lu", total);
			break;
		default:
			strcpy(combine->data, "");
			break;
	}

}

/*
 * HandleDatanodeCommandId ('M') message from a Datanode connection
 */
static void
HandleDatanodeCommandId(ResponseCombiner *combiner, char *msg_body, size_t len)
{
	uint32		n32;
	CommandId	cid;

	Assert(msg_body != NULL);
	Assert(len >= 2);

	/* Get the command Id */
	memcpy(&n32, &msg_body[0], 4);
	cid = ntohl(n32);

	/* If received command Id is higher than current one, set it to a new value */
	if (cid > GetReceivedCommandId())
		SetReceivedCommandId(cid);
}

/*
 * Record waited-for XIDs received from the remote nodes into the transaction
 * state
 */
static void
HandleWaitXids(char *msg_body, size_t len)
{
	int xid_count;
	uint32		n32;
	int cur;
	int i;

	/* Get the xid count */
	xid_count = len / sizeof (TransactionId);

	cur = 0;
	for (i = 0; i < xid_count; i++)
	{
		Assert(cur < len);
		memcpy(&n32, &msg_body[cur], sizeof (TransactionId));
		cur = cur + sizeof (TransactionId);
		TransactionRecordXidWait(ntohl(n32));
	}
}

static void
HandleGlobalTransactionId(char *msg_body, size_t len)
{
	GlobalTransactionId xid;

	Assert(len == sizeof (GlobalTransactionId));
	memcpy(&xid, &msg_body[0], sizeof (GlobalTransactionId));

	SetTopTransactionId(xid);
}

/*
 * Examine the specified combiner state and determine if command was completed
 * successfully
 */
static bool
validate_combiner(ResponseCombiner *combiner)
{
	/* There was error message while combining */
	if (combiner->errorMessage)
		return false;
	/* Check if state is defined */
	if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
		return false;

	/* Check all nodes completed */
	if ((combiner->request_type == REQUEST_TYPE_COMMAND
	        || combiner->request_type == REQUEST_TYPE_QUERY)
	        && combiner->command_complete_count != combiner->node_count)
		return false;

	/* Check count of description responses */
	if (combiner->request_type == REQUEST_TYPE_QUERY
	        && combiner->description_count != combiner->node_count)
		return false;

	/* Check count of copy-in responses */
	if (combiner->request_type == REQUEST_TYPE_COPY_IN
	        && combiner->copy_in_count != combiner->node_count)
		return false;

	/* Check count of copy-out responses */
	if (combiner->request_type == REQUEST_TYPE_COPY_OUT
	        && combiner->copy_out_count != combiner->node_count)
		return false;

	/* Add other checks here as needed */

	/* All is good if we are here */
	return true;
}

/*
 * Close combiner and free allocated memory, if it is not needed
 */
void
CloseCombiner(ResponseCombiner *combiner)
{
	if (combiner->connections)
		pfree(combiner->connections);
	if (combiner->tuple_desc)
		FreeTupleDesc(combiner->tuple_desc);
	if (combiner->errorMessage)
		pfree(combiner->errorMessage);
	if (combiner->errorDetail)
		pfree(combiner->errorDetail);
	if (combiner->errorHint)
		pfree(combiner->errorHint);
	if (combiner->cursor_connections)
		pfree(combiner->cursor_connections);
	if (combiner->tapenodes)
		pfree(combiner->tapenodes);
	if (combiner->tapemarks)
		pfree(combiner->tapemarks);
}

/*
 * Validate combiner and release storage freeing allocated memory
 */
static bool
ValidateAndCloseCombiner(ResponseCombiner *combiner)
{
	bool		valid = validate_combiner(combiner);

	CloseCombiner(combiner);

	return valid;
}

/*
 * It is possible if multiple steps share the same Datanode connection, when
 * executor is running multi-step query or client is running multiple queries
 * using Extended Query Protocol. After returning next tuple ExecRemoteQuery
 * function passes execution control to the executor and then it can be given
 * to the same RemoteQuery or to different one. It is possible that before
 * returning a tuple the function do not read all Datanode responses. In this
 * case pending responses should be read in context of original RemoteQueryState
 * till ReadyForQuery message and data rows should be stored (buffered) to be
 * available when fetch from that RemoteQueryState is requested again.
 * BufferConnection function does the job.
 * If a RemoteQuery is going to use connection it should check connection state.
 * DN_CONNECTION_STATE_QUERY indicates query has data to read and combiner
 * points to the original RemoteQueryState. If combiner differs from "this" the
 * connection should be buffered.
 */
void
BufferConnection(PGXCNodeHandle *conn)
{
	ResponseCombiner *combiner = conn->combiner;
	MemoryContext oldcontext;

	if (combiner == NULL || conn->state != DN_CONNECTION_STATE_QUERY)
		return;

	elog(DEBUG2, "Buffer connection %u to step %s", conn->nodeoid, combiner->cursor);

	/*
	 * When BufferConnection is invoked CurrentContext is related to other
	 * portal, which is trying to control the connection.
	 * TODO See if we can find better context to switch to
	 */
	oldcontext = MemoryContextSwitchTo(combiner->ss.ps.ps_ResultTupleSlot->tts_mcxt);

	/* Verify the connection is in use by the combiner */
	combiner->current_conn = 0;
	while (combiner->current_conn < combiner->conn_count)
	{
		if (combiner->connections[combiner->current_conn] == conn)
			break;
		combiner->current_conn++;
	}
	Assert(combiner->current_conn < combiner->conn_count);

	if (combiner->tapemarks == NULL)
		combiner->tapemarks = (ListCell**) palloc0(combiner->conn_count * sizeof(ListCell*));

	/*
	 * If current bookmark for the current tape is not set it means either
	 * first row in the buffer is from the current tape or no rows from
	 * the tape in the buffer, so if first row is not from current
	 * connection bookmark the last cell in the list.
	 */
	if (combiner->tapemarks[combiner->current_conn] == NULL &&
			list_length(combiner->rowBuffer) > 0)
	{
		RemoteDataRow dataRow = (RemoteDataRow) linitial(combiner->rowBuffer);
		if (dataRow->msgnode != conn->nodeoid)
			combiner->tapemarks[combiner->current_conn] = list_tail(combiner->rowBuffer);
	}

	/*
	 * Buffer data rows until data node return number of rows specified by the
	 * fetch_size parameter of last Execute message (PortalSuspended message)
	 * or end of result set is reached (CommandComplete message)
	 */
	while (true)
	{
		int res;

		/* Move to buffer currentRow (received from the data node) */
		if (combiner->currentRow)
		{
			combiner->rowBuffer = lappend(combiner->rowBuffer,
										  combiner->currentRow);
			combiner->currentRow = NULL;
		}

		res = handle_response(conn, combiner);
		/*
		 * If response message is a DataRow it will be handled on the next
		 * iteration.
		 * PortalSuspended will cause connection state change and break the loop
		 * The same is for CommandComplete, but we need additional handling -
		 * remove connection from the list of active connections.
		 * We may need to add handling error response
		 */

		/* Most often result check first */
		if (res == RESPONSE_DATAROW)
		{
			/*
			 * The row is in the combiner->currentRow, on next iteration it will
			 * be moved to the buffer
			 */
			continue;
		}

		/* incomplete message, read more */
		if (res == RESPONSE_EOF)
		{
			if (pgxc_node_receive(1, &conn, NULL))
			{
				PGXCNodeSetConnectionState(conn,
						DN_CONNECTION_STATE_ERROR_FATAL);
				add_error_message(conn, "Failed to fetch from data node");
			}
		}

		/*
		 * End of result set is reached, so either set the pointer to the
		 * connection to NULL (combiner with sort) or remove it from the list
		 * (combiner without sort)
		 */
		else if (res == RESPONSE_COMPLETE)
		{
			/*
			 * If combiner is doing merge sort we should set reference to the
			 * current connection to NULL in the array, indicating the end
			 * of the tape is reached. FetchTuple will try to access the buffer
			 * first anyway.
			 * Since we remove that reference we can not determine what node
			 * number was this connection, but we need this info to find proper
			 * tuple in the buffer if we are doing merge sort. So store node
			 * number in special array.
			 * NB: We can not test if combiner->tuplesortstate is set here:
			 * connection may require buffering inside tuplesort_begin_merge
			 * - while pre-read rows from the tapes, one of the tapes may be
			 * the local connection with RemoteSubplan in the tree. The
			 * combiner->tuplesortstate is set only after tuplesort_begin_merge
			 * returns.
			 */
			if (combiner->merge_sort)
			{
				combiner->connections[combiner->current_conn] = NULL;
				if (combiner->tapenodes == NULL)
					combiner->tapenodes = (Oid *)
							palloc0(combiner->conn_count * sizeof(Oid));
				combiner->tapenodes[combiner->current_conn] = conn->nodeoid;
			}
			else
			{
				/* Remove current connection, move last in-place, adjust current_conn */
				if (combiner->current_conn < --combiner->conn_count)
					combiner->connections[combiner->current_conn] = combiner->connections[combiner->conn_count];
				else
					combiner->current_conn = 0;
			}
			/*
			 * If combiner runs Simple Query Protocol we need to read in
			 * ReadyForQuery. In case of Extended Query Protocol it is not
			 * sent and we should quit.
			 */
			if (combiner->extended_query)
				break;
		}
		else if (res == RESPONSE_ERROR)
		{
			if (combiner->extended_query)
			{
				/*
				 * Need to sync connection to enable receiving commands
				 * by the datanode
				 */
				if (pgxc_node_send_sync(conn) != 0)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to sync msg to node %u", conn->nodeoid)));
				}
			}
		}
		else if (res == RESPONSE_SUSPENDED || res == RESPONSE_READY)
		{
			/* Now it is OK to quit */
			break;
		}
	}
	Assert(conn->state != DN_CONNECTION_STATE_QUERY);
	MemoryContextSwitchTo(oldcontext);
	conn->combiner = NULL;
}

/*
 * copy the datarow from combiner to the given slot, in the slot's memory
 * context
 */
static void
CopyDataRowTupleToSlot(ResponseCombiner *combiner, TupleTableSlot *slot)
{
	RemoteDataRow 	datarow;
	MemoryContext	oldcontext;
	oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);
	datarow = (RemoteDataRow) palloc(sizeof(RemoteDataRowData) + combiner->currentRow->msglen);
	datarow->msgnode = combiner->currentRow->msgnode;
	datarow->msglen = combiner->currentRow->msglen;
	memcpy(datarow->msg, combiner->currentRow->msg, datarow->msglen);
	ExecStoreDataRowTuple(datarow, slot, true);
	pfree(combiner->currentRow);
	combiner->currentRow = NULL;
	MemoryContextSwitchTo(oldcontext);
}


/*
 * FetchTuple
 *
		Get next tuple from one of the datanode connections.
 * The connections should be in combiner->connections, if "local" dummy
 * connection presents it should be the last active connection in the array.
 *      If combiner is set up to perform merge sort function returns tuple from
 * connection defined by combiner->current_conn, or NULL slot if no more tuple
 * are available from the connection. Otherwise it returns tuple from any
 * connection or NULL slot if no more available connections.
 * 		Function looks into combiner->rowBuffer before accessing connection
 * and return a tuple from there if found.
 * 		Function may wait while more data arrive from the data nodes. If there
 * is a locally executed subplan function advance it and buffer resulting rows
 * instead of waiting.
 */
TupleTableSlot *
FetchTuple(ResponseCombiner *combiner)
{
	PGXCNodeHandle *conn;
	TupleTableSlot *slot;
	Oid 			nodeOid = -1;

	/*
	 * Case if we run local subplan.
	 * We do not have remote connections, so just get local tuple and return it
	 */
	if (outerPlanState(combiner))
	{
		RemoteSubplanState *planstate = (RemoteSubplanState *) combiner;
		RemoteSubplan *plan = (RemoteSubplan *) combiner->ss.ps.plan;
		/* Advance subplan in a loop until we have something to return */
		for (;;)
		{
			Datum 	value = (Datum) 0;
			bool 	isnull = false;
			int 	numnodes;
			int		i;

			slot = ExecProcNode(outerPlanState(combiner));
			/* If locator is not defined deliver all the results */
			if (planstate->locator == NULL)
				return slot;

			/*
			 * If NULL tuple is returned we done with the subplan, finish it up and
			 * return NULL
			 */
			if (TupIsNull(slot))
				return NULL;

			/* Get partitioning value if defined */
			if (plan->distributionKey != InvalidAttrNumber)
				value = slot_getattr(slot, plan->distributionKey, &isnull);

			/* Determine target nodes */
			numnodes = GET_NODES(planstate->locator, value, isnull, NULL);
			for (i = 0; i < numnodes; i++)
			{
				/* Deliver the node */
				if (planstate->dest_nodes[i] == PGXCNodeId-1)
					return slot;
			}
		}
	}

	/*
	 * Get current connection
	 */
	if (combiner->conn_count > combiner->current_conn)
		conn = combiner->connections[combiner->current_conn];
	else
		conn = NULL;

	/*
	 * If doing merge sort determine the node number.
	 * It may be needed to get buffered row.
	 */
	if (combiner->merge_sort)
	{
		Assert(conn || combiner->tapenodes);
		nodeOid = conn ? conn->nodeoid :
						 combiner->tapenodes[combiner->current_conn];
		Assert(OidIsValid(nodeOid));
	}

	/*
	 * First look into the row buffer.
	 * When we are performing merge sort we need to get from the buffer record
	 * from the connection marked as "current". Otherwise get first.
	 */
	if (list_length(combiner->rowBuffer) > 0)
	{
		RemoteDataRow dataRow;

		Assert(combiner->currentRow == NULL);

		if (combiner->merge_sort)
		{
			ListCell *lc;
			ListCell *prev;

			elog(DEBUG1, "Getting buffered tuple from node %x", nodeOid);

			prev = combiner->tapemarks[combiner->current_conn];
			if (prev)
			{
				/*
				 * Start looking through the list from the bookmark.
				 * Probably the first cell we check contains row from the needed
				 * node. Otherwise continue scanning until we encounter one,
				 * advancing prev pointer as well.
				 */
				while((lc = lnext(prev)) != NULL)
				{
					dataRow = (RemoteDataRow) lfirst(lc);
					if (dataRow->msgnode == nodeOid)
					{
						combiner->currentRow = dataRow;
						break;
					}
					prev = lc;
				}
			}
			else
			{
				/*
				 * Either needed row is the first in the buffer or no such row
				 */
				lc = list_head(combiner->rowBuffer);
				dataRow = (RemoteDataRow) lfirst(lc);
				if (dataRow->msgnode == nodeOid)
					combiner->currentRow = dataRow;
				else
					lc = NULL;
			}
			if (lc)
			{
				/*
				 * Delete cell from the buffer. Before we delete we must check
				 * the bookmarks, if the cell is a bookmark for any tape.
				 * If it is the case we are deleting last row of the current
				 * block from the current tape. That tape should have bookmark
				 * like current, and current bookmark will be advanced when we
				 * read the tape once again.
				 */
				int i;
				for (i = 0; i < combiner->conn_count; i++)
				{
					if (combiner->tapemarks[i] == lc)
						combiner->tapemarks[i] = prev;
				}
				elog(DEBUG1, "Found buffered tuple from node %x", nodeOid);
				combiner->rowBuffer = list_delete_cell(combiner->rowBuffer,
													   lc, prev);
			}
			elog(DEBUG1, "Update tapemark");
			combiner->tapemarks[combiner->current_conn] = prev;
		}
		else
		{
			dataRow = (RemoteDataRow) linitial(combiner->rowBuffer);
			combiner->currentRow = dataRow;
			combiner->rowBuffer = list_delete_first(combiner->rowBuffer);
		}
	}

	/* If we have node message in the currentRow slot, and it is from a proper
	 * node, consume it.  */
	if (combiner->currentRow)
	{
		Assert(!combiner->merge_sort ||
			   combiner->currentRow->msgnode == nodeOid);
		slot = combiner->ss.ps.ps_ResultTupleSlot;
		CopyDataRowTupleToSlot(combiner, slot);
		return slot;
	}

	while (conn)
	{
		int res;

		/* Going to use a connection, buffer it if needed */
		CHECK_OWNERSHIP(conn, combiner);

		/*
		 * If current connection is idle it means portal on the data node is
		 * suspended. Request more and try to get it
		 */
		if (combiner->extended_query &&
				conn->state == DN_CONNECTION_STATE_IDLE)
		{
			/*
			 * We do not allow to suspend if querying primary node, so that
			 * only may mean the current node is secondary and subplan was not
			 * executed there yet. Return and go on with second phase.
			 */
			if (combiner->probing_primary)
			{
				return NULL;
			}

			if (pgxc_node_send_execute(conn, combiner->cursor, 1000) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send execute cursor '%s' to node %u", combiner->cursor, conn->nodeoid)));
			}

			if (pgxc_node_send_flush(conn) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed flush cursor '%s' node %u", combiner->cursor, conn->nodeoid)));
			}

			if (pgxc_node_receive(1, &conn, NULL))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed receive data from node %u cursor '%s'", conn->nodeoid, combiner->cursor)));
			}
		}

		/* read messages */
		res = handle_response(conn, combiner);
		if (res == RESPONSE_DATAROW)
		{
			slot = combiner->ss.ps.ps_ResultTupleSlot;
			CopyDataRowTupleToSlot(combiner, slot);
			return slot;
		}
		else if (res == RESPONSE_EOF)
		{
			/* incomplete message, read more */
			if (pgxc_node_receive(1, &conn, NULL))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to receive more data from data node %u", conn->nodeoid)));
			continue;
		}
		else if (res == RESPONSE_SUSPENDED)
		{
			/*
			 * If we are doing merge sort or probing primary node we should
			 * remain on the same node, so query next portion immediately.
			 * Otherwise leave node suspended and fetch lazily.
			 */
			if (combiner->merge_sort || combiner->probing_primary)
			{
				if (pgxc_node_send_execute(conn, combiner->cursor, 1000) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to send execute cursor '%s' to node %u", combiner->cursor, conn->nodeoid)));
				if (pgxc_node_send_flush(conn) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed flush cursor '%s' node %u", combiner->cursor, conn->nodeoid)));
				if (pgxc_node_receive(1, &conn, NULL))
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed receive node from node %u cursor '%s'", conn->nodeoid, combiner->cursor)));
				continue;
			}

			/*
			 * Tell the node to fetch data in background, next loop when we 
			 * pgxc_node_receive, data is already there, so we can run faster
			 * */
			if (pgxc_node_send_execute(conn, combiner->cursor, 1000) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send execute cursor '%s' to node %u", combiner->cursor, conn->nodeoid)));
			}

			if (pgxc_node_send_flush(conn) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed flush cursor '%s' node %u", combiner->cursor, conn->nodeoid)));
			}

			if (++combiner->current_conn >= combiner->conn_count)
				combiner->current_conn = 0;
			conn = combiner->connections[combiner->current_conn];
		}
		else if (res == RESPONSE_COMPLETE)
		{
			/*
			 * In case of Simple Query Protocol we should receive ReadyForQuery
			 * before removing connection from the list. In case of Extended
			 * Query Protocol we may remove connection right away.
			 */
			if (combiner->extended_query)
			{
				/* If we are doing merge sort clean current connection and return
				 * NULL, otherwise remove current connection, move last in-place,
				 * adjust current_conn and continue if it is not last connection */
				if (combiner->merge_sort)
				{
					combiner->connections[combiner->current_conn] = NULL;
					return NULL;
				}
				REMOVE_CURR_CONN(combiner);
				if (combiner->conn_count > 0)
					conn = combiner->connections[combiner->current_conn];
				else
					return NULL;
			}
		}
		else if (res == RESPONSE_ERROR)
		{
			/*
			 * If doing Extended Query Protocol we need to sync connection,
			 * otherwise subsequent commands will be ignored.
			 */
			if (combiner->extended_query)
			{
				if (pgxc_node_send_sync(conn) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to sync msg to node %u", conn->nodeoid)));
			}
			/*
			 * Do not wait for response from primary, it needs to wait
			 * for other nodes to respond. Instead go ahead and send query to
			 * other nodes. It will fail there, but we can continue with
			 * normal cleanup.
			 */
			if (combiner->probing_primary)
			{
				REMOVE_CURR_CONN(combiner);
				return NULL;
			}
		}
		else if (res == RESPONSE_READY)
		{
			/* If we are doing merge sort clean current connection and return
			 * NULL, otherwise remove current connection, move last in-place,
			 * adjust current_conn and continue if it is not last connection */
			if (combiner->merge_sort)
			{
				combiner->connections[combiner->current_conn] = NULL;
				return NULL;
			}
			REMOVE_CURR_CONN(combiner);
			if (combiner->conn_count > 0)
				conn = combiner->connections[combiner->current_conn];
			else
				return NULL;
		}
		else if (res == RESPONSE_TUPDESC)
		{
			ExecSetSlotDescriptor(combiner->ss.ps.ps_ResultTupleSlot,
								  combiner->tuple_desc);
			/* Now slot is responsible for freeng the descriptor */
			combiner->tuple_desc = NULL;
		}
		else if (res == RESPONSE_ASSIGN_GXID)
		{
			/* Do nothing. It must have been handled in handle_response() */
		}
		else if (res == RESPONSE_WAITXIDS)
		{
			/* Do nothing. It must have been handled in handle_response() */
		}
		else
		{
			// Can not get here?
			Assert(false);
		}
	}

	return NULL;
}


/*
 * Handle responses from the Datanode connections
 */
static int
pgxc_node_receive_responses(const int conn_count, PGXCNodeHandle ** connections,
						 struct timeval * timeout, ResponseCombiner *combiner)
{
	int			count = conn_count;
	PGXCNodeHandle *to_receive[conn_count];

	/* make a copy of the pointers to the connections */
	memcpy(to_receive, connections, conn_count * sizeof(PGXCNodeHandle *));

	/*
	 * Read results.
	 * Note we try and read from Datanode connections even if there is an error on one,
	 * so as to avoid reading incorrect results on the next statement.
	 * Other safegaurds exist to avoid this, however.
	 */
	while (count > 0)
	{
		int i = 0;

		if (pgxc_node_receive(count, to_receive, timeout))
			return EOF;
		while (i < count)
		{
			int result =  handle_response(to_receive[i], combiner);
			elog(DEBUG5, "Received response %d on connection to node %s",
					result, to_receive[i]->nodename);
			switch (result)
			{
				case RESPONSE_EOF: /* have something to read, keep receiving */
					i++;
					break;
				case RESPONSE_COMPLETE:
					if (to_receive[i]->state != DN_CONNECTION_STATE_ERROR_FATAL)
						/* Continue read until ReadyForQuery */
						break;
					/* fallthru */
				case RESPONSE_READY:
					/* fallthru */
				case RESPONSE_COPY:
					/* Handling is done, do not track this connection */
					count--;
					/* Move last connection in place */
					if (i < count)
						to_receive[i] = to_receive[count];
					break;
				case RESPONSE_ERROR:
					/* no handling needed, just wait for ReadyForQuery */
					break;

				case RESPONSE_WAITXIDS:
				case RESPONSE_ASSIGN_GXID:
				case RESPONSE_TUPDESC:
					break;

				case RESPONSE_DATAROW:
					combiner->currentRow = NULL;
					break;

				default:
					/* Inconsistent responses */
					add_error_message(to_receive[i], "Unexpected response from the Datanodes");
					elog(DEBUG1, "Unexpected response from the Datanodes, result = %d, request type %d", result, combiner->request_type);
					/* Stop tracking and move last connection in place */
					count--;
					if (i < count)
						to_receive[i] = to_receive[count];
			}
		}
	}

	return 0;
}

/*
 * Read next message from the connection and update the combiner
 * and connection state accordingly
 * If we are in an error state we just consume the messages, and do not proxy
 * Long term, we should look into cancelling executing statements
 * and closing the connections.
 * It returns if states need to be handled
 * Return values:
 * RESPONSE_EOF - need to receive more data for the connection
 * RESPONSE_READY - got ReadyForQuery
 * RESPONSE_COMPLETE - done with the connection, but not yet ready for query.
 * Also this result is output in case of error
 * RESPONSE_SUSPENDED - got PortalSuspended
 * RESPONSE_TUPLEDESC - got tuple description
 * RESPONSE_DATAROW - got data row
 * RESPONSE_COPY - got copy response
 * RESPONSE_BARRIER_OK - barrier command completed successfully
 */
int
handle_response(PGXCNodeHandle *conn, ResponseCombiner *combiner)
{
	char	   *msg;
	int			msg_len;
	char		msg_type;

	for (;;)
	{
		/*
		 * If we are in the process of shutting down, we
		 * may be rolling back, and the buffer may contain other messages.
		 * We want to avoid a procarray exception
		 * as well as an error stack overflow.
		 */
		if (proc_exit_inprogress)
			PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_ERROR_FATAL);

		/*
		 * Don't read from from the connection if there is a fatal error.
		 * We still return RESPONSE_COMPLETE, not RESPONSE_ERROR, since
		 * Handling of RESPONSE_ERROR assumes sending SYNC message, but
		 * State DN_CONNECTION_STATE_ERROR_FATAL indicates connection is
		 * not usable.
		 */
		if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
			return RESPONSE_COMPLETE;

		/* No data available, exit */
		if (!HAS_MESSAGE_BUFFERED(conn))
			return RESPONSE_EOF;

		Assert(conn->combiner == combiner || conn->combiner == NULL);

		/* TODO handle other possible responses */
		msg_type = get_message(conn, &msg_len, &msg);
		elog(DEBUG5, "handle_response - received message %c, node %s, "
				"current_state %d", msg_type, conn->nodename, conn->state);
		switch (msg_type)
		{
			case '\0':			/* Not enough data in the buffer */
				return RESPONSE_EOF;
			case 'c':			/* CopyToCommandComplete */
				HandleCopyOutComplete(combiner);
				break;
			case 'C':			/* CommandComplete */
				HandleCommandComplete(combiner, msg, msg_len, conn);
				conn->combiner = NULL;
				/* 
				 * In case of simple query protocol, wait for the ReadyForQuery
				 * before marking connection as Idle
				 */
				if (combiner->extended_query &&
					conn->state == DN_CONNECTION_STATE_QUERY)
					PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
				return RESPONSE_COMPLETE;
			case 'T':			/* RowDescription */
#ifdef DN_CONNECTION_DEBUG
				Assert(!conn->have_row_desc);
				conn->have_row_desc = true;
#endif
				if (HandleRowDescription(combiner, msg, msg_len))
					return RESPONSE_TUPDESC;
				break;
			case 'D':			/* DataRow */
#ifdef DN_CONNECTION_DEBUG
				Assert(conn->have_row_desc);
#endif
				/* Do not return if data row has not been actually handled */
				if (HandleDataRow(combiner, msg, msg_len, conn->nodeoid))
					return RESPONSE_DATAROW;
				break;
			case 's':			/* PortalSuspended */
				/* No activity is expected on the connection until next query */
				PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
				conn->combiner = NULL;
				return RESPONSE_SUSPENDED;
			case '1': /* ParseComplete */
			case '2': /* BindComplete */
			case '3': /* CloseComplete */
			case 'n': /* NoData */
				/* simple notifications, continue reading */
				break;
			case 'G': /* CopyInResponse */
				PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_COPY_IN);
				HandleCopyIn(combiner);
				/* Done, return to caller to let it know the data can be passed in */
				return RESPONSE_COPY;
			case 'H': /* CopyOutResponse */
				PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_COPY_OUT);
				HandleCopyOut(combiner);
				return RESPONSE_COPY;
			case 'd': /* CopyOutDataRow */
				PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_COPY_OUT);
				HandleCopyDataRow(combiner, msg, msg_len);
				break;
			case 'E':			/* ErrorResponse */
				HandleError(combiner, msg, msg_len, conn);
				add_error_message(conn, combiner->errorMessage);
				/*
				 * In case the remote node was running an extended query
				 * protocol and reported an error, it will keep ignoring all
				 * subsequent commands until it sees a SYNC message. So make
				 * sure that we send down SYNC even before sending a ROLLBACK
				 * command
				 */
				if (conn->in_extended_query)
					conn->needSync = true;
				return RESPONSE_ERROR;
			case 'A':			/* NotificationResponse */
			case 'N':			/* NoticeResponse */
			case 'S':			/* SetCommandComplete */
				/*
				 * Ignore these to prevent multiple messages, one from each
				 * node. Coordinator will send one for DDL anyway
				 */
				break;
			case 'Z':			/* ReadyForQuery */
			{
				/*
				 * Return result depends on previous connection state.
				 * If it was PORTAL_SUSPENDED Coordinator want to send down
				 * another EXECUTE to fetch more rows, otherwise it is done
				 * with the connection
				 */
				conn->transaction_status = msg[0];
				PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
				conn->combiner = NULL;
#ifdef DN_CONNECTION_DEBUG
				conn->have_row_desc = false;
#endif
				return RESPONSE_READY;
			}
			case 'M':			/* Command Id */
				HandleDatanodeCommandId(combiner, msg, msg_len);
				break;
			case 'b':
				PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
				return RESPONSE_BARRIER_OK;
			case 'I':			/* EmptyQuery */
				return RESPONSE_COMPLETE;
			case 'W':
				HandleWaitXids(msg, msg_len);	
				return RESPONSE_WAITXIDS;
			case 'x':
				HandleGlobalTransactionId(msg, msg_len);
				return RESPONSE_ASSIGN_GXID;
			default:
				/* sync lost? */
				elog(WARNING, "Received unsupported message type: %c", msg_type);
				PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_ERROR_FATAL);
				/* stop reading */
				return RESPONSE_COMPLETE;
		}
	}
	/* never happen, but keep compiler quiet */
	return RESPONSE_EOF;
}

/*
 * Has the data node sent Ready For Query
 */

bool
is_data_node_ready(PGXCNodeHandle * conn)
{
	char		*msg;
	int		msg_len;
	char		msg_type;

	for (;;)
	{
		/*
		 * If we are in the process of shutting down, we
		 * may be rolling back, and the buffer may contain other messages.
		 * We want to avoid a procarray exception
		 * as well as an error stack overflow.
		 */
		if (proc_exit_inprogress)
			PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_ERROR_FATAL);

		/* don't read from from the connection if there is a fatal error */
		if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
			return true;

		/* No data available, exit */
		if (!HAS_MESSAGE_BUFFERED(conn))
			return false;

		msg_type = get_message(conn, &msg_len, &msg);
		if (msg_type == 'Z')
		{
			/*
			 * Return result depends on previous connection state.
			 * If it was PORTAL_SUSPENDED Coordinator want to send down
			 * another EXECUTE to fetch more rows, otherwise it is done
			 * with the connection
			 */
			conn->transaction_status = msg[0];
			PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_IDLE);
			conn->combiner = NULL;
			return true;
		}
	}
	/* never happen, but keep compiler quiet */
	return false;
}


/*
 * Send BEGIN command to the Datanodes or Coordinators and receive responses.
 * Also send the GXID for the transaction.
 */
static int
pgxc_node_begin(int conn_count, PGXCNodeHandle **connections,
				GlobalTransactionId gxid, bool need_tran_block,
				bool readOnly, char node_type)
{
	int			i;
	struct timeval *timeout = NULL;
	ResponseCombiner combiner;
	TimestampTz timestamp = GetCurrentGTMStartTimestamp();
	PGXCNodeHandle *new_connections[conn_count];
	int new_count = 0;
	char 		   *init_str;
	char 			lxid[13];

	/*
	 * If no remote connections, we don't have anything to do
	 */
	if (conn_count == 0)
		return 0;

	for (i = 0; i < conn_count; i++)
	{
		if (!readOnly && !IsConnFromDatanode())
			connections[i]->read_only = false;
		/*
		 * PGXC TODO - A connection should not be in DN_CONNECTION_STATE_QUERY
		 * state when we are about to send a BEGIN TRANSACTION command to the
		 * node. We should consider changing the following to an assert and fix
		 * any bugs reported
		 */
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);

		/* Send GXID and check for errors */
		if (GlobalTransactionIdIsValid(gxid) && pgxc_node_send_gxid(connections[i], gxid))
			return EOF;

		/* Send timestamp and check for errors */
		if (GlobalTimestampIsValid(timestamp) && pgxc_node_send_timestamp(connections[i], timestamp))
			return EOF;

		if (IS_PGXC_DATANODE && GlobalTransactionIdIsValid(gxid))
			need_tran_block = true;
		else if (IS_PGXC_REMOTE_COORDINATOR)
			need_tran_block = false;

		elog(DEBUG5, "need_tran_block %d, connections[%d]->transaction_status %c",
				need_tran_block, i, connections[i]->transaction_status);
		/* Send BEGIN if not already in transaction */
		if (need_tran_block && connections[i]->transaction_status == 'I')
		{
			/* Send the BEGIN TRANSACTION command and check for errors */
			if (pgxc_node_send_query(connections[i], "BEGIN"))
				return EOF;

			new_connections[new_count++] = connections[i];
		}
	}

	/*
	 * If we did not send a BEGIN command to any node, we are done. Otherwise,
	 * we need to check for any errors and report them
	 */
	if (new_count == 0)
		return 0;

	InitResponseCombiner(&combiner, new_count, COMBINE_TYPE_NONE);
	/*
	 * Make sure there are zeroes in unused fields
	 */
	memset(&combiner, 0, sizeof(ScanState));

	/* Receive responses */
	if (pgxc_node_receive_responses(new_count, new_connections, timeout, &combiner))
		return EOF;

	/* Verify status */
	if (!ValidateAndCloseCombiner(&combiner))
		return EOF;

	/* Send virtualXID to the remote nodes using SET command */
	sprintf(lxid, "%d", MyProc->lxid);
	PGXCNodeSetParam(true, "coordinator_lxid", lxid);

	/* after transactions are started send down local set commands */
	init_str = PGXCNodeGetTransactionParamStr();
	if (init_str)
	{
		for (i = 0; i < new_count; i++)
		{
			pgxc_node_set_query(new_connections[i], init_str);
		}
	}

	/* No problem, let's get going */
	return 0;
}


/*
 * Execute DISCARD ALL command on all allocated nodes to remove all session
 * specific stuff before releasing them to pool for reuse by other sessions.
 */
static void
pgxc_node_remote_cleanup_all(void)
{
	PGXCNodeAllHandles *handles = get_current_handles();
	PGXCNodeHandle *new_connections[handles->co_conn_count + handles->dn_conn_count];
	int				new_conn_count = 0;
	int				i;
	char		   *resetcmd = "RESET ALL;RESET SESSION AUTHORIZATION;"
							   "RESET transaction_isolation;";

	elog(DEBUG5, "pgxc_node_remote_cleanup_all - handles->co_conn_count %d,"
			"handles->dn_conn_count %d", handles->co_conn_count,
			handles->dn_conn_count);
	/*
	 * We must handle reader and writer connections both since even a read-only
	 * needs to be cleaned up.
	 */
	if (handles->co_conn_count + handles->dn_conn_count == 0)
		return;

	/*
	 * Send down snapshot followed by DISCARD ALL command.
	 */
	for (i = 0; i < handles->co_conn_count; i++)
	{
		PGXCNodeHandle *handle = handles->coord_handles[i];

		/* At this point connection should be in IDLE state */
		if (handle->state != DN_CONNECTION_STATE_IDLE)
		{
			PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_ERROR_FATAL);
			continue;
		}

		/*
		 * We must go ahead and release connections anyway, so do not throw
		 * an error if we have a problem here.
		 */
		if (pgxc_node_send_query(handle, resetcmd))
		{
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to clean up data nodes")));
			PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_ERROR_FATAL);
			continue;
		}
		new_connections[new_conn_count++] = handle;
		handle->combiner = NULL;
	}
	for (i = 0; i < handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *handle = handles->datanode_handles[i];

		/* At this point connection should be in IDLE state */
		if (handle->state != DN_CONNECTION_STATE_IDLE)
		{
			PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_ERROR_FATAL);
			continue;
		}

		/*
		 * We must go ahead and release connections anyway, so do not throw
		 * an error if we have a problem here.
		 */
		if (pgxc_node_send_query(handle, resetcmd))
		{
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to clean up data nodes")));
			PGXCNodeSetConnectionState(handle, DN_CONNECTION_STATE_ERROR_FATAL);
			continue;
		}
		new_connections[new_conn_count++] = handle;
		handle->combiner = NULL;
	}

	if (new_conn_count)
	{
		ResponseCombiner combiner;
		InitResponseCombiner(&combiner, new_conn_count, COMBINE_TYPE_NONE);
		/* Receive responses */
		pgxc_node_receive_responses(new_conn_count, new_connections, NULL, &combiner);
		CloseCombiner(&combiner);
	}
	pfree_pgxc_all_handles(handles);
}

/*
 * Count how many coordinators and datanodes are involved in this transaction
 * so that we can save that information in the GID
 */
static void
pgxc_node_remote_count(int *dnCount, int dnNodeIds[],
		int *coordCount, int coordNodeIds[])
{
	int i;
	PGXCNodeAllHandles *handles = get_current_handles();

	*dnCount = *coordCount = 0;
	for (i = 0; i < handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->datanode_handles[i];
		/*
		 * Skip empty slots
		 */
		if (conn->sock == NO_SOCKET)
			continue;
		else if (conn->transaction_status == 'T')
		{
			if (!conn->read_only)
			{
				dnNodeIds[*dnCount] = conn->nodeid;
				*dnCount = *dnCount + 1;
			}
		}
	}

	for (i = 0; i < handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->coord_handles[i];
		/*
		 * Skip empty slots
		 */
		if (conn->sock == NO_SOCKET)
			continue;
		else if (conn->transaction_status == 'T')
		{
			if (!conn->read_only)
			{
				coordNodeIds[*coordCount] = conn->nodeid;
				*coordCount = *coordCount + 1;
			}
		}
	}
}

/*
 * Prepare nodes which ran write operations during the transaction.
 * Read only remote transactions are committed and connections are released
 * back to the pool.
 * Function returns the list of nodes where transaction is prepared, including
 * local node, if requested, in format expected by the GTM server.
 * If something went wrong the function tries to abort prepared transactions on
 * the nodes where it succeeded and throws error. A warning is emitted if abort
 * prepared fails.
 * After completion remote connection handles are released.
 */
static char *
pgxc_node_remote_prepare(char *prepareGID, bool localNode)
{
	bool 			isOK = true;
	StringInfoData 	nodestr;
	char			*prepare_cmd = (char *) palloc (64 + strlen(prepareGID));
	char			*abort_cmd;
	GlobalTransactionId auxXid;
	char		   *commit_cmd = "COMMIT TRANSACTION";
	int				i;
	ResponseCombiner combiner;
	PGXCNodeHandle *connections[MaxDataNodes + MaxCoords];
	int				conn_count = 0;
	PGXCNodeAllHandles *handles = get_current_handles();

	initStringInfo(&nodestr);
	if (localNode)
		appendStringInfoString(&nodestr, PGXCNodeName);

	sprintf(prepare_cmd, "PREPARE TRANSACTION '%s'", prepareGID);

	for (i = 0; i < handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->datanode_handles[i];

		/*
		 * If something went wrong already we have nothing to do here. The error
		 * will be reported at the end of the function, and we will rollback
		 * remotes as part of the error handling.
		 * Just skip to clean up section and check if we have already prepared
		 * somewhere, we should abort that prepared transaction.
		 */
		if (!isOK)
			goto prepare_err;

		/*
		 * Skip empty slots
		 */
		if (conn->sock == NO_SOCKET)
			continue;
		else if (conn->transaction_status == 'T')
		{
			/* Read in any pending input */
			if (conn->state != DN_CONNECTION_STATE_IDLE)
				BufferConnection(conn);

			if (conn->read_only)
			{
				/* Send down prepare command */
				if (pgxc_node_send_query(conn, commit_cmd))
				{
					/*
					 * not a big deal, it was read only, the connection will be
					 * abandoned later.
					 */
					ereport(LOG,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("failed to send COMMIT command to "
								"the node %u", conn->nodeoid)));
				}
				else
				{
					/* Read responses from these */
					connections[conn_count++] = conn;
				}
			}
			else
			{
				/* Send down prepare command */
				if (pgxc_node_send_query(conn, prepare_cmd))
				{
					/*
					 * That is the trouble, we really want to prepare it.
					 * Just emit warning so far and go to clean up.
					 */
					isOK = false;
					ereport(WARNING,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("failed to send PREPARE TRANSACTION command to "
								"the node %u", conn->nodeoid)));
				}
				else
				{
					char *nodename = get_pgxc_nodename(conn->nodeoid);
					if (nodestr.len > 0)
						appendStringInfoChar(&nodestr, ',');
					appendStringInfoString(&nodestr, nodename);
					/* Read responses from these */
					connections[conn_count++] = conn;
					/*
					 * If it fails on remote node it would just return ROLLBACK.
					 * Set the flag for the message handler so the response is
					 * verified.
					 */
					conn->ck_resp_rollback = true;
				}
			}
		}
		else if (conn->transaction_status == 'E')
		{
			/*
			 * Probably can not happen, if there was a error the engine would
			 * abort anyway, even in case of explicit PREPARE.
			 * Anyway, just in case...
			 */
			isOK = false;
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("remote node %u is in error state", conn->nodeoid)));
		}
	}

	for (i = 0; i < handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->coord_handles[i];

		/*
		 * If something went wrong already we have nothing to do here. The error
		 * will be reported at the end of the function, and we will rollback
		 * remotes as part of the error handling.
		 * Just skip to clean up section and check if we have already prepared
		 * somewhere, we should abort that prepared transaction.
		 */
		if (!isOK)
			goto prepare_err;

		/*
		 * Skip empty slots
		 */
		if (conn->sock == NO_SOCKET)
			continue;
		else if (conn->transaction_status == 'T')
		{
			if (conn->read_only)
			{
				/* Send down prepare command */
				if (pgxc_node_send_query(conn, commit_cmd))
				{
					/*
					 * not a big deal, it was read only, the connection will be
					 * abandoned later.
					 */
					ereport(LOG,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("failed to send COMMIT command to "
								"the node %u", conn->nodeoid)));
				}
				else
				{
					/* Read responses from these */
					connections[conn_count++] = conn;
				}
			}
			else
			{
				/* Send down prepare command */
				if (pgxc_node_send_query(conn, prepare_cmd))
				{
					/*
					 * That is the trouble, we really want to prepare it.
					 * Just emit warning so far and go to clean up.
					 */
					isOK = false;
					ereport(WARNING,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("failed to send PREPARE TRANSACTION command to "
								"the node %u", conn->nodeoid)));
				}
				else
				{
					char *nodename = get_pgxc_nodename(conn->nodeoid);
					if (nodestr.len > 0)
						appendStringInfoChar(&nodestr, ',');
					appendStringInfoString(&nodestr, nodename);
					/* Read responses from these */
					connections[conn_count++] = conn;
					/*
					 * If it fails on remote node it would just return ROLLBACK.
					 * Set the flag for the message handler so the response is
					 * verified.
					 */
					conn->ck_resp_rollback = true;
				}
			}
		}
		else if (conn->transaction_status == 'E')
		{
			/*
			 * Probably can not happen, if there was a error the engine would
			 * abort anyway, even in case of explicit PREPARE.
			 * Anyway, just in case...
			 */
			isOK = false;
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("remote node %u is in error state", conn->nodeoid)));
		}
	}

	SetSendCommandId(false);

	if (!isOK)
		goto prepare_err;

	/* exit if nothing has been prepared */
	if (conn_count > 0)
	{
		int result;
		/*
		 * Receive and check for any errors. In case of errors, we don't bail out
		 * just yet. We first go through the list of connections and look for
		 * errors on each connection. This is important to ensure that we run
		 * an appropriate ROLLBACK command later on (prepared transactions must be
		 * rolled back with ROLLBACK PREPARED commands).
		 *
		 * PGXCTODO - There doesn't seem to be a solid mechanism to track errors on
		 * individual connections. The transaction_status field doesn't get set
		 * every time there is an error on the connection. The combiner mechanism is
		 * good for parallel proessing, but I think we should have a leak-proof
		 * mechanism to track connection status
		 */
		InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
		/* Receive responses */
		result = pgxc_node_receive_responses(conn_count, connections, NULL, &combiner);
		if (result || !validate_combiner(&combiner))
			goto prepare_err;
		else
			CloseCombiner(&combiner);

		/* Before exit clean the flag, to avoid unnecessary checks */
		for (i = 0; i < conn_count; i++)
			connections[i]->ck_resp_rollback = false;

		pfree_pgxc_all_handles(handles);
		if (!temp_object_included && !PersistentConnections)
		{
			/* Clean up remote sessions */
			pgxc_node_remote_cleanup_all();
			release_handles();
		}
	}

	pfree(prepare_cmd);
	return nodestr.data;

prepare_err:
 	abort_cmd = (char *) palloc (64 + strlen(prepareGID));
	sprintf(abort_cmd, "ROLLBACK PREPARED '%s'", prepareGID);

	auxXid = GetAuxilliaryTransactionId();
	conn_count = 0;
	for (i = 0; i < handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->datanode_handles[i];

		/*
		 * PREPARE succeeded on that node, roll it back there
		 */
		if (conn->ck_resp_rollback)
		{
			conn->ck_resp_rollback = false;

			if (conn->state != DN_CONNECTION_STATE_IDLE)
			{
				ereport(WARNING,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Error while PREPARING transaction %s on "
							 "node %s. Administrative action may be required "
							 "to abort this transaction on the node",
							 prepareGID, conn->nodename)));
				continue;
			}

			/* sanity checks */
			Assert(conn->sock != NO_SOCKET);
			/* Send down abort prepared command */
			if (pgxc_node_send_gxid(conn, auxXid))
			{
				/*
				 * Prepared transaction is left on the node, but we can not
				 * do anything with that except warn the user.
				 */
				ereport(WARNING,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("failed to send xid to "
								"the node %u", conn->nodeoid)));
			}
			if (pgxc_node_send_query(conn, abort_cmd))
			{
				/*
				 * Prepared transaction is left on the node, but we can not
				 * do anything with that except warn the user.
				 */
				ereport(WARNING,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("failed to send ABORT PREPARED command to "
								"the node %u", conn->nodeoid)));
			}
			else
			{
				/* Read responses from these */
				connections[conn_count++] = conn;
			}
		}
	}
	for (i = 0; i < handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->coord_handles[i];

		if (conn->ck_resp_rollback)
		{
			conn->ck_resp_rollback = false;

			if (conn->state != DN_CONNECTION_STATE_IDLE)
			{
				ereport(WARNING,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Error while PREPARING transaction %s on "
							 "node %s. Administrative action may be required "
							 "to abort this transaction on the node",
							 prepareGID, conn->nodename)));
				continue;
			}

			/* sanity checks */
			Assert(conn->sock != NO_SOCKET);
			/* Send down abort prepared command */
			if (pgxc_node_send_gxid(conn, auxXid))
			{
				/*
				 * Prepared transaction is left on the node, but we can not
				 * do anything with that except warn the user.
				 */
				ereport(WARNING,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("failed to send xid to "
								"the node %u", conn->nodeoid)));
			}
			if (pgxc_node_send_query(conn, abort_cmd))
			{
				/*
				 * Prepared transaction is left on the node, but we can not
				 * do anything with that except warn the user.
				 */
				ereport(WARNING,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("failed to send ABORT PREPARED command to "
								"the node %u", conn->nodeoid)));
			}
			else
			{
				/* Read responses from these */
				connections[conn_count++] = conn;
			}
		}
	}
	if (conn_count > 0)
	{
		/* Just read out responses, throw error from the first combiner */
		ResponseCombiner combiner2;
		InitResponseCombiner(&combiner2, conn_count, COMBINE_TYPE_NONE);
		/* Receive responses */
		pgxc_node_receive_responses(conn_count, connections, NULL, &combiner2);
		CloseCombiner(&combiner2);
	}

	if (!temp_object_included && !PersistentConnections)
	{
		/* Clean up remote sessions */
		pgxc_node_remote_cleanup_all();
		release_handles();
	}

	pfree_pgxc_all_handles(handles);
	pfree(abort_cmd);

	/*
	 * If the flag is set we are here because combiner carries error message
	 */
	if (isOK)
		pgxc_node_report_error(&combiner);
	else
		elog(ERROR, "failed to PREPARE transaction on one or more nodes");
	return NULL;
}


/*
 * Commit transactions on remote nodes.
 * If barrier lock is set wait while it is released.
 * Release remote connection after completion.
 */
static void
pgxc_node_remote_commit(void)
{
	int				result = 0;
	char		   *commitCmd = "COMMIT TRANSACTION";
	int				i;
	ResponseCombiner combiner;
	PGXCNodeHandle *connections[MaxDataNodes + MaxCoords];
	int				conn_count = 0;
	PGXCNodeAllHandles *handles = get_current_handles();

	SetSendCommandId(false);

	/*
	 * Barrier:
	 *
	 * We should acquire the BarrierLock in SHARE mode here to ensure that
	 * there are no in-progress barrier at this point. This mechanism would
	 * work as long as LWLock mechanism does not starve a EXCLUSIVE lock
	 * requester
	 */
	LWLockAcquire(BarrierLock, LW_SHARED);

	for (i = 0; i < handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->datanode_handles[i];

		/* Skip empty slots */
		if (conn->sock == NO_SOCKET)
			continue;

		/*
		 * We do not need to commit remote node if it is not in transaction.
		 * If transaction is in error state the commit command will cause
		 * rollback, that is OK
		 */
		if (conn->transaction_status != 'I')
		{
			/* Read in any pending input */
			if (conn->state != DN_CONNECTION_STATE_IDLE)
				BufferConnection(conn);

			if (pgxc_node_send_query(conn, commitCmd))
			{
				/*
				 * Do not bother with clean up, just bomb out. The error handler
				 * will invoke RollbackTransaction which will do the work.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("failed to send COMMIT command to the node %u",
								conn->nodeoid)));
			}
			else
			{
				/* Read responses from these */
				connections[conn_count++] = conn;
			}
		}
	}

	for (i = 0; i < handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->coord_handles[i];

		/* Skip empty slots */
		if (conn->sock == NO_SOCKET)
			continue;

		/*
		 * We do not need to commit remote node if it is not in transaction.
		 * If transaction is in error state the commit command will cause
		 * rollback, that is OK
		 */
		if (conn->transaction_status != 'I')
		{
			if (pgxc_node_send_query(conn, commitCmd))
			{
				/*
				 * Do not bother with clean up, just bomb out. The error handler
				 * will invoke RollbackTransaction which will do the work.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("failed to send COMMIT command to the node %u",
								conn->nodeoid)));
			}
			else
			{
				/* Read responses from these */
				connections[conn_count++] = conn;
			}
		}
	}

	/*
	 * Release the BarrierLock.
	 */
	LWLockRelease(BarrierLock);

	if (conn_count)
	{
		InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
		/* Receive responses */
		result = pgxc_node_receive_responses(conn_count, connections, NULL, &combiner);
		if (result || !validate_combiner(&combiner))
			result = EOF;
		else
			CloseCombiner(&combiner);
	}

	stat_transaction(conn_count);

	if (result)
	{
		if (combiner.errorMessage)
			pgxc_node_report_error(&combiner);
		else
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to COMMIT the transaction on one or more nodes")));
	}

	if (!temp_object_included && !PersistentConnections)
	{
		/* Clean up remote sessions */
		pgxc_node_remote_cleanup_all();
		release_handles();
	}

	pfree_pgxc_all_handles(handles);
}


/*
 * Rollback transactions on remote nodes.
 * Release remote connection after completion.
 */
static void
pgxc_node_remote_abort(void)
{
	int				result = 0;
	char		   *rollbackCmd = "ROLLBACK TRANSACTION";
	int				i;
	ResponseCombiner combiner;
	PGXCNodeHandle *connections[MaxDataNodes + MaxCoords];
	int				conn_count = 0;
	PGXCNodeAllHandles *handles = get_current_handles();
	struct timeval timeout;

	SetSendCommandId(false);

	elog(DEBUG5, "pgxc_node_remote_abort - dn_conn_count %d, co_conn_count %d",
			handles->dn_conn_count, handles->co_conn_count);

	timeout.tv_sec = 60;
	timeout.tv_usec = 0;

	for (i = 0; i < handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->datanode_handles[i];

		/* Skip empty slots */
		if (conn->sock == NO_SOCKET)
			continue;

		elog(DEBUG5, "node %s, conn->transaction_status %c",
				conn->nodename,
				conn->transaction_status);

		if (conn->transaction_status != 'I')
		{
			/* Read in any pending input */
			if (conn->state != DN_CONNECTION_STATE_IDLE)
				BufferConnection(conn);

			/*
			 * If the remote session was running extended query protocol when
			 * it failed, it will expect a SYNC message before it accepts any
			 * other command
			 */
			if (conn->needSync)
			{
				pgxc_node_send_sync(conn);
				pgxc_node_receive(1, &conn, &timeout);
			}
			/*
			 * Do not matter, is there committed or failed transaction,
			 * just send down rollback to finish it.
			 */
			if (pgxc_node_send_rollback(conn, rollbackCmd))
			{
				add_error_message(conn,
						"failed to send ROLLBACK TRANSACTION command");
			}
			else
			{
				/* Read responses from these */
				connections[conn_count++] = conn;
			}
		}
	}

	for (i = 0; i < handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->coord_handles[i];

		/* Skip empty slots */
		if (conn->sock == NO_SOCKET)
			continue;

		if (conn->transaction_status != 'I')
		{
			/* Send SYNC if the remote session is expecting one */
			if (conn->needSync)
			{
				pgxc_node_send_sync(conn);
				pgxc_node_receive(1, &conn, &timeout);
			}
			/*
			 * Do not matter, is there committed or failed transaction,
			 * just send down rollback to finish it.
			 */
			if (pgxc_node_send_rollback(conn, rollbackCmd))
			{
				add_error_message(conn,
						"failed to send ROLLBACK TRANSACTION command");
			}
			else
			{
				/* Read responses from these */
				connections[conn_count++] = conn;
			}
		}
	}

	if (conn_count)
	{
		InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
		/* Receive responses */
		result = pgxc_node_receive_responses(conn_count, connections, &timeout, &combiner);
		if (result || !validate_combiner(&combiner))
			result = EOF;
		else
			CloseCombiner(&combiner);
	}

	stat_transaction(conn_count);

	if (result)
	{
		if (combiner.errorMessage)
			pgxc_node_report_error(&combiner);
		else
			ereport(LOG,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to ROLLBACK the transaction on one or more nodes")));
	}

	pfree_pgxc_all_handles(handles);
}

/*
 * Begin COPY command
 * The copy_connections array must have room for NumDataNodes items
 */
void
DataNodeCopyBegin(RemoteCopyData *rcstate)
{
	int i;
	List *nodelist = rcstate->rel_loc->rl_nodeList;
	PGXCNodeHandle **connections;
	bool need_tran_block;
	GlobalTransactionId gxid;
	ResponseCombiner combiner;
	Snapshot snapshot = GetActiveSnapshot();
	int conn_count = list_length(nodelist);

	/* Get needed datanode connections */
	if (!rcstate->is_from && IsLocatorReplicated(rcstate->rel_loc->locatorType))
	{
		/* Connections is a single handle to read from */
		connections = (PGXCNodeHandle **) palloc(sizeof(PGXCNodeHandle *));
		connections[0] = get_any_handle(nodelist);
		conn_count = 1;
	}
	else
	{
		PGXCNodeAllHandles *pgxc_handles;
		pgxc_handles = get_handles(nodelist, NULL, false, true);
		connections = pgxc_handles->datanode_handles;
		Assert(pgxc_handles->dn_conn_count == conn_count);
		pfree(pgxc_handles);
	}

	/*
	 * If more than one nodes are involved or if we are already in a
	 * transaction block, we must the remote statements in a transaction block
	 */
	need_tran_block = (conn_count > 1) || (TransactionBlockStatusCode() == 'T');

	elog(DEBUG1, "conn_count = %d, need_tran_block = %s", conn_count,
			need_tran_block ? "true" : "false");

	/* Gather statistics */
	stat_statement();
	stat_transaction(conn_count);

	gxid = GetCurrentTransactionId();

	/* Start transaction on connections where it is not started */
	if (pgxc_node_begin(conn_count, connections, gxid, need_tran_block, false, PGXC_NODE_DATANODE))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not begin transaction on data nodes.")));
	}

	/*
	 * COPY TO do not use locator, it just takes connections from it, and
	 * we do not look up distribution data type in this case.
	 * So always use LOCATOR_TYPE_RROBIN to avoid errors because of not
	 * defined partType if real locator type is HASH or MODULO.
	 * Create locator before sending down query, because createLocator may
	 * fail and we leave with dirty connections.
	 * If we get an error now datanode connection will be clean and error
	 * handler will issue transaction abort.
	 */
	rcstate->locator = createLocator(
			rcstate->is_from ? rcstate->rel_loc->locatorType
					: LOCATOR_TYPE_RROBIN,
			rcstate->is_from ? RELATION_ACCESS_INSERT : RELATION_ACCESS_READ,
			rcstate->dist_type,
			LOCATOR_LIST_POINTER,
			conn_count,
			(void *) connections,
			NULL,
			false);

	/* Send query to nodes */
	for (i = 0; i < conn_count; i++)
	{
		CHECK_OWNERSHIP(connections[i], NULL);

		if (snapshot && pgxc_node_send_snapshot(connections[i], snapshot))
		{
			add_error_message(connections[i], "Can not send request");
			pfree(connections);
			freeLocator(rcstate->locator);
			rcstate->locator = NULL;
			return;
		}
		if (pgxc_node_send_query(connections[i], rcstate->query_buf.data) != 0)
		{
			add_error_message(connections[i], "Can not send request");
			pfree(connections);
			freeLocator(rcstate->locator);
			rcstate->locator = NULL;
			return;
		}
	}

	/*
	 * We are expecting CopyIn response, but do not want to send it to client,
	 * caller should take care about this, because here we do not know if
	 * client runs console or file copy
	 */
	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
	/*
	 * Make sure there are zeroes in unused fields
	 */
	memset(&combiner, 0, sizeof(ScanState));

	/* Receive responses */
	if (pgxc_node_receive_responses(conn_count, connections, NULL, &combiner)
			|| !ValidateAndCloseCombiner(&combiner))
	{
		DataNodeCopyFinish(conn_count, connections);
		freeLocator(rcstate->locator);
		rcstate->locator = NULL;
		return;
	}
	pfree(connections);
}


/*
 * Send a data row to the specified nodes
 */
int
DataNodeCopyIn(char *data_row, int len, int conn_count, PGXCNodeHandle** copy_connections)
{
	/* size + data row + \n */
	int msgLen = 4 + len + 1;
	int nLen = htonl(msgLen);
	int i;

	for(i = 0; i < conn_count; i++)
	{
		PGXCNodeHandle *handle = copy_connections[i];
		if (handle->state == DN_CONNECTION_STATE_COPY_IN)
		{
			/* precalculate to speed up access */
			int bytes_needed = handle->outEnd + 1 + msgLen;

			/* flush buffer if it is almost full */
			if (bytes_needed > COPY_BUFFER_SIZE)
			{
				int to_send = handle->outEnd;

				/* First look if data node has sent a error message */
				int read_status = pgxc_node_read_data(handle, true);
				if (read_status == EOF || read_status < 0)
				{
					add_error_message(handle, "failed to read data from data node");
					return EOF;
				}

				if (handle->inStart < handle->inEnd)
				{
					ResponseCombiner combiner;
					InitResponseCombiner(&combiner, 1, COMBINE_TYPE_NONE);
					/*
					 * Make sure there are zeroes in unused fields
					 */
					memset(&combiner, 0, sizeof(ScanState));
					handle_response(handle, &combiner);
					if (!ValidateAndCloseCombiner(&combiner))
						return EOF;
				}

				if (DN_CONNECTION_STATE_ERROR(handle))
					return EOF;

				/*
				 * Try to send down buffered data if we have
				 */
				if (to_send && send_some(handle, to_send) < 0)
				{
					add_error_message(handle, "failed to send data to data node");
					return EOF;
				}
			}

			if (ensure_out_buffer_capacity(bytes_needed, handle) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("out of memory")));
			}

			handle->outBuffer[handle->outEnd++] = 'd';
			memcpy(handle->outBuffer + handle->outEnd, &nLen, 4);
			handle->outEnd += 4;
			memcpy(handle->outBuffer + handle->outEnd, data_row, len);
			handle->outEnd += len;
			handle->outBuffer[handle->outEnd++] = '\n';

			handle->in_extended_query = false;
		}
		else
		{
			add_error_message(handle, "Invalid data node connection");
			return EOF;
		}
	}
	return 0;
}

uint64
DataNodeCopyOut(PGXCNodeHandle** copy_connections,
							  int conn_count, FILE* copy_file)
{
	ResponseCombiner combiner;
	uint64		processed;
	bool 		error;

	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_SUM);
	/*
	 * Make sure there are zeroes in unused fields
	 */
	memset(&combiner, 0, sizeof(ScanState));
	combiner.processed = 0;
	/* If there is an existing file where to copy data, pass it to combiner */
	if (copy_file)
	{
		combiner.copy_file = copy_file;
		combiner.remoteCopyType = REMOTE_COPY_FILE;
	}
	else
	{
		combiner.copy_file = NULL;
		combiner.remoteCopyType = REMOTE_COPY_STDOUT;
	}
	error = (pgxc_node_receive_responses(conn_count, copy_connections, NULL, &combiner) != 0);

	processed = combiner.processed;

	if (!ValidateAndCloseCombiner(&combiner) || error)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes when combining, request type %d", combiner.request_type)));
	}

	return processed;
}


uint64
DataNodeCopyStore(PGXCNodeHandle** copy_connections,
								int conn_count, Tuplestorestate* store)
{
	ResponseCombiner combiner;
	uint64		processed;
	bool 		error;

	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_SUM);
	/*
	 * Make sure there are zeroes in unused fields
	 */
	memset(&combiner, 0, sizeof(ScanState));
	combiner.processed = 0;
	combiner.remoteCopyType = REMOTE_COPY_TUPLESTORE;
	combiner.tuplestorestate = store;

	error = (pgxc_node_receive_responses(conn_count, copy_connections, NULL, &combiner) != 0);

	processed = combiner.processed;

	if (!ValidateAndCloseCombiner(&combiner) || error)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Unexpected response from the data nodes when combining, request type %d", combiner.request_type)));
	}

	return processed;
}


/*
 * Finish copy process on all connections
 */
void
DataNodeCopyFinish(int conn_count, PGXCNodeHandle** connections)
{
	int		i;
	ResponseCombiner combiner;
	bool 		error = false;
	for (i = 0; i < conn_count; i++)
	{
		PGXCNodeHandle *handle = connections[i];

		error = true;
		if (handle->state == DN_CONNECTION_STATE_COPY_IN || handle->state == DN_CONNECTION_STATE_COPY_OUT)
			error = DataNodeCopyEnd(handle, false);
	}

	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
	/*
	 * Make sure there are zeroes in unused fields
	 */
	memset(&combiner, 0, sizeof(ScanState));
	error = (pgxc_node_receive_responses(conn_count, connections, NULL, &combiner) != 0) || error;

	if (!validate_combiner(&combiner) || error)
	{
		if (combiner.errorMessage)
			pgxc_node_report_error(&combiner);
		else
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Error while running COPY")));
	}
	else
		CloseCombiner(&combiner);
}

/*
 * End copy process on a connection
 */
bool
DataNodeCopyEnd(PGXCNodeHandle *handle, bool is_error)
{
	int 		nLen = htonl(4);

	if (handle == NULL)
		return true;

	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + 4, handle) != 0)
		return true;

	if (is_error)
		handle->outBuffer[handle->outEnd++] = 'f';
	else
		handle->outBuffer[handle->outEnd++] = 'c';

	memcpy(handle->outBuffer + handle->outEnd, &nLen, 4);
	handle->outEnd += 4;

	handle->in_extended_query = false;
	/* We need response right away, so send immediately */
	if (pgxc_node_flush(handle) < 0)
		return true;

	return false;
}


/*
 * Get Node connections depending on the connection type:
 * Datanodes Only, Coordinators only or both types
 */
static PGXCNodeAllHandles *
get_exec_connections(RemoteQueryState *planstate,
					 ExecNodes *exec_nodes,
					 RemoteQueryExecType exec_type,
					 bool is_global_session)
{
	List 	   *nodelist = NIL;
	List 	   *primarynode = NIL;
	List	   *coordlist = NIL;
	PGXCNodeHandle *primaryconnection;
	int			co_conn_count, dn_conn_count;
	bool		is_query_coord_only = false;
	PGXCNodeAllHandles *pgxc_handles = NULL;

	/*
	 * If query is launched only on Coordinators, we have to inform get_handles
	 * not to ask for Datanode connections even if list of Datanodes is NIL.
	 */
	if (exec_type == EXEC_ON_COORDS)
		is_query_coord_only = true;

	if (exec_type == EXEC_ON_CURRENT)
		return get_current_handles();

	if (exec_nodes)
	{
		if (exec_nodes->en_expr)
		{
			/* execution time determining of target Datanodes */
			bool isnull;
			ExprState *estate = ExecInitExpr(exec_nodes->en_expr,
											 (PlanState *) planstate);
			Datum partvalue = ExecEvalExpr(estate,
										   planstate->combiner.ss.ps.ps_ExprContext,
										   &isnull,
										   NULL);
			RelationLocInfo *rel_loc_info = GetRelationLocInfo(exec_nodes->en_relid);
			/* PGXCTODO what is the type of partvalue here */
			ExecNodes *nodes = GetRelationNodes(rel_loc_info,
												partvalue,
												isnull,
												exec_nodes->accesstype);
			/*
			 * en_expr is set by pgxc_set_en_expr only for distributed
			 * relations while planning DMLs, hence a select for update
			 * on a replicated table here is an assertion
			 */
			Assert(!(exec_nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE &&
						IsRelationReplicated(rel_loc_info)));

			if (nodes)
			{
				nodelist = nodes->nodeList;
				primarynode = nodes->primarynodelist;
				pfree(nodes);
			}
			FreeRelationLocInfo(rel_loc_info);
		}
		else if (OidIsValid(exec_nodes->en_relid))
		{
			RelationLocInfo *rel_loc_info = GetRelationLocInfo(exec_nodes->en_relid);
			ExecNodes *nodes = GetRelationNodes(rel_loc_info, 0, true, exec_nodes->accesstype);

			/*
			 * en_relid is set only for DMLs, hence a select for update on a
			 * replicated table here is an assertion
			 */
			Assert(!(exec_nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE &&
						IsRelationReplicated(rel_loc_info)));

			/* Use the obtained list for given table */
			if (nodes)
				nodelist = nodes->nodeList;

			/*
			 * Special handling for ROUND ROBIN distributed tables. The target
			 * node must be determined at the execution time
			 */
			if (rel_loc_info->locatorType == LOCATOR_TYPE_RROBIN && nodes)
			{
				nodelist = nodes->nodeList;
				primarynode = nodes->primarynodelist;
			}
			else if (nodes)
			{
				if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
				{
					nodelist = exec_nodes->nodeList;
					primarynode = exec_nodes->primarynodelist;
				}
			}

			if (nodes)
				pfree(nodes);
			FreeRelationLocInfo(rel_loc_info);
		}
		else
		{
			if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
				nodelist = exec_nodes->nodeList;
			else if (exec_type == EXEC_ON_COORDS)
				coordlist = exec_nodes->nodeList;

			primarynode = exec_nodes->primarynodelist;
		}
	}

	/* Set node list and DN number */
	if (list_length(nodelist) == 0 &&
		(exec_type == EXEC_ON_ALL_NODES ||
		 exec_type == EXEC_ON_DATANODES))
	{
		/* Primary connection is included in this number of connections if it exists */
		dn_conn_count = NumDataNodes;
	}
	else
	{
		if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
		{
			if (primarynode)
				dn_conn_count = list_length(nodelist) + 1;
			else
				dn_conn_count = list_length(nodelist);
		}
		else
			dn_conn_count = 0;
	}

	/* Set Coordinator list and Coordinator number */
	if ((list_length(nodelist) == 0 && exec_type == EXEC_ON_ALL_NODES) ||
		(list_length(coordlist) == 0 && exec_type == EXEC_ON_COORDS))
	{
		coordlist = GetAllCoordNodes();
		co_conn_count = list_length(coordlist);
	}
	else
	{
		if (exec_type == EXEC_ON_COORDS)
			co_conn_count = list_length(coordlist);
		else
			co_conn_count = 0;
	}

	/* Get other connections (non-primary) */
	pgxc_handles = get_handles(nodelist, coordlist, is_query_coord_only, is_global_session);
	if (!pgxc_handles)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Could not obtain connection from pool")));

	/* Get connection for primary node, if used */
	if (primarynode)
	{
		/* Let's assume primary connection is always a Datanode connection for the moment */
		PGXCNodeAllHandles *pgxc_conn_res;
		pgxc_conn_res = get_handles(primarynode, NULL, false, is_global_session);

		/* primary connection is unique */
		primaryconnection = pgxc_conn_res->datanode_handles[0];

		pfree(pgxc_conn_res);

		if (!primaryconnection)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Could not obtain connection from pool")));
		pgxc_handles->primary_handle = primaryconnection;
	}

	/* Depending on the execution type, we still need to save the initial node counts */
	pgxc_handles->dn_conn_count = dn_conn_count;
	pgxc_handles->co_conn_count = co_conn_count;

	return pgxc_handles;
}


static bool
pgxc_start_command_on_connection(PGXCNodeHandle *connection,
									RemoteQueryState *remotestate,
									Snapshot snapshot)
{
	CommandId	cid;
	ResponseCombiner *combiner = (ResponseCombiner *) remotestate;
	RemoteQuery	*step = (RemoteQuery *) combiner->ss.ps.plan;
	CHECK_OWNERSHIP(connection, combiner);

	elog(DEBUG5, "pgxc_start_command_on_connection - node %s, state %d",
			connection->nodename, connection->state);

	/*
	 * Scan descriptor would be valid and would contain a valid snapshot
	 * in cases when we need to send out of order command id to data node
	 * e.g. in case of a fetch
	 */
	cid = GetCurrentCommandId(false);

	if (pgxc_node_send_cmd_id(connection, cid) < 0 )
		return false;

	if (snapshot && pgxc_node_send_snapshot(connection, snapshot))
		return false;
	if (step->statement || step->cursor || remotestate->rqs_num_params)
	{
		/* need to use Extended Query Protocol */
		int	fetch = 0;
		bool	prepared = false;
		char	nodetype = PGXC_NODE_DATANODE;

		/* if prepared statement is referenced see if it is already
		 * exist */
		if (step->statement)
			prepared =
				ActivateDatanodeStatementOnNode(step->statement,
						PGXCNodeGetNodeId(connection->nodeoid,
							&nodetype));

		/*
		 * execute and fetch rows only if they will be consumed
		 * immediately by the sorter
		 */
		if (step->cursor)
			fetch = 1;

		combiner->extended_query = true;

		if (pgxc_node_send_query_extended(connection,
							prepared ? NULL : step->sql_statement,
							step->statement,
							step->cursor,
							remotestate->rqs_num_params,
							remotestate->rqs_param_types,
							remotestate->paramval_len,
							remotestate->paramval_data,
							step->has_row_marks ? true : step->read_only,
							fetch) != 0)
			return false;
	}
	else
	{
		combiner->extended_query = false;
		if (pgxc_node_send_query(connection, step->sql_statement) != 0)
			return false;
	}
	return true;
}

/*
 * Execute utility statement on multiple Datanodes
 * It does approximately the same as
 *
 * RemoteQueryState *state = ExecInitRemoteQuery(plan, estate, flags);
 * Assert(TupIsNull(ExecRemoteQuery(state));
 * ExecEndRemoteQuery(state)
 *
 * But does not need an Estate instance and does not do some unnecessary work,
 * like allocating tuple slots.
 */
void
ExecRemoteUtility(RemoteQuery *node)
{
	RemoteQueryState *remotestate;
	ResponseCombiner *combiner;
	bool		force_autocommit = node->force_autocommit;
	RemoteQueryExecType exec_type = node->exec_type;
	GlobalTransactionId gxid = InvalidGlobalTransactionId;
	Snapshot snapshot = NULL;
	PGXCNodeAllHandles *pgxc_connections;
	int			co_conn_count;
	int			dn_conn_count;
	bool		need_tran_block;
	ExecDirectType		exec_direct_type = node->exec_direct_type;
	int			i;
	CommandId	cid = GetCurrentCommandId(true);	

	if (!force_autocommit)
		RegisterTransactionLocalNode(true);

	remotestate = makeNode(RemoteQueryState);
	combiner = (ResponseCombiner *)remotestate;
	InitResponseCombiner(combiner, 0, node->combine_type);

	/*
	 * Do not set global_session if it is a utility statement. 
	 * Avoids CREATE NODE error on cluster configuration.
	 */
	pgxc_connections = get_exec_connections(NULL, node->exec_nodes, exec_type, 
											exec_direct_type != EXEC_DIRECT_UTILITY);

	dn_conn_count = pgxc_connections->dn_conn_count;
	co_conn_count = pgxc_connections->co_conn_count;
	/* exit right away if no nodes to run command on */
	if (dn_conn_count == 0 && co_conn_count == 0)
	{
		pfree_pgxc_all_handles(pgxc_connections);
		return;
	}

	if (force_autocommit)
		need_tran_block = false;
	else
		need_tran_block = true;

	/* Commands launched through EXECUTE DIRECT do not need start a transaction */
	if (exec_direct_type == EXEC_DIRECT_UTILITY)
	{
		need_tran_block = false;

		/* This check is not done when analyzing to limit dependencies */
		if (IsTransactionBlock())
			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("cannot run EXECUTE DIRECT with utility inside a transaction block")));
	}

	gxid = GetCurrentTransactionId();
	if (ActiveSnapshotSet())
		snapshot = GetActiveSnapshot();
	if (!GlobalTransactionIdIsValid(gxid))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to get next transaction ID")));

	{
		if (pgxc_node_begin(dn_conn_count, pgxc_connections->datanode_handles,
					gxid, need_tran_block, false, PGXC_NODE_DATANODE))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Could not begin transaction on Datanodes")));
		for (i = 0; i < dn_conn_count; i++)
		{
			PGXCNodeHandle *conn = pgxc_connections->datanode_handles[i];

			if (conn->state == DN_CONNECTION_STATE_QUERY)
				BufferConnection(conn);
			if (snapshot && pgxc_node_send_snapshot(conn, snapshot))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send snapshot to Datanodes")));
			}
			if (pgxc_node_send_cmd_id(conn, cid) < 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command ID to Datanodes")));
			}

			if (pgxc_node_send_query(conn, node->sql_statement) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to Datanodes")));
			}
		}
	}

	{
		if (pgxc_node_begin(co_conn_count, pgxc_connections->coord_handles,
					gxid, need_tran_block, false, PGXC_NODE_COORDINATOR))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Could not begin transaction on coordinators")));
		/* Now send it to Coordinators if necessary */
		for (i = 0; i < co_conn_count; i++)
		{
			if (snapshot && pgxc_node_send_snapshot(pgxc_connections->coord_handles[i], snapshot))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to coordinators")));
			}
			if (pgxc_node_send_cmd_id(pgxc_connections->coord_handles[i], cid) < 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command ID to Datanodes")));
			}

			if (pgxc_node_send_query(pgxc_connections->coord_handles[i], node->sql_statement) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to coordinators")));
			}
		}
	}

	/*
	 * Stop if all commands are completed or we got a data row and
	 * initialized state node for subsequent invocations
	 */
	{
		while (dn_conn_count > 0)
		{
			int i = 0;

			if (pgxc_node_receive(dn_conn_count, pgxc_connections->datanode_handles, NULL))
				break;
			/*
			 * Handle input from the Datanodes.
			 * We do not expect Datanodes returning tuples when running utility
			 * command.
			 * If we got EOF, move to the next connection, will receive more
			 * data on the next iteration.
			 */
			while (i < dn_conn_count)
			{
				PGXCNodeHandle *conn = pgxc_connections->datanode_handles[i];
				int res = handle_response(conn, combiner);
				if (res == RESPONSE_EOF)
				{
					i++;
				}
				else if (res == RESPONSE_COMPLETE)
				{
					/* Ignore, wait for ReadyForQuery */
				}
				else if (res == RESPONSE_ERROR)
				{
					/* Ignore, wait for ReadyForQuery */
				}
				else if (res == RESPONSE_READY)
				{
					if (i < --dn_conn_count)
						pgxc_connections->datanode_handles[i] =
							pgxc_connections->datanode_handles[dn_conn_count];
				}
				else if (res == RESPONSE_TUPDESC)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Unexpected response from Datanode")));
				}
				else if (res == RESPONSE_DATAROW)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Unexpected response from Datanode")));
				}
			}
		}
	}

	/* Make the same for Coordinators */
	{
		while (co_conn_count > 0)
		{
			int i = 0;

			if (pgxc_node_receive(co_conn_count, pgxc_connections->coord_handles, NULL))
				break;

			while (i < co_conn_count)
			{
				int res = handle_response(pgxc_connections->coord_handles[i], combiner);
				if (res == RESPONSE_EOF)
				{
					i++;
				}
				else if (res == RESPONSE_COMPLETE)
				{
					/* Ignore, wait for ReadyForQuery */
				}
				else if (res == RESPONSE_ERROR)
				{
					/* Ignore, wait for ReadyForQuery */
				}
				else if (res == RESPONSE_READY)
				{
					if (i < --co_conn_count)
						pgxc_connections->coord_handles[i] =
							 pgxc_connections->coord_handles[co_conn_count];
				}
				else if (res == RESPONSE_TUPDESC)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Unexpected response from coordinator")));
				}
				else if (res == RESPONSE_DATAROW)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Unexpected response from coordinator")));
				}
			}
		}
	}

	/*
	 * We have processed all responses from nodes and if we have
	 * error message pending we can report it. All connections should be in
	 * consistent state now and so they can be released to the pool after ROLLBACK.
	 */
	pfree_pgxc_all_handles(pgxc_connections);
	pgxc_node_report_error(combiner);
}


/*
 * Called when the backend is ending.
 */
void
PGXCNodeCleanAndRelease(int code, Datum arg)
{

	/* Disconnect from Pooler, if any connection is still held Pooler close it */
	PoolManagerDisconnect();

	/* Close connection with GTM */
	CloseGTM();

	/* Dump collected statistics to the log */
	stat_log();
}

void
ExecCloseRemoteStatement(const char *stmt_name, List *nodelist)
{
	PGXCNodeAllHandles *all_handles;
	PGXCNodeHandle	  **connections;
	ResponseCombiner	combiner;
	int					conn_count;
	int 				i;

	/* Exit if nodelist is empty */
	if (list_length(nodelist) == 0)
		return;

	/* get needed Datanode connections */
	all_handles = get_handles(nodelist, NIL, false, true);
	conn_count = all_handles->dn_conn_count;
	connections = all_handles->datanode_handles;

	for (i = 0; i < conn_count; i++)
	{
		if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
			BufferConnection(connections[i]);
		if (pgxc_node_send_close(connections[i], true, stmt_name) != 0)
		{
			/*
			 * statements are not affected by statement end, so consider
			 * unclosed statement on the Datanode as a fatal issue and
			 * force connection is discarded
			 */
			PGXCNodeSetConnectionState(connections[i],
					DN_CONNECTION_STATE_ERROR_FATAL);
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode statemrnt")));
		}
		if (pgxc_node_send_sync(connections[i]) != 0)
		{
			PGXCNodeSetConnectionState(connections[i],
					DN_CONNECTION_STATE_ERROR_FATAL);
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode statement")));
		}
		PGXCNodeSetConnectionState(connections[i], DN_CONNECTION_STATE_CLOSE);
	}

	InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
	/*
	 * Make sure there are zeroes in unused fields
	 */
	memset(&combiner, 0, sizeof(ScanState));

	while (conn_count > 0)
	{
		if (pgxc_node_receive(conn_count, connections, NULL))
		{
			for (i = 0; i < conn_count; i++)
				PGXCNodeSetConnectionState(connections[i],
						DN_CONNECTION_STATE_ERROR_FATAL);

			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close Datanode statement")));
		}
		i = 0;
		while (i < conn_count)
		{
			int res = handle_response(connections[i], &combiner);
			if (res == RESPONSE_EOF)
			{
				i++;
			}
			else if (res == RESPONSE_READY ||
					connections[i]->state == DN_CONNECTION_STATE_ERROR_FATAL)
			{
				if (--conn_count > i)
					connections[i] = connections[conn_count];
			}
		}
	}

	ValidateAndCloseCombiner(&combiner);
	pfree_pgxc_all_handles(all_handles);
}

/*
 * DataNodeCopyInBinaryForAll
 *
 * In a COPY TO, send to all Datanodes PG_HEADER for a COPY TO in binary mode.
 */
int
DataNodeCopyInBinaryForAll(char *msg_buf, int len, int conn_count,
									  PGXCNodeHandle** connections)
{
	int 		i;
	int msgLen = 4 + len + 1;
	int nLen = htonl(msgLen);

	for (i = 0; i < conn_count; i++)
	{
		PGXCNodeHandle *handle = connections[i];
		if (handle->state == DN_CONNECTION_STATE_COPY_IN)
		{
			/* msgType + msgLen */
			if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
			{
				ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					errmsg("out of memory")));
			}

			handle->outBuffer[handle->outEnd++] = 'd';
			memcpy(handle->outBuffer + handle->outEnd, &nLen, 4);
			handle->outEnd += 4;
			memcpy(handle->outBuffer + handle->outEnd, msg_buf, len);
			handle->outEnd += len;
			handle->outBuffer[handle->outEnd++] = '\n';
		}
		else
		{
			add_error_message(handle, "Invalid Datanode connection");
			return EOF;
		}
	}

	return 0;
}

/*
 * Encode parameter values to format of DataRow message (the same format is
 * used in Bind) to prepare for sending down to Datanodes.
 * The data row is copied to RemoteQueryState.paramval_data.
 */
void
SetDataRowForExtParams(ParamListInfo paraminfo, RemoteQueryState *rq_state)
{
	StringInfoData buf;
	uint16 n16;
	int i;
	int real_num_params = 0;
	RemoteQuery *node = (RemoteQuery*) rq_state->combiner.ss.ps.plan;

	/* If there are no parameters, there is no data to BIND. */
	if (!paraminfo)
		return;

	Assert(!rq_state->paramval_data);

	/*
	 * It is necessary to fetch parameters
	 * before looking at the output value.
	 */
	for (i = 0; i < paraminfo->numParams; i++)
	{
		ParamExternData *param;

		param = &paraminfo->params[i];

		if (!OidIsValid(param->ptype) && paraminfo->paramFetch != NULL)
			(*paraminfo->paramFetch) (paraminfo, i + 1);

		/*
		 * This is the last parameter found as useful, so we need
		 * to include all the previous ones to keep silent the remote
		 * nodes. All the parameters prior to the last usable having no
		 * type available will be considered as NULL entries.
		 */
		if (OidIsValid(param->ptype))
			real_num_params = i + 1;
	}

	/*
	 * If there are no parameters available, simply leave.
	 * This is possible in the case of a query called through SPI
	 * and using no parameters.
	 */
	if (real_num_params == 0)
	{
		rq_state->paramval_data = NULL;
		rq_state->paramval_len = 0;
		return;
	}

	initStringInfo(&buf);

	/* Number of parameter values */
	n16 = htons(real_num_params);
	appendBinaryStringInfo(&buf, (char *) &n16, 2);

	/* Parameter values */
	for (i = 0; i < real_num_params; i++)
	{
		ParamExternData *param = &paraminfo->params[i];
		uint32 n32;

		/*
		 * Parameters with no types are considered as NULL and treated as integer
		 * The same trick is used for dropped columns for remote DML generation.
		 */
		if (param->isnull || !OidIsValid(param->ptype))
		{
			n32 = htonl(-1);
			appendBinaryStringInfo(&buf, (char *) &n32, 4);
		}
		else
		{
			Oid		typOutput;
			bool	typIsVarlena;
			Datum	pval;
			char   *pstring;
			int		len;

			/* Get info needed to output the value */
			getTypeOutputInfo(param->ptype, &typOutput, &typIsVarlena);

			/*
			 * If we have a toasted datum, forcibly detoast it here to avoid
			 * memory leakage inside the type's output routine.
			 */
			if (typIsVarlena)
				pval = PointerGetDatum(PG_DETOAST_DATUM(param->value));
			else
				pval = param->value;

			/* Convert Datum to string */
			pstring = OidOutputFunctionCall(typOutput, pval);

			/* copy data to the buffer */
			len = strlen(pstring);
			n32 = htonl(len);
			appendBinaryStringInfo(&buf, (char *) &n32, 4);
			appendBinaryStringInfo(&buf, pstring, len);
		}
	}


	/*
	 * If parameter types are not already set, infer them from
	 * the paraminfo.
	 */
	if (node->rq_num_params > 0)
	{
		/*
		 * Use the already known param types for BIND. Parameter types
		 * can be already known when the same plan is executed multiple
		 * times.
		 */
		if (node->rq_num_params != real_num_params)
			elog(ERROR, "Number of user-supplied parameters do not match "
						"the number of remote parameters");
		rq_state->rqs_num_params = node->rq_num_params;
		rq_state->rqs_param_types = node->rq_param_types;
	}
	else
	{
		rq_state->rqs_num_params = real_num_params;
		rq_state->rqs_param_types = (Oid *) palloc(sizeof(Oid) * real_num_params);
		for (i = 0; i < real_num_params; i++)
			rq_state->rqs_param_types[i] = paraminfo->params[i].ptype;
	}

	/* Assign the newly allocated data row to paramval */
	rq_state->paramval_data = buf.data;
	rq_state->paramval_len = buf.len;
}

/*
 * Clear per transaction remote information
 */
void
AtEOXact_Remote(void)
{
	PGXCNodeResetParams(true);
}

/*
 * Invoked when local transaction is about to be committed.
 * If nodestring is specified commit specified prepared transaction on remote
 * nodes, otherwise commit remote nodes which are in transaction.
 */
void
PreCommit_Remote(char *prepareGID, char *nodestring, bool preparedLocalNode)
{
	struct rusage		start_r;
	struct timeval		start_t;

	if (log_gtm_stats)
		ResetUsageCommon(&start_r, &start_t);

	/*
	 * Made node connections persistent if we are committing transaction
	 * that touched temporary tables. We never drop that flag, so after some
	 * transaction has created a temp table the session's remote connections
	 * become persistent.
	 * We do not need to set that flag if transaction that has created a temp
	 * table finally aborts - remote connections are not holding temporary
	 * objects in this case.
	 */
	if (IS_PGXC_LOCAL_COORDINATOR && MyXactAccessedTempRel)
		temp_object_included = true;


	/*
	 * OK, everything went fine. At least one remote node is in PREPARED state
	 * and the transaction is successfully prepared on all the involved nodes.
	 * Now we are ready to commit the transaction. We need a new GXID to send
	 * down the remote nodes to execute the forthcoming COMMIT PREPARED
	 * command. So grab one from the GTM and track it. It will be closed along
	 * with the main transaction at the end.
	 */
	if (nodestring)
	{
		Assert(preparedLocalNode);
		pgxc_node_remote_finish(prepareGID, true, nodestring,
								GetAuxilliaryTransactionId(),
								GetTopGlobalTransactionId());

	}
	else
		pgxc_node_remote_commit();

	if (log_gtm_stats)
		ShowUsageCommon("PreCommit_Remote", &start_r, &start_t);
}

/*
 * Do abort processing for the transaction. We must abort the transaction on
 * all the involved nodes. If a node has already prepared a transaction, we run
 * ROLLBACK PREPARED command on the node. Otherwise, a simple ROLLBACK command
 * is sufficient.
 *
 * We must guard against the case when a transaction is prepared succefully on
 * all the nodes and some error occurs after we send a COMMIT PREPARED message
 * to at lease one node. Such a transaction must not be aborted to preserve
 * global consistency. We handle this case by recording the nodes involved in
 * the transaction at the GTM and keep the transaction open at the GTM so that
 * its reported as "in-progress" on all the nodes until resolved
 */
bool
PreAbort_Remote(void)
{
	/*
	 * We are about to abort current transaction, and there could be an
	 * unexpected error leaving the node connection in some state requiring
	 * clean up, like COPY or pending query results.
	 * If we are running copy we should send down CopyFail message and read
	 * all possible incoming messages, there could be copy rows (if running
	 * COPY TO) ErrorResponse, ReadyForQuery.
	 * If there are pending results (connection state is DN_CONNECTION_STATE_QUERY)
	 * we just need to read them in and discard, all necessary commands are
	 * already sent. The end of input could be CommandComplete or
	 * PortalSuspended, in either case subsequent ROLLBACK closes the portal.
	 */
	PGXCNodeAllHandles *all_handles;
	PGXCNodeHandle	   *clean_nodes[NumCoords + NumDataNodes];
	int					node_count = 0;
	int					cancel_dn_count = 0, cancel_co_count = 0;
	int					cancel_dn_list[NumDataNodes];
	int					cancel_co_list[NumCoords];
	int 				i;
	struct rusage		start_r;
	struct timeval		start_t;

	if (log_gtm_stats)
		ResetUsageCommon(&start_r, &start_t);

	all_handles = get_current_handles();
	/*
	 * Find "dirty" coordinator connections.
	 * COPY is never running on a coordinator connections, we just check for
	 * pending data.
	 */
	for (i = 0; i < all_handles->co_conn_count; i++)
	{
		PGXCNodeHandle *handle = all_handles->coord_handles[i];

		if (handle->state == DN_CONNECTION_STATE_QUERY)
		{
			/*
			 * Forget previous combiner if any since input will be handled by
			 * different one.
			 */
			handle->combiner = NULL;
			clean_nodes[node_count++] = handle;
			cancel_co_list[cancel_co_count++] = i;
		}
	}

	/*
	 * The same for data nodes, but cancel COPY if it is running.
	 */
	for (i = 0; i < all_handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *handle = all_handles->datanode_handles[i];

		if (handle->state == DN_CONNECTION_STATE_QUERY)
		{
			/*
			 * Forget previous combiner if any since input will be handled by
			 * different one.
			 */
			handle->combiner = NULL;
			clean_nodes[node_count++] = handle;
			cancel_dn_list[cancel_dn_count++] = i;
		}
		else if (handle->state == DN_CONNECTION_STATE_COPY_IN ||
				handle->state == DN_CONNECTION_STATE_COPY_OUT)
		{
			DataNodeCopyEnd(handle, true);
			/*
			 * Forget previous combiner if any since input will be handled by
			 * different one.
			 */
			handle->combiner = NULL;
			clean_nodes[node_count++] = handle;
			cancel_dn_list[cancel_dn_count++] = i;
		}
	}

	/*
	 * Cancel running queries on the datanodes and the coordinators.
	 */
	PoolManagerCancelQuery(cancel_dn_count, cancel_dn_list, cancel_co_count,
			cancel_co_list);

	/*
	 * Now read and discard any data from the connections found "dirty"
	 */
	if (node_count > 0)
	{
		ResponseCombiner combiner;

		InitResponseCombiner(&combiner, node_count, COMBINE_TYPE_NONE);
		/*
		 * Make sure there are zeroes in unused fields
		 */
		memset(&combiner, 0, sizeof(ScanState));
		combiner.connections = clean_nodes;
		combiner.conn_count = node_count;
		combiner.request_type = REQUEST_TYPE_ERROR;

		pgxc_connections_cleanup(&combiner);

		/* prevent pfree'ing local variable */
		combiner.connections = NULL;

		CloseCombiner(&combiner);
	}

	pgxc_node_remote_abort();

	/*
	 * Drop the connections to ensure aborts are handled properly.
	 *
	 * XXX We should really be consulting PersistentConnections parameter and
	 * keep the connections if its set. But as a short term measure, to address
	 * certain issues for aborted transactions, we drop the connections.
	 * Revisit and fix the issue
	 */
	elog(DEBUG5, "temp_object_included %d", temp_object_included);
	if (!temp_object_included)
	{
		/* Clean up remote sessions */
		pgxc_node_remote_cleanup_all();
		release_handles();
	}

	pfree_pgxc_all_handles(all_handles);

	if (log_gtm_stats)
		ShowUsageCommon("PreAbort_Remote", &start_r, &start_t);

	return true;
}


/*
 * Invoked when local transaction is about to be prepared.
 * If invoked on a Datanode just commit transaction on remote connections,
 * since secondary sessions are read only and never need to be prepared.
 * Otherwise run PREPARE on remote connections, where writable commands were
 * sent (connections marked as not read-only).
 * If that is explicit PREPARE (issued by client) notify GTM.
 * In case of implicit PREPARE not involving local node (ex. caused by
 * INSERT, UPDATE or DELETE) commit prepared transaction immediately.
 * Return list of node names where transaction was actually prepared, include
 * the name of the local node if localNode is true.
 */
char *
PrePrepare_Remote(char *prepareGID, bool localNode, bool implicit)
{
	/* Always include local node if running explicit prepare */
	char *nodestring;
	struct rusage		start_r;
	struct timeval		start_t;

	if (log_gtm_stats)
		ResetUsageCommon(&start_r, &start_t);

	/*
	 * Primary session is doing 2PC, just commit secondary processes and exit
	 */
	if (IS_PGXC_DATANODE)
	{
		pgxc_node_remote_commit();
		return NULL;
	}

	nodestring = pgxc_node_remote_prepare(prepareGID,
												!implicit || localNode);

	if (!implicit && IS_PGXC_LOCAL_COORDINATOR)
		/* Save the node list and gid on GTM. */
		StartPreparedTranGTM(GetTopGlobalTransactionId(), prepareGID,
							 nodestring);

	/*
	 * If no need to commit on local node go ahead and commit prepared
	 * transaction right away.
	 */
	if (implicit && !localNode && nodestring)
	{
		pgxc_node_remote_finish(prepareGID, true, nodestring,
								GetAuxilliaryTransactionId(),
								GetTopGlobalTransactionId());
		pfree(nodestring);
		nodestring = NULL;
	}

	if (log_gtm_stats)
		ShowUsageCommon("PrePrepare_Remote", &start_r, &start_t);

	return nodestring;
}

/*
 * Invoked immediately after local node is prepared.
 * Notify GTM about completed prepare.
 */
void
PostPrepare_Remote(char *prepareGID, bool implicit)
{
	struct rusage		start_r;
	struct timeval		start_t;

	if (log_gtm_stats)
		ResetUsageCommon(&start_r, &start_t);

	if (!implicit)
		PrepareTranGTM(GetTopGlobalTransactionId());

	if (log_gtm_stats)
		ShowUsageCommon("PostPrepare_Remote", &start_r, &start_t);
}

/*
 * Returns true if 2PC is required for consistent commit: if there was write
 * activity on two or more nodes within current transaction.
 */
bool
IsTwoPhaseCommitRequired(bool localWrite)
{
	PGXCNodeAllHandles *handles;
	bool				found = localWrite;
	int 				i;

	/* Never run 2PC on Datanode-to-Datanode connection */
	if (IS_PGXC_DATANODE)
		return false;

	if (MyXactAccessedTempRel)
	{
		elog(DEBUG1, "Transaction accessed temporary objects - "
				"2PC will not be used and that can lead to data inconsistencies "
				"in case of failures");
		return false;
	}

	/*
	 * If no XID assigned, no need to run 2PC since neither coordinator nor any
	 * remote nodes did write operation
	 */
	if (!TransactionIdIsValid(GetTopTransactionIdIfAny()))
		return false;

	handles = get_current_handles();
	for (i = 0; i < handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->datanode_handles[i];
		if (conn->sock != NO_SOCKET && !conn->read_only &&
				conn->transaction_status == 'T')
		{
			if (found)
				return true; /* second found */
			else
				found = true; /* first found */
		}
	}
	for (i = 0; i < handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = handles->coord_handles[i];
		if (conn->sock != NO_SOCKET && !conn->read_only &&
				conn->transaction_status == 'T')
		{
			if (found)
				return true; /* second found */
			else
				found = true; /* first found */
		}
	}
	return false;
}

/*
 * Execute COMMIT/ABORT PREPARED issued by the remote client on remote nodes.
 * Contacts GTM for the list of involved nodes and for work complete
 * notification. Returns true if prepared transaction on local node needs to be
 * finished too.
 */
bool
FinishRemotePreparedTransaction(char *prepareGID, bool commit)
{
	char				   *nodestring;
	GlobalTransactionId		gxid, prepare_gxid;
	bool					prepared_local = false;

	/*
	 * Get the list of nodes involved in this transaction.
	 *
	 * This function returns the GXID of the prepared transaction. It also
	 * returns a fresh GXID which can be used for running COMMIT PREPARED
	 * commands on the remote nodes. Both these GXIDs can then be either
	 * committed or aborted together.
	 *
	 * XXX While I understand that we get the prepared and a new GXID with a
	 * single call, it doesn't look nicer and create confusion. We should
	 * probably split them into two parts. This is used only for explicit 2PC
	 * which should not be very common in XC
	 *
	 * In xc_maintenance_mode mode, we don't fail if the GTM does not have
	 * knowledge about the prepared transaction. That may happen for various
	 * reasons such that an earlier attempt cleaned up it from GTM or GTM was
	 * restarted in between. The xc_maintenance_mode is a kludge to come out of
	 * such situations. So it seems alright to not be too strict about the
	 * state
	 */
	if ((GetGIDDataGTM(prepareGID, &gxid, &prepare_gxid, &nodestring) < 0) &&
		!xc_maintenance_mode)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("prepared transaction with identifier \"%s\" does not exist",
						prepareGID)));

	/*
	 * Please note that with xc_maintenance_mode = on, COMMIT/ROLLBACK PREPARED will not
	 * propagate to remote nodes. Only GTM status is cleaned up.
	 */
	if (xc_maintenance_mode)
	{
		if (commit)
		{
			pgxc_node_remote_commit();
			CommitPreparedTranGTM(prepare_gxid, gxid, 0, NULL);
		}
		else
		{
			pgxc_node_remote_abort();
			RollbackTranGTM(prepare_gxid);
			RollbackTranGTM(gxid);
		}
		return false;
	}

	prepared_local = pgxc_node_remote_finish(prepareGID, commit, nodestring,
											 gxid, prepare_gxid);

	if (commit)
	{
		/*
		 * XXX For explicit 2PC, there will be enough delay for any
		 * waited-committed transactions to send a final COMMIT message to the
		 * GTM.
		 */
		CommitPreparedTranGTM(prepare_gxid, gxid, 0, NULL);
	}
	else
	{
		RollbackTranGTM(prepare_gxid);
		RollbackTranGTM(gxid);
	}

	return prepared_local;
}


/*
 * Complete previously prepared transactions on remote nodes.
 * Release remote connection after completion.
 */
static bool
pgxc_node_remote_finish(char *prepareGID, bool commit,
						char *nodestring, GlobalTransactionId gxid,
						GlobalTransactionId prepare_gxid)
{
	char			   *finish_cmd;
	PGXCNodeHandle	   *connections[MaxCoords + MaxDataNodes];
	int					conn_count = 0;
	ResponseCombiner	combiner;
	PGXCNodeAllHandles *pgxc_handles;
	bool				prepared_local = false;
	char			   *nodename;
	List			   *nodelist = NIL;
	List			   *coordlist = NIL;
	int					i;
	/*
	 * Now based on the nodestring, run COMMIT/ROLLBACK PREPARED command on the
	 * remote nodes and also finish the transaction locally is required
	 */
	nodename = strtok(nodestring, ",");
	while (nodename != NULL)
	{
		int		nodeIndex;
		char	nodetype;

		/* Get node type and index */
		nodetype = PGXC_NODE_NONE;
		nodeIndex = PGXCNodeGetNodeIdFromName(nodename, &nodetype);
		if (nodetype == PGXC_NODE_NONE)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("PGXC Node %s: object not defined",
							nodename)));

		/* Check if node is requested is the self-node or not */
		if (nodetype == PGXC_NODE_COORDINATOR)
		{
			if (nodeIndex == PGXCNodeId - 1)
				prepared_local = true;
			else
				coordlist = lappend_int(coordlist, nodeIndex);
		}
		else
			nodelist = lappend_int(nodelist, nodeIndex);

		nodename = strtok(NULL, ",");
	}

	if (nodelist == NIL && coordlist == NIL)
		return prepared_local;

	pgxc_handles = get_handles(nodelist, coordlist, false, true);

	finish_cmd = (char *) palloc(64 + strlen(prepareGID));

	if (commit)
		sprintf(finish_cmd, "COMMIT PREPARED '%s'", prepareGID);
	else
		sprintf(finish_cmd, "ROLLBACK PREPARED '%s'", prepareGID);

	for (i = 0; i < pgxc_handles->dn_conn_count; i++)
	{
		PGXCNodeHandle *conn = pgxc_handles->datanode_handles[i];

		if (pgxc_node_send_gxid(conn, gxid))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("failed to send GXID for %s PREPARED command",
							commit ? "COMMIT" : "ROLLBACK")));
		}

		if (pgxc_node_send_query(conn, finish_cmd))
		{
			/*
			 * Do not bother with clean up, just bomb out. The error handler
			 * will invoke RollbackTransaction which will do the work.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("failed to send %s PREPARED command to the node %u",
							commit ? "COMMIT" : "ROLLBACK", conn->nodeoid)));
		}
		else
		{
			/* Read responses from these */
			connections[conn_count++] = conn;
		}
	}

	for (i = 0; i < pgxc_handles->co_conn_count; i++)
	{
		PGXCNodeHandle *conn = pgxc_handles->coord_handles[i];

		if (pgxc_node_send_gxid(conn, gxid))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("failed to send GXID for %s PREPARED command",
							commit ? "COMMIT" : "ROLLBACK")));
		}

		if (pgxc_node_send_query(conn, finish_cmd))
		{
			/*
			 * Do not bother with clean up, just bomb out. The error handler
			 * will invoke RollbackTransaction which will do the work.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("failed to send %s PREPARED command to the node %u",
							commit ? "COMMIT" : "ROLLBACK", conn->nodeoid)));
		}
		else
		{
			/* Read responses from these */
			connections[conn_count++] = conn;
		}
	}

	if (conn_count)
	{
		InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
		/* Receive responses */
		if (pgxc_node_receive_responses(conn_count, connections, NULL, &combiner) ||
				!validate_combiner(&combiner))
		{
			if (combiner.errorMessage)
				pgxc_node_report_error(&combiner);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to COMMIT the transaction on one or more nodes")));
		}
		else
			CloseCombiner(&combiner);
	}

	if (!temp_object_included && !PersistentConnections)
	{
		/* Clean up remote sessions */
		pgxc_node_remote_cleanup_all();
		release_handles();
	}

	pfree_pgxc_all_handles(pgxc_handles);
	pfree(finish_cmd);

	return prepared_local;
}

/*****************************************************************************
 *
 * Simplified versions of ExecInitRemoteQuery, ExecRemoteQuery and
 * ExecEndRemoteQuery: in XCP they are only used to execute simple queries.
 *
 *****************************************************************************/
RemoteQueryState *
ExecInitRemoteQuery(RemoteQuery *node, EState *estate, int eflags)
{
	RemoteQueryState   *remotestate;
	ResponseCombiner   *combiner;

	remotestate = makeNode(RemoteQueryState);
	combiner = (ResponseCombiner *) remotestate;
	InitResponseCombiner(combiner, 0, node->combine_type);
	combiner->ss.ps.plan = (Plan *) node;
	combiner->ss.ps.state = estate;

	combiner->ss.ps.qual = NIL;

	combiner->request_type = REQUEST_TYPE_QUERY;

	ExecInitResultTupleSlot(estate, &combiner->ss.ps);
	ExecAssignResultTypeFromTL((PlanState *) remotestate);

	/*
	 * If there are parameters supplied, get them into a form to be sent to the
	 * Datanodes with bind message. We should not have had done this before.
	 */
	SetDataRowForExtParams(estate->es_param_list_info, remotestate);

	/* We need expression context to evaluate */
	if (node->exec_nodes && node->exec_nodes->en_expr)
	{
		Expr *expr = node->exec_nodes->en_expr;

		if (IsA(expr, Var) && ((Var *) expr)->vartype == TIDOID)
		{
			/* Special case if expression does not need to be evaluated */
		}
		else
		{
			/* prepare expression evaluation */
			ExecAssignExprContext(estate, &combiner->ss.ps);
		}
	}

	return remotestate;
}


/*
 * Execute step of PGXC plan.
 * The step specifies a command to be executed on specified nodes.
 * On first invocation connections to the data nodes are initialized and
 * command is executed. Further, as well as within subsequent invocations,
 * responses are received until step is completed or there is a tuple to emit.
 * If there is a tuple it is returned, otherwise returned NULL. The NULL result
 * from the function indicates completed step.
 * The function returns at most one tuple per invocation.
 */
TupleTableSlot *
ExecRemoteQuery(RemoteQueryState *node)
{
	ResponseCombiner *combiner = (ResponseCombiner *) node;
	RemoteQuery    *step = (RemoteQuery *) combiner->ss.ps.plan;
	TupleTableSlot *resultslot = combiner->ss.ps.ps_ResultTupleSlot;

	if (!node->query_Done)
	{
		GlobalTransactionId gxid = InvalidGlobalTransactionId;
		Snapshot		snapshot = GetActiveSnapshot();
		PGXCNodeHandle **connections = NULL;
		PGXCNodeHandle *primaryconnection = NULL;
		int				i;
		int				regular_conn_count = 0;
		int				total_conn_count = 0;
		bool			need_tran_block;
		PGXCNodeAllHandles *pgxc_connections;

		/*
		 * Get connections for Datanodes only, utilities and DDLs
		 * are launched in ExecRemoteUtility
		 */
		pgxc_connections = get_exec_connections(node, step->exec_nodes,
												step->exec_type,
												true);

		if (step->exec_type == EXEC_ON_DATANODES)
		{
			connections = pgxc_connections->datanode_handles;
			total_conn_count = regular_conn_count = pgxc_connections->dn_conn_count;
		}
		else if (step->exec_type == EXEC_ON_COORDS)
		{
			connections = pgxc_connections->coord_handles;
			total_conn_count = regular_conn_count = pgxc_connections->co_conn_count;
		}

		primaryconnection = pgxc_connections->primary_handle;

		/*
		 * Primary connection is counted separately but is included in total_conn_count if used.
		 */
		if (primaryconnection)
			regular_conn_count--;

		/*
		 * We save only regular connections, at the time we exit the function
		 * we finish with the primary connection and deal only with regular
		 * connections on subsequent invocations
		 */
		combiner->node_count = regular_conn_count;

		/*
		 * Start transaction on data nodes if we are in explicit transaction
		 * or going to use extended query protocol or write to multiple nodes
		 */
		if (step->force_autocommit)
			need_tran_block = false;
		else
			need_tran_block = step->cursor ||
					(!step->read_only && total_conn_count > 1) ||
					(TransactionBlockStatusCode() == 'T');

		stat_statement();
		stat_transaction(total_conn_count);

		gxid = GetCurrentTransactionIdIfAny();
		/* See if we have a primary node, execute on it first before the others */
		if (primaryconnection)
		{
			if (pgxc_node_begin(1, &primaryconnection, gxid, need_tran_block,
								step->read_only, PGXC_NODE_DATANODE))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Could not begin transaction on data node.")));

			/* If explicit transaction is needed gxid is already sent */
			if (!pgxc_start_command_on_connection(primaryconnection, node, snapshot))
			{
				pgxc_node_remote_abort();
				pfree_pgxc_all_handles(pgxc_connections);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
			Assert(combiner->combine_type == COMBINE_TYPE_SAME);

			pgxc_node_receive(1, &primaryconnection, NULL);
			/* Make sure the command is completed on the primary node */
			while (true)
			{
				int res = handle_response(primaryconnection, combiner);
				if (res == RESPONSE_READY)
					break;
				else if (res == RESPONSE_EOF)
					pgxc_node_receive(1, &primaryconnection, NULL);
				else if (res == RESPONSE_COMPLETE || res == RESPONSE_ERROR)
				    /* Get ReadyForQuery */
					continue;
				else if (res == RESPONSE_ASSIGN_GXID)
					continue;
				else
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Unexpected response from data node")));
			}
			if (combiner->errorMessage)
				pgxc_node_report_error(combiner);
		}

		for (i = 0; i < regular_conn_count; i++)
		{
			if (pgxc_node_begin(1, &connections[i], gxid, need_tran_block,
								step->read_only, PGXC_NODE_DATANODE))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Could not begin transaction on data node.")));

			/* If explicit transaction is needed gxid is already sent */
			if (!pgxc_start_command_on_connection(connections[i], node, snapshot))
			{
				pgxc_node_remote_abort();
				pfree_pgxc_all_handles(pgxc_connections);
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to send command to data nodes")));
			}
			connections[i]->combiner = combiner;
		}

		if (step->cursor)
		{
			combiner->cursor = step->cursor;
			combiner->cursor_count = regular_conn_count;
			combiner->cursor_connections = (PGXCNodeHandle **) palloc(regular_conn_count * sizeof(PGXCNodeHandle *));
			memcpy(combiner->cursor_connections, connections, regular_conn_count * sizeof(PGXCNodeHandle *));
		}

		combiner->connections = connections;
		combiner->conn_count = regular_conn_count;
		combiner->current_conn = 0;

		if (combiner->cursor_count)
		{
			combiner->conn_count = combiner->cursor_count;
			memcpy(connections, combiner->cursor_connections,
				   combiner->cursor_count * sizeof(PGXCNodeHandle *));
			combiner->connections = connections;
		}

		node->query_Done = true;

		if (step->sort)
		{
			SimpleSort *sort = step->sort;

			/*
			 * First message is already in the buffer
			 * Further fetch will be under tuplesort control
			 * If query does not produce rows tuplesort will not
			 * be initialized
			 */
			combiner->tuplesortstate = tuplesort_begin_merge(
								   resultslot->tts_tupleDescriptor,
								   sort->numCols,
								   sort->sortColIdx,
								   sort->sortOperators,
								   sort->sortCollations,
								   sort->nullsFirst,
								   combiner,
								   work_mem);
		}
	}

	if (combiner->tuplesortstate)
	{
		if (tuplesort_gettupleslot((Tuplesortstate *) combiner->tuplesortstate,
									  true, resultslot))
			return resultslot;
		else
			ExecClearTuple(resultslot);
	}
	else
	{
		TupleTableSlot *slot = FetchTuple(combiner);
		if (!TupIsNull(slot))
			return slot;
	}

	if (combiner->errorMessage)
		pgxc_node_report_error(combiner);

	return NULL;
}


/*
 * Clean up and discard any data on the data node connections that might not
 * handled yet, including pending on the remote connection.
 */
static void
pgxc_connections_cleanup(ResponseCombiner *combiner)
{
	/* clean up the buffer */
	list_free_deep(combiner->rowBuffer);
	combiner->rowBuffer = NIL;

	/*
	 * Read in and discard remaining data from the connections, if any
	 */
	combiner->current_conn = 0;
	while (combiner->conn_count > 0)
	{
		int res;
		PGXCNodeHandle *conn = combiner->connections[combiner->current_conn];

		/*
		 * Possible if we are doing merge sort.
		 * We can do usual procedure and move connections around since we are
		 * cleaning up and do not care what connection at what position
		 */
		if (conn == NULL)
		{
			REMOVE_CURR_CONN(combiner);
			continue;
		}

		/* throw away current message that may be in the buffer */
		if (combiner->currentRow)
		{
			pfree(combiner->currentRow);
			combiner->currentRow = NULL;
		}

		/* no data is expected */
		if (conn->state == DN_CONNECTION_STATE_IDLE ||
				conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
		{
			REMOVE_CURR_CONN(combiner);
			continue;
		}

		/*
		 * Connection owner is different, so no our data pending at
		 * the connection, nothing to read in.
		 */
		if (conn->combiner && conn->combiner != combiner)
		{
			REMOVE_CURR_CONN(combiner);
			continue;
		}

		res = handle_response(conn, combiner);
		if (res == RESPONSE_EOF)
		{
			struct timeval timeout;
			timeout.tv_sec = END_QUERY_TIMEOUT / 1000;
			timeout.tv_usec = (END_QUERY_TIMEOUT % 1000) * 1000;

			if (pgxc_node_receive(1, &conn, &timeout))
				elog(LOG, "Failed to read response from data nodes when ending query");
		}
	}

	/*
	 * Release tuplesort resources
	 */
	if (combiner->tuplesortstate)
	{
		/*
		 * Free these before tuplesort_end, because these arrays may appear
		 * in the tuplesort's memory context, tuplesort_end deletes this
		 * context and may invalidate the memory.
		 * We still want to free them here, because these may be in different
		 * context.
		 */
		if (combiner->tapenodes)
		{
			pfree(combiner->tapenodes);
			combiner->tapenodes = NULL;
		}
		if (combiner->tapemarks)
		{
			pfree(combiner->tapemarks);
			combiner->tapemarks = NULL;
		}
		/*
		 * tuplesort_end invalidates minimal tuple if it is in the slot because
		 * deletes the TupleSort memory context, causing seg fault later when
		 * releasing tuple table
		 */
		ExecClearTuple(combiner->ss.ps.ps_ResultTupleSlot);
		tuplesort_end((Tuplesortstate *) combiner->tuplesortstate);
		combiner->tuplesortstate = NULL;
	}
}


/*
 * End the remote query
 */
void
ExecEndRemoteQuery(RemoteQueryState *node)
{
	ResponseCombiner *combiner = (ResponseCombiner *) node;

	/*
	 * Clean up remote connections
	 */
	pgxc_connections_cleanup(combiner);

	/*
	 * Clean up parameters if they were set, since plan may be reused
	 */
	if (node->paramval_data)
	{
		pfree(node->paramval_data);
		node->paramval_data = NULL;
		node->paramval_len = 0;
	}

	CloseCombiner(combiner);
	pfree(node);
}


/**********************************************
 *
 * Routines to support RemoteSubplan plan node
 *
 **********************************************/


/*
 * The routine walks recursively over the plan tree and changes cursor names of
 * RemoteSubplan nodes to make them different from launched from the other
 * datanodes. The routine changes cursor names in place, so caller should
 * take writable copy of the plan tree.
 */
void
RemoteSubplanMakeUnique(Node *plan, int unique)
{
	if (plan == NULL)
		return;

	if (IsA(plan, List))
	{
		ListCell *lc;
		foreach(lc, (List *) plan)
		{
			RemoteSubplanMakeUnique(lfirst(lc), unique);
		}
		return;
	}

	/*
	 * Transform SharedQueue name
	 */
	if (IsA(plan, RemoteSubplan))
	{
		((RemoteSubplan *)plan)->unique = unique;
	}
	/* Otherwise it is a Plan descendant */
	RemoteSubplanMakeUnique((Node *) ((Plan *) plan)->lefttree, unique);
	RemoteSubplanMakeUnique((Node *) ((Plan *) plan)->righttree, unique);
	/* Tranform special cases */
	switch (nodeTag(plan))
	{
		case T_Append:
			RemoteSubplanMakeUnique((Node *) ((Append *) plan)->appendplans,
									unique);
			break;
		case T_MergeAppend:
			RemoteSubplanMakeUnique((Node *) ((MergeAppend *) plan)->mergeplans,
									unique);
			break;
		case T_BitmapAnd:
			RemoteSubplanMakeUnique((Node *) ((BitmapAnd *) plan)->bitmapplans,
									unique);
			break;
		case T_BitmapOr:
			RemoteSubplanMakeUnique((Node *) ((BitmapOr *) plan)->bitmapplans,
									unique);
			break;
		case T_SubqueryScan:
			RemoteSubplanMakeUnique((Node *) ((SubqueryScan *) plan)->subplan,
									unique);
			break;
		default:
			break;
	}
}

struct find_params_context
{
	RemoteParam *rparams;
	Bitmapset *defineParams;
};

static bool
determine_param_types_walker(Node *node, struct find_params_context *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Param))
	{
		Param *param = (Param *) node;
		int paramno = param->paramid;

		if (param->paramkind == PARAM_EXEC &&
				bms_is_member(paramno, context->defineParams))
		{
			RemoteParam *cur = context->rparams;
			while (cur->paramkind != PARAM_EXEC || cur->paramid != paramno)
				cur++;
			cur->paramtype = param->paramtype;
			context->defineParams = bms_del_member(context->defineParams,
												   paramno);
			return bms_is_empty(context->defineParams);
		}
	}
	return expression_tree_walker(node, determine_param_types_walker,
								  (void *) context);

}

/*
 * Scan expressions in the plan tree to find Param nodes and get data types
 * from them
 */
static bool
determine_param_types(Plan *plan,  struct find_params_context *context)
{
	Bitmapset *intersect;

	if (plan == NULL)
		return false;

	intersect = bms_intersect(plan->allParam, context->defineParams);
	if (bms_is_empty(intersect))
	{
		/* the subplan does not depend on params we are interested in */
		bms_free(intersect);
		return false;
	}
	bms_free(intersect);

	/* scan target list */
	if (expression_tree_walker((Node *) plan->targetlist,
							   determine_param_types_walker,
							   (void *) context))
		return true;
	/* scan qual */
	if (expression_tree_walker((Node *) plan->qual,
							   determine_param_types_walker,
							   (void *) context))
		return true;

	/* Check additional node-type-specific fields */
	switch (nodeTag(plan))
	{
		case T_Result:
			if (expression_tree_walker((Node *) ((Result *) plan)->resconstantqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_SeqScan:
		case T_SampleScan:
			break;

		case T_IndexScan:
			if (expression_tree_walker((Node *) ((IndexScan *) plan)->indexqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_IndexOnlyScan:
			if (expression_tree_walker((Node *) ((IndexOnlyScan *) plan)->indexqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_BitmapIndexScan:
			if (expression_tree_walker((Node *) ((BitmapIndexScan *) plan)->indexqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_BitmapHeapScan:
			if (expression_tree_walker((Node *) ((BitmapHeapScan *) plan)->bitmapqualorig,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_TidScan:
			if (expression_tree_walker((Node *) ((TidScan *) plan)->tidquals,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_SubqueryScan:
			if (determine_param_types(((SubqueryScan *) plan)->subplan, context))
				return true;
			break;

		case T_FunctionScan:
			if (expression_tree_walker((Node *) ((FunctionScan *) plan)->functions,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_ValuesScan:
			if (expression_tree_walker((Node *) ((ValuesScan *) plan)->values_lists,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_ModifyTable:
			{
				ListCell   *l;

				foreach(l, ((ModifyTable *) plan)->plans)
				{
					if (determine_param_types((Plan *) lfirst(l), context))
						return true;
				}
			}
			break;

		case T_RemoteSubplan:
			break;

		case T_Append:
			{
				ListCell   *l;

				foreach(l, ((Append *) plan)->appendplans)
				{
					if (determine_param_types((Plan *) lfirst(l), context))
						return true;
				}
			}
			break;

		case T_MergeAppend:
			{
				ListCell   *l;

				foreach(l, ((MergeAppend *) plan)->mergeplans)
				{
					if (determine_param_types((Plan *) lfirst(l), context))
						return true;
				}
			}
			break;

		case T_BitmapAnd:
			{
				ListCell   *l;

				foreach(l, ((BitmapAnd *) plan)->bitmapplans)
				{
					if (determine_param_types((Plan *) lfirst(l), context))
						return true;
				}
			}
			break;

		case T_BitmapOr:
			{
				ListCell   *l;

				foreach(l, ((BitmapOr *) plan)->bitmapplans)
				{
					if (determine_param_types((Plan *) lfirst(l), context))
						return true;
				}
			}
			break;

		case T_NestLoop:
			if (expression_tree_walker((Node *) ((Join *) plan)->joinqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_MergeJoin:
			if (expression_tree_walker((Node *) ((Join *) plan)->joinqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			if (expression_tree_walker((Node *) ((MergeJoin *) plan)->mergeclauses,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_HashJoin:
			if (expression_tree_walker((Node *) ((Join *) plan)->joinqual,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			if (expression_tree_walker((Node *) ((HashJoin *) plan)->hashclauses,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_Limit:
			if (expression_tree_walker((Node *) ((Limit *) plan)->limitOffset,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			if (expression_tree_walker((Node *) ((Limit *) plan)->limitCount,
									   determine_param_types_walker,
									   (void *) context))
				return true;
			break;

		case T_RecursiveUnion:
			break;

		case T_LockRows:
			break;

		case T_WindowAgg:
			if (expression_tree_walker((Node *) ((WindowAgg *) plan)->startOffset,
									   determine_param_types_walker,
									   (void *) context))
			if (expression_tree_walker((Node *) ((WindowAgg *) plan)->endOffset,
									   determine_param_types_walker,
									   (void *) context))
			break;

		case T_Hash:
		case T_Agg:
		case T_Material:
		case T_Sort:
		case T_Unique:
		case T_SetOp:
		case T_Group:
			break;

		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(plan));
	}


	/* recurse into subplans */
	return determine_param_types(plan->lefttree, context) ||
			determine_param_types(plan->righttree, context);
}


RemoteSubplanState *
ExecInitRemoteSubplan(RemoteSubplan *node, EState *estate, int eflags)
{
	RemoteStmt			rstmt;
	RemoteSubplanState *remotestate;
	ResponseCombiner   *combiner;
	CombineType			combineType;
	struct rusage		start_r;
	struct timeval		start_t;

	if (log_remotesubplan_stats)
		ResetUsageCommon(&start_r, &start_t);

	remotestate = makeNode(RemoteSubplanState);
	combiner = (ResponseCombiner *) remotestate;
	/*
	 * We do not need to combine row counts if we will receive intermediate
	 * results or if we won't return row count.
	 */
	if (IS_PGXC_DATANODE || estate->es_plannedstmt->commandType == CMD_SELECT)
	{
		combineType = COMBINE_TYPE_NONE;
		remotestate->execOnAll = node->execOnAll;
	}
	else
	{
		if (node->execOnAll)
			combineType = COMBINE_TYPE_SUM;
		else
			combineType = COMBINE_TYPE_SAME;
		/*
		 * If we are updating replicated table we should run plan on all nodes.
		 * We are choosing single node only to read
		 */
		remotestate->execOnAll = true;
	}
	remotestate->execNodes = list_copy(node->nodeList);
	InitResponseCombiner(combiner, 0, combineType);
	combiner->ss.ps.plan = (Plan *) node;
	combiner->ss.ps.state = estate;

	combiner->ss.ps.qual = NIL;

	combiner->request_type = REQUEST_TYPE_QUERY;

	ExecInitResultTupleSlot(estate, &combiner->ss.ps);
	ExecAssignResultTypeFromTL((PlanState *) remotestate);

	/*
	 * We optimize execution if we going to send down query to next level
	 */
	remotestate->local_exec = false;
	if (IS_PGXC_DATANODE)
	{
		if (remotestate->execNodes == NIL)
		{
			/*
			 * Special case, if subplan is not distributed, like Result, or
			 * query against catalog tables only.
			 * We are only interested in filtering out the subplan results and
			 * get only those we are interested in.
			 * XXX we may want to prevent multiple executions in this case
			 * either, to achieve this we will set single execNode on planning
			 * time and this case would never happen, this code branch could
			 * be removed.
			 */
			remotestate->local_exec = true;
		}
		else if (!remotestate->execOnAll)
		{
			/*
			 * XXX We should change planner and remove this flag.
			 * We want only one node is producing the replicated result set,
			 * and planner should choose that node - it is too hard to determine
			 * right node at execution time, because it should be guaranteed
			 * that all consumers make the same decision.
			 * For now always execute replicated plan on local node to save
			 * resources.
			 */

			/*
			 * Make sure local node is in execution list
			 */
			if (list_member_int(remotestate->execNodes, PGXCNodeId-1))
			{
				list_free(remotestate->execNodes);
				remotestate->execNodes = NIL;
				remotestate->local_exec = true;
			}
			else
			{
				/*
				 * To support, we need to connect to some producer, so
				 * each producer should be prepared to serve rows for random
				 * number of consumers. It is hard, because new consumer may
				 * connect after producing is started, on the other hand,
				 * absence of expected consumer is a problem too.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("Getting replicated results from remote node is not supported")));
			}
		}
	}

	/*
	 * If we are going to execute subplan locally or doing explain initialize
	 * the subplan. Otherwise have remote node doing that.
	 */
	if (remotestate->local_exec || (eflags & EXEC_FLAG_EXPLAIN_ONLY))
	{
		outerPlanState(remotestate) = ExecInitNode(outerPlan(node), estate,
												   eflags);
		if (node->distributionNodes)
		{
			Oid 		distributionType = InvalidOid;
			TupleDesc 	typeInfo;

			typeInfo = combiner->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
			if (node->distributionKey != InvalidAttrNumber)
			{
				Form_pg_attribute attr;
				attr = typeInfo->attrs[node->distributionKey - 1];
				distributionType = attr->atttypid;
			}
			/* Set up locator */
			remotestate->locator = createLocator(node->distributionType,
												 RELATION_ACCESS_INSERT,
												 distributionType,
												 LOCATOR_LIST_LIST,
												 0,
												 (void *) node->distributionNodes,
												 (void **) &remotestate->dest_nodes,
												 false);
		}
		else
			remotestate->locator = NULL;
	}

	/*
	 * Encode subplan if it will be sent to remote nodes
	 */
	if (remotestate->execNodes && !(eflags & EXEC_FLAG_EXPLAIN_ONLY))
	{
		ParamListInfo ext_params;
		/* Encode plan if we are going to execute it on other nodes */
		rstmt.type = T_RemoteStmt;
		if (node->distributionType == LOCATOR_TYPE_NONE && IS_PGXC_DATANODE)
		{
			/*
			 * There are cases when planner can not determine distribution of a
			 * subplan, in particular it does not determine distribution of
			 * subquery nodes. Such subplans executed from current location
			 * (node) and combine all results, like from coordinator nodes.
			 * However, if there are multiple locations where distributed
			 * executor is running this node, and there are more of
			 * RemoteSubplan plan nodes in the subtree there will be a problem -
			 * Instances of the inner RemoteSubplan nodes will be using the same
			 * SharedQueue, causing error. To avoid this problem we should
			 * traverse the subtree and change SharedQueue name to make it
			 * unique.
			 */
			RemoteSubplanMakeUnique((Node *) outerPlan(node), PGXCNodeId);
		}
		rstmt.planTree = outerPlan(node);
		/*
		 * If datanode launch further execution of a command it should tell
		 * it is a SELECT, otherwise secondary data nodes won't return tuples
		 * expecting there will be nothing to return.
		 */
		if (IsA(outerPlan(node), ModifyTable))
		{
			rstmt.commandType = estate->es_plannedstmt->commandType;
			rstmt.hasReturning = estate->es_plannedstmt->hasReturning;
			rstmt.resultRelations = estate->es_plannedstmt->resultRelations;
		}
		else
		{
			rstmt.commandType = CMD_SELECT;
			rstmt.hasReturning = false;
			rstmt.resultRelations = NIL;
		}
		rstmt.rtable = estate->es_range_table;
		rstmt.subplans = estate->es_plannedstmt->subplans;
		rstmt.nParamExec = estate->es_plannedstmt->nParamExec;
		ext_params = estate->es_param_list_info;
		rstmt.nParamRemote = (ext_params ? ext_params->numParams : 0) +
				bms_num_members(node->scan.plan.allParam);
		if (rstmt.nParamRemote > 0)
		{
			Bitmapset *tmpset;
			int i;
			int paramno;

			/* Allocate enough space */
			rstmt.remoteparams = (RemoteParam *) palloc(rstmt.nParamRemote *
														sizeof(RemoteParam));
			paramno = 0;
			if (ext_params)
			{
				for (i = 0; i < ext_params->numParams; i++)
				{
					ParamExternData *param = &ext_params->params[i];
					/*
					 * If parameter type is not yet defined but can be defined
					 * do that
					 */
					if (!OidIsValid(param->ptype) && ext_params->paramFetch)
						(*ext_params->paramFetch) (ext_params, i + 1);

					/*
					 * If the parameter type is still not defined, assume that
					 * it is unused. But we put a default INT4OID type for such
					 * unused parameters to keep the parameter pushdown code
					 * happy.
					 *
					 * These unused parameters are never accessed during
					 * execution and we will just a null value for these
					 * "dummy" parameters. But including them here ensures that
					 * we send down the parameters in the correct order and at
					 * the position that the datanode needs
					 */
					if (OidIsValid(param->ptype))
					{
						rstmt.remoteparams[paramno].paramused = 1;
						rstmt.remoteparams[paramno].paramtype = param->ptype;
					}
					else
					{
						rstmt.remoteparams[paramno].paramused = 0;
						rstmt.remoteparams[paramno].paramtype = INT4OID;
					}

					rstmt.remoteparams[paramno].paramkind = PARAM_EXTERN;
					rstmt.remoteparams[paramno].paramid = i + 1;
					paramno++;
				}
				/* store actual number of parameters */
				rstmt.nParamRemote = paramno;
			}

			if (!bms_is_empty(node->scan.plan.allParam))
			{
				Bitmapset *defineParams = NULL;
				tmpset = bms_copy(node->scan.plan.allParam);
				while ((i = bms_first_member(tmpset)) >= 0)
				{
					ParamExecData *prmdata;

					prmdata = &(estate->es_param_exec_vals[i]);
					rstmt.remoteparams[paramno].paramkind = PARAM_EXEC;
					rstmt.remoteparams[paramno].paramid = i;
					rstmt.remoteparams[paramno].paramtype = prmdata->ptype;
					rstmt.remoteparams[paramno].paramused = 1;
					/* Will scan plan tree to find out data type of the param */
					if (prmdata->ptype == InvalidOid)
						defineParams = bms_add_member(defineParams, i);
					paramno++;
				}
				/* store actual number of parameters */
				rstmt.nParamRemote = paramno;
				bms_free(tmpset);
				if (!bms_is_empty(defineParams))
				{
					struct find_params_context context;
					bool all_found;

					context.rparams = rstmt.remoteparams;
					context.defineParams = defineParams;

					all_found = determine_param_types(node->scan.plan.lefttree,
													  &context);
					/*
					 * Remove not defined params from the list of remote params.
					 * If they are not referenced no need to send them down
					 */
					if (!all_found)
					{
						for (i = 0; i < rstmt.nParamRemote; i++)
						{
							if (rstmt.remoteparams[i].paramkind == PARAM_EXEC &&
									bms_is_member(rstmt.remoteparams[i].paramid,
												  context.defineParams))
							{
								/* Copy last parameter inplace */
								rstmt.nParamRemote--;
								if (i < rstmt.nParamRemote)
									rstmt.remoteparams[i] =
										rstmt.remoteparams[rstmt.nParamRemote];
								/* keep current in the same position */
								i--;
							}
						}
					}
					bms_free(context.defineParams);
				}
			}
			remotestate->nParamRemote = rstmt.nParamRemote;
			remotestate->remoteparams = rstmt.remoteparams;
		}
		else
			rstmt.remoteparams = NULL;
		rstmt.rowMarks = estate->es_plannedstmt->rowMarks;
		rstmt.distributionKey = node->distributionKey;
		rstmt.distributionType = node->distributionType;
		rstmt.distributionNodes = node->distributionNodes;
		rstmt.distributionRestrict = node->distributionRestrict;

		set_portable_output(true);
		remotestate->subplanstr = nodeToString(&rstmt);
		set_portable_output(false);

		/*
		 * Connect to remote nodes and send down subplan
		 */
		if (!(eflags & EXEC_FLAG_SUBPLAN))
			ExecFinishInitRemoteSubplan(remotestate);
	}
	remotestate->bound = false;
	/*
	 * It does not makes sense to merge sort if there is only one tuple source.
	 * By the contract it is already sorted
	 */
	if (node->sort && remotestate->execOnAll &&
			list_length(remotestate->execNodes) > 1)
		combiner->merge_sort = true;

	if (log_remotesubplan_stats)
		ShowUsageCommon("ExecInitRemoteSubplan", &start_r, &start_t);

	return remotestate;
}


void
ExecFinishInitRemoteSubplan(RemoteSubplanState *node)
{
	ResponseCombiner   *combiner = (ResponseCombiner *) node;
	RemoteSubplan  	   *plan = (RemoteSubplan *) combiner->ss.ps.plan;
	EState			   *estate = combiner->ss.ps.state;
	Oid        		   *paramtypes = NULL;
	GlobalTransactionId gxid = InvalidGlobalTransactionId;
	Snapshot			snapshot;
	TimestampTz			timestamp;
	int 				i;
	bool				is_read_only;
	char				cursor[NAMEDATALEN];

	/*
	 * Name is required to store plan as a statement
	 */
	Assert(plan->cursor);

	if (plan->unique)
		snprintf(cursor, NAMEDATALEN, "%s_%d", plan->cursor, plan->unique);
	else
		strncpy(cursor, plan->cursor, NAMEDATALEN);

	/* If it is alreaty fully initialized nothing to do */
	if (combiner->connections)
		return;

	/* local only or explain only execution */
	if (node->subplanstr == NULL)
		return;

	/* 
	 * Check if any results are planned to be received here.
	 * Otherwise it does not make sense to send out the subplan.
	 */
	if (IS_PGXC_DATANODE && plan->distributionRestrict && 
			!list_member_int(plan->distributionRestrict, PGXCNodeId - 1))
		return;

	/*
	 * Acquire connections and send down subplan where it will be stored
	 * as a prepared statement.
	 * That does not require transaction id or snapshot, so does not send them
	 * here, postpone till bind.
	 */
	if (node->execOnAll)
	{
		PGXCNodeAllHandles *pgxc_connections;
		pgxc_connections = get_handles(node->execNodes, NIL, false, true);
		combiner->conn_count = pgxc_connections->dn_conn_count;
		combiner->connections = pgxc_connections->datanode_handles;
		combiner->current_conn = 0;
		pfree(pgxc_connections);
	}
	else
	{
		combiner->connections = (PGXCNodeHandle **) palloc(sizeof(PGXCNodeHandle *));
		combiner->connections[0] = get_any_handle(node->execNodes);
		combiner->conn_count = 1;
		combiner->current_conn = 0;
	}

	gxid = GetCurrentTransactionIdIfAny();

	/* extract parameter data types */
	if (node->nParamRemote > 0)
	{
		paramtypes = (Oid *) palloc(node->nParamRemote * sizeof(Oid));
		for (i = 0; i < node->nParamRemote; i++)
			paramtypes[i] = node->remoteparams[i].paramtype;
	}
	/* send down subplan */
	snapshot = GetActiveSnapshot();
	timestamp = GetCurrentGTMStartTimestamp();
	/*
	 * Datanode should not send down statements that may modify
	 * the database. Potgres assumes that all sessions under the same
	 * postmaster have different xids. That may cause a locking problem.
	 * Shared locks acquired for reading still work fine.
	 */
	is_read_only = IS_PGXC_DATANODE ||
			!IsA(outerPlan(plan), ModifyTable);

	for (i = 0; i < combiner->conn_count; i++)
	{
		PGXCNodeHandle *connection = combiner->connections[i];

		if (pgxc_node_begin(1, &connection, gxid, true,
							is_read_only, PGXC_NODE_DATANODE))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Could not begin transaction on data node.")));

		if (pgxc_node_send_timestamp(connection, timestamp))
		{
			combiner->conn_count = 0;
			pfree(combiner->connections);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send command to data nodes")));
		}
		if (snapshot && pgxc_node_send_snapshot(connection, snapshot))
		{
			combiner->conn_count = 0;
			pfree(combiner->connections);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send snapshot to data nodes")));
		}
		if (pgxc_node_send_cmd_id(connection, estate->es_snapshot->curcid) < 0 )
		{
			combiner->conn_count = 0;
			pfree(combiner->connections);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send command ID to data nodes")));
		}
		pgxc_node_send_plan(connection, cursor, "Remote Subplan",
							node->subplanstr, node->nParamRemote, paramtypes);
		if (pgxc_node_flush(connection))
		{
			combiner->conn_count = 0;
			pfree(combiner->connections);
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to send subplan to data nodes")));
		}
	}
}


static void
append_param_data(StringInfo buf, Oid ptype, int pused, Datum value, bool isnull)
{
	uint32 n32;

	/* Assume unused parameters to have null values */
	if (!pused)
		ptype = INT4OID;

	if (isnull)
	{
		n32 = htonl(-1);
		appendBinaryStringInfo(buf, (char *) &n32, 4);
	}
	else
	{
		Oid		typOutput;
		bool	typIsVarlena;
		Datum	pval;
		char   *pstring;
		int		len;

		/* Get info needed to output the value */
		getTypeOutputInfo(ptype, &typOutput, &typIsVarlena);

		/*
		 * If we have a toasted datum, forcibly detoast it here to avoid
		 * memory leakage inside the type's output routine.
		 */
		if (typIsVarlena)
			pval = PointerGetDatum(PG_DETOAST_DATUM(value));
		else
			pval = value;

		/* Convert Datum to string */
		pstring = OidOutputFunctionCall(typOutput, pval);

		/* copy data to the buffer */
		len = strlen(pstring);
		n32 = htonl(len);
		appendBinaryStringInfo(buf, (char *) &n32, 4);
		appendBinaryStringInfo(buf, pstring, len);
	}
}


static int encode_parameters(int nparams, RemoteParam *remoteparams,
							 PlanState *planstate, char** result)
{
	EState 		   *estate = planstate->state;
	StringInfoData	buf;
	uint16 			n16;
	int 			i;
	ExprContext	   *econtext;
	MemoryContext 	oldcontext;

	if (planstate->ps_ExprContext == NULL)
		ExecAssignExprContext(estate, planstate);

	econtext = planstate->ps_ExprContext;
	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	MemoryContextReset(econtext->ecxt_per_tuple_memory);

	initStringInfo(&buf);

	/* Number of parameter values */
	n16 = htons(nparams);
	appendBinaryStringInfo(&buf, (char *) &n16, 2);

	/* Parameter values */
	for (i = 0; i < nparams; i++)
	{
		RemoteParam *rparam = &remoteparams[i];
		int ptype = rparam->paramtype;
		int pused = rparam->paramused;
		if (rparam->paramkind == PARAM_EXTERN)
		{
			ParamExternData *param;
			param = &(estate->es_param_list_info->params[rparam->paramid - 1]);
			append_param_data(&buf, ptype, pused, param->value, param->isnull);
		}
		else
		{
			ParamExecData *param;
			param = &(estate->es_param_exec_vals[rparam->paramid]);
			if (param->execPlan)
			{
				/* Parameter not evaluated yet, so go do it */
				ExecSetParamPlan((SubPlanState *) param->execPlan,
								 planstate->ps_ExprContext);
				/* ExecSetParamPlan should have processed this param... */
				Assert(param->execPlan == NULL);
			}
			if (!param->done)
				param->isnull = true;
			append_param_data(&buf, ptype, pused, param->value, param->isnull);

		}
	}

	/* Take data from the buffer */
	*result = palloc(buf.len);
	memcpy(*result, buf.data, buf.len);
	MemoryContextSwitchTo(oldcontext);
	return buf.len;
}


TupleTableSlot *
ExecRemoteSubplan(RemoteSubplanState *node)
{
	ResponseCombiner *combiner = (ResponseCombiner *) node;
	RemoteSubplan  *plan = (RemoteSubplan *) combiner->ss.ps.plan;
	EState		   *estate = combiner->ss.ps.state;
	TupleTableSlot *resultslot = combiner->ss.ps.ps_ResultTupleSlot;
	struct rusage	start_r;
	struct timeval		start_t;

	/* 
	 * We allow combiner->conn_count == 0 after node initialization
	 * if we figured out that current node won't receive any result
	 * because of distributionRestrict is set by planner.
	 * But we should distinguish this case from others, when conn_count is 0.
	 * That is possible if local execution is chosen or data are buffered 
	 * at the coordinator or data are exhausted and node was reset.
	 * in last two cases connections are saved to cursor_connections and we
	 * can check their presence.  
	 */
	if (!node->local_exec && combiner->conn_count == 0 && 
			combiner->cursor_count == 0)
		return NULL;

	if (log_remotesubplan_stats)
		ResetUsageCommon(&start_r, &start_t);

primary_mode_phase_two:
	if (!node->bound)
	{
		int fetch = 0;
		int paramlen = 0;
		char *paramdata = NULL;
		/*
		 * Conditions when we want to execute query on the primary node first:
		 * Coordinator running replicated ModifyTable on multiple nodes
		 */
		bool primary_mode = combiner->probing_primary ||
				(IS_PGXC_COORDINATOR &&
				 combiner->combine_type == COMBINE_TYPE_SAME &&
				 OidIsValid(primary_data_node) &&
				 combiner->conn_count > 1);
		char cursor[NAMEDATALEN];

		if (plan->cursor)
		{
			fetch = 1000;
			if (plan->unique)
				snprintf(cursor, NAMEDATALEN, "%s_%d", plan->cursor, plan->unique);
			else
				strncpy(cursor, plan->cursor, NAMEDATALEN);
		}
		else
			cursor[0] = '\0';

		/*
		 * Send down all available parameters, if any is used by the plan
		 */
		if (estate->es_param_list_info ||
				!bms_is_empty(plan->scan.plan.allParam))
			paramlen = encode_parameters(node->nParamRemote,
										 node->remoteparams,
										 &combiner->ss.ps,
										 &paramdata);

		/*
		 * The subplan being rescanned, need to restore connections and
		 * re-bind the portal
		 */
		if (combiner->cursor)
		{
			int i;

			/*
			 * On second phase of primary mode connections are properly set,
			 * so do not copy.
			 */
			if (!combiner->probing_primary)
			{
				combiner->conn_count = combiner->cursor_count;
				memcpy(combiner->connections, combiner->cursor_connections,
							combiner->cursor_count * sizeof(PGXCNodeHandle *));
			}

			for (i = 0; i < combiner->conn_count; i++)
			{
				PGXCNodeHandle *conn = combiner->connections[i];

				CHECK_OWNERSHIP(conn, combiner);

				/* close previous cursor only on phase 1 */
				if (!primary_mode || !combiner->probing_primary)
					pgxc_node_send_close(conn, false, combiner->cursor);

				/*
				 * If we now should probe primary, skip execution on non-primary
				 * nodes
				 */
				if (primary_mode && !combiner->probing_primary &&
						conn->nodeoid != primary_data_node)
					continue;

				/* rebind */
				pgxc_node_send_bind(conn, combiner->cursor, combiner->cursor,
									paramlen, paramdata);
				/* execute */
				pgxc_node_send_execute(conn, combiner->cursor, fetch);
				/* submit */
				if (pgxc_node_send_flush(conn))
				{
					combiner->conn_count = 0;
					pfree(combiner->connections);
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to send command to data nodes")));
				}

				/*
				 * There could be only one primary node, but can not leave the
				 * loop now, because we need to close cursors.
				 */
				if (primary_mode && !combiner->probing_primary)
				{
					combiner->current_conn = i;
				}
			}
		}
		else if (node->execNodes)
		{
			CommandId		cid;
			int 			i;

			/*
			 * There are prepared statement, connections should be already here
			 */
			Assert(combiner->conn_count > 0);

			combiner->extended_query = true;
			cid = estate->es_snapshot->curcid;

			for (i = 0; i < combiner->conn_count; i++)
			{
				PGXCNodeHandle *conn = combiner->connections[i];

				CHECK_OWNERSHIP(conn, combiner);

				/*
				 * If we now should probe primary, skip execution on non-primary
				 * nodes
				 */
				if (primary_mode && !combiner->probing_primary &&
						conn->nodeoid != primary_data_node)
					continue;

				/*
				 * Update Command Id. Other command may be executed after we
				 * prepare and advanced Command Id. We should use one that
				 * was active at the moment when command started.
				 */
				if (pgxc_node_send_cmd_id(conn, cid))
				{
					combiner->conn_count = 0;
					pfree(combiner->connections);
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to send command ID to data nodes")));
				}

				/*
				 * Resend the snapshot as well since the connection may have
				 * been buffered and use by other commands, with different
				 * snapshot. Set the snapshot back to what it was
				 */
				if (pgxc_node_send_snapshot(conn, estate->es_snapshot))
				{
					combiner->conn_count = 0;
					pfree(combiner->connections);
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to send snapshot to data nodes")));
				}

				/* bind */
				pgxc_node_send_bind(conn, cursor, cursor, paramlen, paramdata);
				/* execute */
				pgxc_node_send_execute(conn, cursor, fetch);
				/* submit */
				if (pgxc_node_send_flush(conn))
				{
					combiner->conn_count = 0;
					pfree(combiner->connections);
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("Failed to send command to data nodes")));
				}

				/*
				 * There could be only one primary node, so if we executed
				 * subquery on the phase one of primary mode we can leave the
				 * loop now.
				 */
				if (primary_mode && !combiner->probing_primary)
				{
					combiner->current_conn = i;
					break;
				}
			}

			/*
			 * On second phase of primary mode connections are backed up
			 * already, so do not copy.
			 */
			if (primary_mode)
			{
				if (combiner->probing_primary)
				{
					combiner->cursor = pstrdup(cursor);
				}
				else
				{
					combiner->cursor_count = combiner->conn_count;
					combiner->cursor_connections = (PGXCNodeHandle **) palloc(
								combiner->conn_count * sizeof(PGXCNodeHandle *));
					memcpy(combiner->cursor_connections, combiner->connections,
								combiner->conn_count * sizeof(PGXCNodeHandle *));
				}
			}
			else
			{
				combiner->cursor = pstrdup(cursor);
				combiner->cursor_count = combiner->conn_count;
				combiner->cursor_connections = (PGXCNodeHandle **) palloc(
							combiner->conn_count * sizeof(PGXCNodeHandle *));
				memcpy(combiner->cursor_connections, combiner->connections,
							combiner->conn_count * sizeof(PGXCNodeHandle *));
			}
		}

		if (combiner->merge_sort)
		{
			/*
			 * Requests are already made and sorter can fetch tuples to populate
			 * sort buffer.
			 */
			combiner->tuplesortstate = tuplesort_begin_merge(
									   resultslot->tts_tupleDescriptor,
									   plan->sort->numCols,
									   plan->sort->sortColIdx,
									   plan->sort->sortOperators,
									   plan->sort->sortCollations,
									   plan->sort->nullsFirst,
									   combiner,
									   work_mem);
		}
		if (primary_mode)
		{
			if (combiner->probing_primary)
			{
				combiner->probing_primary = false;
				node->bound = true;
			}
			else
				combiner->probing_primary = true;
		}
		else
			node->bound = true;
	}

	if (combiner->tuplesortstate)
	{
		if (tuplesort_gettupleslot((Tuplesortstate *) combiner->tuplesortstate,
								   true, resultslot))
		{
			if (log_remotesubplan_stats)
				ShowUsageCommon("ExecRemoteSubplan", &start_r, &start_t);
			return resultslot;
		}
	}
	else
	{
		TupleTableSlot *slot = FetchTuple(combiner);
		if (!TupIsNull(slot))
		{
			if (log_remotesubplan_stats)
				ShowUsageCommon("ExecRemoteSubplan", &start_r, &start_t);
			return slot;
		}
		else if (combiner->probing_primary)
			/* phase1 is successfully completed, run on other nodes */
			goto primary_mode_phase_two;
	}
	if (combiner->errorMessage)
		pgxc_node_report_error(combiner);

	if (log_remotesubplan_stats)
		ShowUsageCommon("ExecRemoteSubplan", &start_r, &start_t);

	return NULL;
}


void
ExecReScanRemoteSubplan(RemoteSubplanState *node)
{
	ResponseCombiner *combiner = (ResponseCombiner *)node;

	/*
	 * If we haven't queried remote nodes yet, just return. If outerplan'
	 * chgParam is not NULL then it will be re-scanned by ExecProcNode,
	 * else - no reason to re-scan it at all.
	 */
	if (!node->bound)
		return;

	/*
	 * If we execute locally rescan local copy of the plan
	 */
	if (outerPlanState(node))
		ExecReScan(outerPlanState(node));

	/*
	 * Consume any possible pending input
	 */
	pgxc_connections_cleanup(combiner);

	/* misc cleanup */
	combiner->command_complete_count = 0;
	combiner->description_count = 0;

	/*
	 * Force query is re-bound with new parameters
	 */
	node->bound = false;
}


void
ExecEndRemoteSubplan(RemoteSubplanState *node)
{
	ResponseCombiner *combiner = (ResponseCombiner *)node;
	RemoteSubplan    *plan = (RemoteSubplan *) combiner->ss.ps.plan;
	int i;
	struct rusage	start_r;
	struct timeval		start_t;

	if (log_remotesubplan_stats)
		ResetUsageCommon(&start_r, &start_t);

	if (outerPlanState(node))
		ExecEndNode(outerPlanState(node));
	if (node->locator)
		freeLocator(node->locator);

	/*
	 * Consume any possible pending input
	 */
	if (node->bound)
		pgxc_connections_cleanup(combiner);

	/*
	 * Update coordinator statistics
	 */
	if (IS_PGXC_COORDINATOR)
	{
		EState *estate = combiner->ss.ps.state;

		if (estate->es_num_result_relations > 0 && estate->es_processed > 0)
		{
			switch (estate->es_plannedstmt->commandType)
			{
				case CMD_INSERT:
					/* One statement can insert into only one relation */
					pgstat_count_remote_insert(
								estate->es_result_relations[0].ri_RelationDesc,
								estate->es_processed);
					break;
				case CMD_UPDATE:
				case CMD_DELETE:
					{
						/*
						 * We can not determine here how many row were updated
						 * or delete in each table, so assume same number of
						 * affected row in each table.
						 * If resulting number of rows is 0 because of rounding,
						 * increment each counter at least on 1.
						 */
						int		i;
						int 	n;
						bool 	update;

						update = (estate->es_plannedstmt->commandType == CMD_UPDATE);
						n = estate->es_processed / estate->es_num_result_relations;
						if (n == 0)
							n = 1;
						for (i = 0; i < estate->es_num_result_relations; i++)
						{
							Relation r;
							r = estate->es_result_relations[i].ri_RelationDesc;
							if (update)
								pgstat_count_remote_update(r, n);
							else
								pgstat_count_remote_delete(r, n);
						}
					}
					break;
				default:
					/* nothing to count */
					break;
			}
		}
	}

	/*
	 * Close portals. While cursors_connections exist there are open portals
	 */
	if (combiner->cursor)
	{
		/* Restore connections where there are active statements */
		combiner->conn_count = combiner->cursor_count;
		memcpy(combiner->connections, combiner->cursor_connections,
					combiner->cursor_count * sizeof(PGXCNodeHandle *));
		for (i = 0; i < combiner->cursor_count; i++)
		{
			PGXCNodeHandle *conn;

			conn = combiner->cursor_connections[i];

			CHECK_OWNERSHIP(conn, combiner);

			if (pgxc_node_send_close(conn, false, combiner->cursor) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Failed to close data node cursor")));
		}
		/* The cursor stuff is not needed */
		combiner->cursor = NULL;
		combiner->cursor_count = 0;
		pfree(combiner->cursor_connections);
		combiner->cursor_connections = NULL;
	}

	/* Close statements, even if they never were bound */
	for (i = 0; i < combiner->conn_count; i++)
	{
		PGXCNodeHandle *conn;
		char			cursor[NAMEDATALEN];

		if (plan->cursor)
		{
			if (plan->unique)
				snprintf(cursor, NAMEDATALEN, "%s_%d", plan->cursor, plan->unique);
			else
				strncpy(cursor, plan->cursor, NAMEDATALEN);
		}
		else
			cursor[0] = '\0';

		conn = combiner->connections[i];

		CHECK_OWNERSHIP(conn, combiner);

		if (pgxc_node_send_close(conn, true, cursor) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close data node statement")));
		/* Send SYNC and wait for ReadyForQuery */
		if (pgxc_node_send_sync(conn) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to synchronize data node")));
		/*
		 * Formally connection is not in QUERY state, we set the state to read
		 * CloseDone and ReadyForQuery responses. Upon receiving ReadyForQuery
		 * state will be changed back to IDLE and conn->coordinator will be
		 * cleared.
		 */
		PGXCNodeSetConnectionState(conn, DN_CONNECTION_STATE_CLOSE);
	}

	while (combiner->conn_count > 0)
	{
		if (pgxc_node_receive(combiner->conn_count,
							  combiner->connections, NULL))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to close remote subplan")));
		i = 0;
		while (i < combiner->conn_count)
		{
			int res = handle_response(combiner->connections[i], combiner);
			if (res == RESPONSE_EOF)
			{
				i++;
			}
			else if (res == RESPONSE_READY)
			{
				/* Done, connection is reade for query */
				if (--combiner->conn_count > i)
					combiner->connections[i] =
							combiner->connections[combiner->conn_count];
			}
			else if (res == RESPONSE_DATAROW)
			{
				/*
				 * If we are finishing slowly running remote subplan while it
				 * is still working (because of Limit, for example) it may
				 * produce one or more tuples between connection cleanup and
				 * handling Close command. One tuple does not cause any problem,
				 * but if it will not be read the next tuple will trigger
				 * assertion failure. So if we got a tuple, just read and
				 * discard it here.
				 */
				pfree(combiner->currentRow);
				combiner->currentRow = NULL;
			}
			/* Ignore other possible responses */
		}
	}

	ValidateAndCloseCombiner(combiner);
	pfree(node);

	if (log_remotesubplan_stats)
		ShowUsageCommon("ExecEndRemoteSubplan", &start_r, &start_t);
}

/*
 * pgxc_node_report_error
 * Throw error from Datanode if any.
 */
static void
pgxc_node_report_error(ResponseCombiner *combiner)
{
	/* If no combiner, nothing to do */
	if (!combiner)
		return;
	if (combiner->errorMessage)
	{
		char *code = combiner->errorCode;
		if ((combiner->errorDetail == NULL) && (combiner->errorHint == NULL))
			ereport(ERROR,
					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
					errmsg("%s", combiner->errorMessage)));
		else if ((combiner->errorDetail != NULL) && (combiner->errorHint != NULL))
			ereport(ERROR,
					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
					errmsg("%s", combiner->errorMessage),
					errdetail("%s", combiner->errorDetail),
					errhint("%s", combiner->errorHint)));
		else if (combiner->errorDetail != NULL)
			ereport(ERROR,
					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
					errmsg("%s", combiner->errorMessage),
					errdetail("%s", combiner->errorDetail)));
		else
			ereport(ERROR,
					(errcode(MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4])),
					errmsg("%s", combiner->errorMessage),
					errhint("%s", combiner->errorHint)));
	}
}


/*
 * get_success_nodes:
 * Currently called to print a user-friendly message about
 * which nodes the query failed.
 * Gets all the nodes where no 'E' (error) messages were received; i.e. where the
 * query ran successfully.
 */
static ExecNodes *
get_success_nodes(int node_count, PGXCNodeHandle **handles, char node_type, StringInfo failednodes)
{
	ExecNodes *success_nodes = NULL;
	int i;

	for (i = 0; i < node_count; i++)
	{
		PGXCNodeHandle *handle = handles[i];
		int nodenum = PGXCNodeGetNodeId(handle->nodeoid, &node_type);

		if (!handle->error)
		{
			if (!success_nodes)
				success_nodes = makeNode(ExecNodes);
			success_nodes->nodeList = lappend_int(success_nodes->nodeList, nodenum);
		}
		else
		{
			if (failednodes->len == 0)
				appendStringInfo(failednodes, "Error message received from nodes:");
			appendStringInfo(failednodes, " %s#%d",
				(node_type == PGXC_NODE_COORDINATOR ? "coordinator" : "datanode"),
				nodenum + 1);
		}
	}
	return success_nodes;
}

/*
 * pgxc_all_success_nodes: Uses get_success_nodes() to collect the
 * user-friendly message from coordinator as well as datanode.
 */
void
pgxc_all_success_nodes(ExecNodes **d_nodes, ExecNodes **c_nodes, char **failednodes_msg)
{
	PGXCNodeAllHandles *connections = get_exec_connections(NULL, NULL, EXEC_ON_ALL_NODES, true);
	StringInfoData failednodes;
	initStringInfo(&failednodes);

	*d_nodes = get_success_nodes(connections->dn_conn_count,
	                             connections->datanode_handles,
								 PGXC_NODE_DATANODE,
								 &failednodes);

	*c_nodes = get_success_nodes(connections->co_conn_count,
	                             connections->coord_handles,
								 PGXC_NODE_COORDINATOR,
								 &failednodes);

	if (failednodes.len == 0)
		*failednodes_msg = NULL;
	else
		*failednodes_msg = failednodes.data;

	pfree_pgxc_all_handles(connections);
}


/*
 * set_dbcleanup_callback:
 * Register a callback function which does some non-critical cleanup tasks
 * on xact success or abort, such as tablespace/database directory cleanup.
 */
void set_dbcleanup_callback(xact_callback function, void *paraminfo, int paraminfo_size)
{
	void *fparams;

	fparams = MemoryContextAlloc(TopMemoryContext, paraminfo_size);
	memcpy(fparams, paraminfo, paraminfo_size);

	dbcleanup_info.function = function;
	dbcleanup_info.fparams = fparams;
}

/*
 * AtEOXact_DBCleanup: To be called at post-commit or pre-abort.
 * Calls the cleanup function registered during this transaction, if any.
 */
void AtEOXact_DBCleanup(bool isCommit)
{
	if (dbcleanup_info.function)
		(*dbcleanup_info.function)(isCommit, dbcleanup_info.fparams);

	/*
	 * Just reset the callbackinfo. We anyway don't want this to be called again,
	 * until explicitly set.
	 */
	dbcleanup_info.function = NULL;
	if (dbcleanup_info.fparams)
	{
		pfree(dbcleanup_info.fparams);
		dbcleanup_info.fparams = NULL;
	}
}

char *
GetImplicit2PCGID(const char *implicit2PC_head, bool localWrite)
{
	int dnCount = 0, coordCount = 0;
	int dnNodeIds[MaxDataNodes];
	int coordNodeIds[MaxCoords];
	MemoryContext oldContext = CurrentMemoryContext;
	StringInfoData str;
	int i;

	oldContext = MemoryContextSwitchTo(TopTransactionContext);
	initStringInfo(&str);
	/*
	 * Check how many coordinators and datanodes are involved in this
	 * transaction
	 */
	pgxc_node_remote_count(&dnCount, dnNodeIds, &coordCount, coordNodeIds);
	appendStringInfo(&str, "%s%u:%s:%c:%d:%d",
			implicit2PC_head,
			GetTopTransactionId(),
			PGXCNodeName,
			localWrite ? 'T' : 'F',
			dnCount,
			coordCount + (localWrite ? 1 : 0));

	for (i = 0; i < dnCount; i++)
		appendStringInfo(&str, ":%d", dnNodeIds[i]);
	for (i = 0; i < coordCount; i++)
		appendStringInfo(&str, ":%d", coordNodeIds[i]);

	if (localWrite)
		appendStringInfo(&str, ":%d", PGXCNodeIdentifier);

	MemoryContextSwitchTo(oldContext);

	return str.data;
}
