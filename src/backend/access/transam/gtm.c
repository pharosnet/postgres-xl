/*-------------------------------------------------------------------------
 *
 * gtm.c
 *
 *	  Module interfacing with GTM
 *
 *
 *-------------------------------------------------------------------------
 */

#include <sys/types.h>
#include <unistd.h>

#include "postgres.h"
#include "gtm/libpq-fe.h"
#include "gtm/gtm_client.h"
#include "access/gtm.h"
#include "access/transam.h"
#include "access/xact.h"
#include "utils/elog.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "gtm/gtm_c.h"
#include "postmaster/autovacuum.h"
#include "postmaster/clustermon.h"
#include "storage/backendid.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/pg_rusage.h"

/* To access sequences */
#define GetMyCoordName \
	OidIsValid(MyCoordId) ? get_pgxc_nodename(MyCoordId) : ""
/* Configuration variables */
char *GtmHost = "localhost";
int GtmPort = 6666;
static int GtmConnectTimeout = 60;
bool IsXidFromGTM = false;
bool gtm_backup_barrier = false;
extern bool FirstSnapshotSet;

static GTM_Conn *conn;

/* Used to check if needed to commit/abort at datanodes */
GlobalTransactionId currentGxid = InvalidGlobalTransactionId;

bool
IsGTMConnected()
{
	return conn != NULL;
}

static void
CheckConnection(void)
{
	/* Be sure that a backend does not use a postmaster connection */
	if (IsUnderPostmaster && GTMPQispostmaster(conn) == 1)
	{
		InitGTM();
		return;
	}

	if (GTMPQstatus(conn) != CONNECTION_OK)
		InitGTM();
}

void
InitGTM(void)
{
	/* 256 bytes should be enough */
	char conn_str[256];

	/* If this thread is postmaster itself, it contacts gtm identifying itself */
	if (!IsUnderPostmaster)
	{
		GTM_PGXCNodeType remote_type = GTM_NODE_DEFAULT;

		if (IS_PGXC_COORDINATOR)
			remote_type = GTM_NODE_COORDINATOR;
		else if (IS_PGXC_DATANODE)
			remote_type = GTM_NODE_DATANODE;

		/* Use 60s as connection timeout */
		sprintf(conn_str, "host=%s port=%d node_name=%s remote_type=%d postmaster=1 connect_timeout=%d",
								GtmHost, GtmPort, PGXCNodeName, remote_type,
								GtmConnectTimeout);

		/* Log activity of GTM connections */
		elog(DEBUG1, "Postmaster: connection established to GTM with string %s", conn_str);
	}
	else
	{
		/* Use 60s as connection timeout */
		sprintf(conn_str, "host=%s port=%d node_name=%s connect_timeout=%d",
				GtmHost, GtmPort, PGXCNodeName, GtmConnectTimeout);

		/* Log activity of GTM connections */
		if (IsAutoVacuumWorkerProcess())
			elog(DEBUG1, "Autovacuum worker: connection established to GTM with string %s", conn_str);
		else if (IsAutoVacuumLauncherProcess())
			elog(DEBUG1, "Autovacuum launcher: connection established to GTM with string %s", conn_str);
		else if (IsClusterMonitorProcess())
			elog(DEBUG1, "Cluster monitor: connection established to GTM with string %s", conn_str);
		else
			elog(DEBUG1, "Postmaster child: connection established to GTM with string %s", conn_str);
	}

	conn = PQconnectGTM(conn_str);
	if (GTMPQstatus(conn) != CONNECTION_OK)
	{
		int save_errno = errno;

		ereport(WARNING,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("can not connect to GTM: %m")));

		errno = save_errno;

		CloseGTM();
	}

	else if (IS_PGXC_COORDINATOR)
		register_session(conn, PGXCNodeName, MyProcPid, MyBackendId);
}

void
CloseGTM(void)
{
	GTMPQfinish(conn);
	conn = NULL;

	/* Log activity of GTM connections */
	if (!IsUnderPostmaster)
		elog(DEBUG1, "Postmaster: connection to GTM closed");
	else if (IsAutoVacuumWorkerProcess())
		elog(DEBUG1, "Autovacuum worker: connection to GTM closed");
	else if (IsAutoVacuumLauncherProcess())
		elog(DEBUG1, "Autovacuum launcher: connection to GTM closed");
	else if (IsClusterMonitorProcess())
		elog(DEBUG1, "Cluster monitor: connection to GTM closed");
	else
		elog(DEBUG1, "Postmaster child: connection to GTM closed");
}

GlobalTransactionId
BeginTranGTM(GTM_Timestamp *timestamp, const char *globalSession)
{
	GlobalTransactionId  xid = InvalidGlobalTransactionId;
	struct rusage start_r;
	struct timeval start_t;

	if (log_gtm_stats)
		ResetUsageCommon(&start_r, &start_t);

	CheckConnection();
	// TODO Isolation level
	if (conn)
		xid =  begin_transaction(conn, GTM_ISOLATION_RC, globalSession, timestamp);

	/* If something went wrong (timeout), try and reset GTM connection
	 * and retry. This is safe at the beginning of a transaction.
	 */
	if (!TransactionIdIsValid(xid))
	{
		CloseGTM();
		InitGTM();
		if (conn)
			xid = begin_transaction(conn, GTM_ISOLATION_RC, globalSession, timestamp);
	}
	if (xid)
		IsXidFromGTM = true;
	currentGxid = xid;

	elog(DEBUG2, "BeginTranGTM - session:%s, xid: %d", globalSession, xid);

	if (log_gtm_stats)
		ShowUsageCommon("BeginTranGTM", &start_r, &start_t);
	return xid;
}

GlobalTransactionId
BeginTranAutovacuumGTM(void)
{
	GlobalTransactionId  xid = InvalidGlobalTransactionId;

	CheckConnection();
	// TODO Isolation level
	if (conn)
		xid =  begin_transaction_autovacuum(conn, GTM_ISOLATION_RC);

	/*
	 * If something went wrong (timeout), try and reset GTM connection and retry.
	 * This is safe at the beginning of a transaction.
	 */
	if (!TransactionIdIsValid(xid))
	{
		CloseGTM();
		InitGTM();
		if (conn)
			xid =  begin_transaction_autovacuum(conn, GTM_ISOLATION_RC);
	}
	currentGxid = xid;

	elog(DEBUG3, "BeginTranGTM - %d", xid);
	return xid;
}

int
CommitTranGTM(GlobalTransactionId gxid, int waited_xid_count,
		GlobalTransactionId *waited_xids)
{
	int ret;
	struct rusage start_r;
	struct timeval start_t;

	if (!GlobalTransactionIdIsValid(gxid))
		return 0;

	if (log_gtm_stats)
		ResetUsageCommon(&start_r, &start_t);

	elog(DEBUG3, "CommitTranGTM: %d", gxid);

	CheckConnection();
	ret = -1;
	if (conn)
	ret = commit_transaction(conn, gxid, waited_xid_count, waited_xids);

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will close the transaction locally anyway, and closing GTM will force
	 * it to be closed on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret = commit_transaction(conn, gxid, waited_xid_count, waited_xids);
	}

	/* Close connection in case commit is done by autovacuum worker or launcher */
	if (IsAutoVacuumWorkerProcess() || IsAutoVacuumLauncherProcess())
		CloseGTM();

	currentGxid = InvalidGlobalTransactionId;

	if (log_gtm_stats)
		ShowUsageCommon("CommitTranGTM", &start_r, &start_t);
	return ret;
}

/*
 * For a prepared transaction, commit the gxid used for PREPARE TRANSACTION
 * and for COMMIT PREPARED.
 */
int
CommitPreparedTranGTM(GlobalTransactionId gxid,
		GlobalTransactionId prepared_gxid, int waited_xid_count,
		GlobalTransactionId *waited_xids)
{
	int ret = 0;
	struct rusage start_r;
	struct timeval start_t;

	if (!GlobalTransactionIdIsValid(gxid) || !GlobalTransactionIdIsValid(prepared_gxid))
		return ret;

	if (log_gtm_stats)
		ResetUsageCommon(&start_r, &start_t);

	elog(DEBUG3, "CommitPreparedTranGTM: %d:%d", gxid, prepared_gxid);

	CheckConnection();
	ret = -1;
	if (conn)
		ret = commit_prepared_transaction(conn, gxid, prepared_gxid,
			waited_xid_count, waited_xids);

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will close the transaction locally anyway, and closing GTM will force
	 * it to be closed on GTM.
	 */

	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret = commit_prepared_transaction(conn, gxid, prepared_gxid,
					waited_xid_count, waited_xids);
	}
	currentGxid = InvalidGlobalTransactionId;

	if (log_gtm_stats)
		ShowUsageCommon("CommitPreparedTranGTM", &start_r, &start_t);

	return ret;
}

int
RollbackTranGTM(GlobalTransactionId gxid)
{
	int ret = -1;

	if (!GlobalTransactionIdIsValid(gxid))
		return 0;
	CheckConnection();

	if (conn)
		ret = abort_transaction(conn, gxid);

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will abort the transaction locally anyway, and closing GTM will force
	 * it to end on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret = abort_transaction(conn, gxid);
	}

	currentGxid = InvalidGlobalTransactionId;
	return ret;
}

int
StartPreparedTranGTM(GlobalTransactionId gxid,
					 char *gid,
					 char *nodestring)
{
	int ret = 0;

	if (!GlobalTransactionIdIsValid(gxid))
		return 0;
	CheckConnection();

	ret = -1;
	if (conn)
		ret = start_prepared_transaction(conn, gxid, gid, nodestring);

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will abort the transaction locally anyway, and closing GTM will force
	 * it to end on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret = start_prepared_transaction(conn, gxid, gid, nodestring);
	}

	return ret;
}

int
PrepareTranGTM(GlobalTransactionId gxid)
{
	int ret;
	struct rusage start_r;
	struct timeval start_t;

	if (!GlobalTransactionIdIsValid(gxid))
		return 0;
	CheckConnection();

	if (log_gtm_stats)
		ResetUsageCommon(&start_r, &start_t);

	ret = -1;
	if (conn)
		ret = prepare_transaction(conn, gxid);

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will close the transaction locally anyway, and closing GTM will force
	 * it to be closed on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret = prepare_transaction(conn, gxid);
	}
	currentGxid = InvalidGlobalTransactionId;

	if (log_gtm_stats)
		ShowUsageCommon("PrepareTranGTM", &start_r, &start_t);

	return ret;
}


int
GetGIDDataGTM(char *gid,
			  GlobalTransactionId *gxid,
			  GlobalTransactionId *prepared_gxid,
			  char **nodestring)
{
	int ret = 0;

	CheckConnection();
	ret = -1;
	if (conn)
		ret = get_gid_data(conn, GTM_ISOLATION_RC, gid, gxid,
					   prepared_gxid, nodestring);

	/*
	 * If something went wrong (timeout), try and reset GTM connection.
	 * We will abort the transaction locally anyway, and closing GTM will force
	 * it to end on GTM.
	 */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret = get_gid_data(conn, GTM_ISOLATION_RC, gid, gxid,
							   prepared_gxid, nodestring);
	}

	return ret;
}

GTM_Snapshot
GetSnapshotGTM(GlobalTransactionId gxid, bool canbe_grouped)
{
	GTM_Snapshot ret_snapshot = NULL;
	struct rusage start_r;
	struct timeval start_t;

	CheckConnection();

	if (log_gtm_stats)
		ResetUsageCommon(&start_r, &start_t);

	if (conn)
		ret_snapshot = get_snapshot(conn, gxid, canbe_grouped);
	if (ret_snapshot == NULL)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret_snapshot = get_snapshot(conn, gxid, canbe_grouped);
	}

	if (log_gtm_stats)
		ShowUsageCommon("GetSnapshotGTM", &start_r, &start_t);

	return ret_snapshot;
}


/*
 * Create a sequence on the GTM.
 */
int
CreateSequenceGTM(char *seqname, GTM_Sequence increment, GTM_Sequence minval,
				  GTM_Sequence maxval, GTM_Sequence startval, bool cycle)
{
	GTM_SequenceKeyData seqkey;
	CheckConnection();
	seqkey.gsk_keylen = strlen(seqname) + 1;
	seqkey.gsk_key = seqname;

	return conn ? open_sequence(conn, &seqkey, increment, minval, maxval,
			startval, cycle, GetTopTransactionId()) : 0;
}

/*
 * Alter a sequence on the GTM
 */
int
AlterSequenceGTM(char *seqname, GTM_Sequence increment, GTM_Sequence minval,
				 GTM_Sequence maxval, GTM_Sequence startval, GTM_Sequence lastval, bool cycle, bool is_restart)
{
	GTM_SequenceKeyData seqkey;
	CheckConnection();
	seqkey.gsk_keylen = strlen(seqname) + 1;
	seqkey.gsk_key = seqname;

	return conn ? alter_sequence(conn, &seqkey, increment, minval, maxval,
			startval, lastval, cycle, is_restart) : 0;
}

/*
 * get the current sequence value
 */

GTM_Sequence
GetCurrentValGTM(char *seqname)
{
	GTM_Sequence ret = -1;
	GTM_SequenceKeyData seqkey;
	char   *coordName = IS_PGXC_COORDINATOR ? PGXCNodeName : GetMyCoordName;
	int		coordPid = IS_PGXC_COORDINATOR ? MyProcPid : MyCoordPid;
	int		status;

	CheckConnection();
	seqkey.gsk_keylen = strlen(seqname) + 1;
	seqkey.gsk_key = seqname;

	if (conn)
		status = get_current(conn, &seqkey, coordName, coordPid, &ret);
	else
		status = GTM_RESULT_COMM_ERROR;

	/* retry once */
	if (status == GTM_RESULT_COMM_ERROR)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			status = get_current(conn, &seqkey, coordName, coordPid, &ret);
	}
	if (status != GTM_RESULT_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("%s", GTMPQerrorMessage(conn))));
	return ret;
}

/*
 * Get the next sequence value
 */
GTM_Sequence
GetNextValGTM(char *seqname, GTM_Sequence range, GTM_Sequence *rangemax)
{
	GTM_Sequence ret = -1;
	GTM_SequenceKeyData seqkey;
	char   *coordName = IS_PGXC_COORDINATOR ? PGXCNodeName : GetMyCoordName;
	int		coordPid = IS_PGXC_COORDINATOR ? MyProcPid : MyCoordPid;
	int		status;

	CheckConnection();
	seqkey.gsk_keylen = strlen(seqname) + 1;
	seqkey.gsk_key = seqname;

	if (conn)
		status = get_next(conn, &seqkey, coordName,
						  coordPid, range, &ret, rangemax);
	else
		status = GTM_RESULT_COMM_ERROR;

	/* retry once */
	if (status == GTM_RESULT_COMM_ERROR)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			status = get_next(conn, &seqkey, coordName, coordPid,
							  range, &ret, rangemax);
	}
	if (status != GTM_RESULT_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("%s", GTMPQerrorMessage(conn))));
	return ret;
}

/*
 * Set values for sequence
 */
int
SetValGTM(char *seqname, GTM_Sequence nextval, bool iscalled)
{
	GTM_SequenceKeyData seqkey;
	char   *coordName = IS_PGXC_COORDINATOR ? PGXCNodeName : GetMyCoordName;
	int		coordPid = IS_PGXC_COORDINATOR ? MyProcPid : MyCoordPid;

	CheckConnection();
	seqkey.gsk_keylen = strlen(seqname) + 1;
	seqkey.gsk_key = seqname;

	return conn ? set_val(conn, &seqkey, coordName, coordPid, nextval, iscalled) : -1;
}

/*
 * Drop the sequence depending the key type
 *
 * Type of Sequence name use in key;
 *		GTM_SEQ_FULL_NAME, full name of sequence
 *		GTM_SEQ_DB_NAME, DB name part of sequence key
 */
int
DropSequenceGTM(char *name, GTM_SequenceKeyType type)
{
	GTM_SequenceKeyData seqkey;
	CheckConnection();
	seqkey.gsk_keylen = strlen(name) + 1;
	seqkey.gsk_key = name;
	seqkey.gsk_type = type;

	return conn ? close_sequence(conn, &seqkey, GetTopTransactionId()) : -1;
}

/*
 * Rename the sequence
 */
int
RenameSequenceGTM(char *seqname, const char *newseqname)
{
	GTM_SequenceKeyData seqkey, newseqkey;
	CheckConnection();
	seqkey.gsk_keylen = strlen(seqname) + 1;
	seqkey.gsk_key = seqname;
	newseqkey.gsk_keylen = strlen(newseqname) + 1;
	newseqkey.gsk_key = (char *) newseqname;

	return conn ? rename_sequence(conn, &seqkey, &newseqkey,
			GetTopTransactionId()) : -1;
}

/*
 * Register Given Node
 * Connection for registering is just used once then closed
 */
int
RegisterGTM(GTM_PGXCNodeType type)
{
	int ret;

	CheckConnection();

	if (!conn)
		return EOF;

	ret = node_register(conn, type, 0, PGXCNodeName, "");

	/* If something went wrong, retry once */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret = node_register(conn, type, 0, PGXCNodeName, "");
	}

	return ret;
}

/*
 * UnRegister Given Node
 * Connection for registering is just used once then closed
 */
int
UnregisterGTM(GTM_PGXCNodeType type)
{
	int ret;

	CheckConnection();

	if (!conn)
		return EOF;

	ret = node_unregister(conn, type, PGXCNodeName);

	/* If something went wrong, retry once */
	if (ret < 0)
	{
		CloseGTM();
		InitGTM();
		if (conn)
			ret = node_unregister(conn, type, PGXCNodeName);
	}

	/*
	 * If node is unregistered cleanly, cut the connection.
	 * and Node shuts down smoothly.
	 */
	CloseGTM();

	return ret;
}

/*
 * Report BARRIER
 */
int
ReportBarrierGTM(const char *barrier_id)
{
	if (!gtm_backup_barrier)
		return EINVAL;

	CheckConnection();

	if (!conn)
		return EOF;

	return(report_barrier(conn, barrier_id));
}

int
ReportGlobalXmin(GlobalTransactionId gxid, GlobalTransactionId *global_xmin,
		GlobalTransactionId *latest_completed_xid)
{
	int errcode = GTM_ERRCODE_UNKNOWN;

	CheckConnection();
	if (!conn)
		return EOF;

	report_global_xmin(conn, PGXCNodeName,
			IS_PGXC_COORDINATOR ?  GTM_NODE_COORDINATOR : GTM_NODE_DATANODE,
			gxid, global_xmin, latest_completed_xid, &errcode);
	return errcode;
}
