/*-------------------------------------------------------------------------
 *
 * procarray.h
 *	  POSTGRES process array definitions.
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/storage/procarray.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PROCARRAY_H
#define PROCARRAY_H

#include "storage/standby.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

#ifdef XCP
extern int GlobalSnapshotSource;

typedef enum GlobalSnapshotSourceType
{
	GLOBAL_SNAPSHOT_SOURCE_GTM,
	GLOBAL_SNAPSHOT_SOURCE_COORDINATOR
} GlobalSnapshotSourceType;
#endif

extern Size ProcArrayShmemSize(void);
extern void CreateSharedProcArray(void);
extern void ProcArrayAdd(PGPROC *proc);
extern void ProcArrayRemove(PGPROC *proc, TransactionId latestXid);

extern void ProcArrayEndTransaction(PGPROC *proc, TransactionId latestXid);
extern void ProcArrayClearTransaction(PGPROC *proc);

#ifdef PGXC  /* PGXC_DATANODE */
typedef enum
{
	SNAPSHOT_UNDEFINED,   /* Coordinator has not sent snapshot or not yet connected */
	SNAPSHOT_LOCAL,       /* Coordinator has instructed Datanode to build up snapshot from the local procarray */
	SNAPSHOT_COORDINATOR, /* Coordinator has sent snapshot data */
	SNAPSHOT_DIRECT       /* Datanode obtained directly from GTM */
} SnapshotSource;

extern void SetGlobalSnapshotData(TransactionId xmin, TransactionId xmax, int xcnt,
		TransactionId *xip,
		SnapshotSource source);
extern void UnsetGlobalSnapshotData(void);
extern void ReloadConnInfoOnBackends(void);
#endif /* PGXC */
extern void ProcArrayInitRecovery(TransactionId initializedUptoXID);
extern void ProcArrayApplyRecoveryInfo(RunningTransactions running);
extern void ProcArrayApplyXidAssignment(TransactionId topxid,
							int nsubxids, TransactionId *subxids);

extern void RecordKnownAssignedTransactionIds(TransactionId xid);
extern void ExpireTreeKnownAssignedTransactionIds(TransactionId xid,
									  int nsubxids, TransactionId *subxids,
									  TransactionId max_xid);
extern void ExpireAllKnownAssignedTransactionIds(void);
extern void ExpireOldKnownAssignedTransactionIds(TransactionId xid);

extern int	GetMaxSnapshotXidCount(void);
extern int	GetMaxSnapshotSubxidCount(void);

extern Snapshot GetSnapshotData(Snapshot snapshot, bool latest);

extern bool ProcArrayInstallImportedXmin(TransactionId xmin,
							 TransactionId sourcexid);
extern bool ProcArrayInstallRestoredXmin(TransactionId xmin, PGPROC *proc);
extern void ProcArrayCheckXminConsistency(TransactionId global_xmin);
extern void SetLatestCompletedXid(TransactionId latestCompletedXid);

extern RunningTransactions GetRunningTransactionData(void);

extern bool TransactionIdIsInProgress(TransactionId xid);
extern bool TransactionIdIsActive(TransactionId xid);
extern TransactionId GetOldestXmin(Relation rel, bool ignoreVacuum);
extern TransactionId GetOldestXminInternal(Relation rel, bool ignoreVacuum,
		bool computeLocal, TransactionId lastGlobalXmin);
extern TransactionId GetOldestActiveTransactionId(void);
extern TransactionId GetOldestSafeDecodingTransactionId(void);

extern VirtualTransactionId *GetVirtualXIDsDelayingChkpt(int *nvxids);
extern bool HaveVirtualXIDsDelayingChkpt(VirtualTransactionId *vxids, int nvxids);

extern PGPROC *BackendPidGetProc(int pid);
extern int	BackendXidGetPid(TransactionId xid);
extern bool IsBackendPid(int pid);

extern VirtualTransactionId *GetCurrentVirtualXIDs(TransactionId limitXmin,
					  bool excludeXmin0, bool allDbs, int excludeVacuum,
					  int *nvxids);
extern VirtualTransactionId *GetConflictingVirtualXIDs(TransactionId limitXmin, Oid dbOid);
extern pid_t CancelVirtualTransaction(VirtualTransactionId vxid, ProcSignalReason sigmode);

extern bool MinimumActiveBackends(int min);
extern int	CountDBBackends(Oid databaseid);
extern void CancelDBBackends(Oid databaseid, ProcSignalReason sigmode, bool conflictPending);
extern int	CountUserBackends(Oid roleid);
extern bool CountOtherDBBackends(Oid databaseId,
					 int *nbackends, int *nprepared);

extern void XidCacheRemoveRunningXids(TransactionId xid,
						  int nxids, const TransactionId *xids,
						  TransactionId latestXid);
#ifdef XCP
extern void GetGlobalSessionInfo(int pid, Oid *coordId, int *coordPid);
extern int	GetFirstBackendId(int *numBackends, int *backends);
#endif /* XCP */

extern void ProcArraySetReplicationSlotXmin(TransactionId xmin,
							TransactionId catalog_xmin, bool already_locked);

extern void ProcArrayGetReplicationSlotXmin(TransactionId *xmin,
								TransactionId *catalog_xmin);

#endif   /* PROCARRAY_H */
