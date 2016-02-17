/*-------------------------------------------------------------------------
 *
 * gtm.h
 * 
 *	  Module interfacing with GTM definitions
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef ACCESS_GTM_H
#define ACCESS_GTM_H

#include "gtm/gtm_c.h"

/* Configuration variables */
extern char *GtmHost;
extern int GtmPort;
extern bool gtm_backup_barrier;

extern bool IsXidFromGTM;
extern GlobalTransactionId currentGxid;

extern bool IsGTMConnected(void);
extern void InitGTM(void);
extern void CloseGTM(void);
extern GlobalTransactionId BeginTranGTM(GTM_Timestamp *timestamp, const char *globalSession);
extern GlobalTransactionId BeginTranAutovacuumGTM(void);
extern int CommitTranGTM(GlobalTransactionId gxid, int waited_xid_count,
		GlobalTransactionId *waited_xids);
extern int RollbackTranGTM(GlobalTransactionId gxid);
extern int StartPreparedTranGTM(GlobalTransactionId gxid,
								char *gid,
								char *nodestring);
extern int PrepareTranGTM(GlobalTransactionId gxid);
extern int GetGIDDataGTM(char *gid,
						 GlobalTransactionId *gxid,
						 GlobalTransactionId *prepared_gxid,
						 char **nodestring);
extern int CommitPreparedTranGTM(GlobalTransactionId gxid,
								 GlobalTransactionId prepared_gxid,
								 int waited_xid_count,
								 GlobalTransactionId *waited_xids);

extern GTM_Snapshot GetSnapshotGTM(GlobalTransactionId gxid, bool canbe_grouped);

/* Node registration APIs with GTM */
extern int RegisterGTM(GTM_PGXCNodeType type);
extern int UnregisterGTM(GTM_PGXCNodeType type);

/* Sequence interface APIs with GTM */
extern GTM_Sequence GetCurrentValGTM(char *seqname);
extern GTM_Sequence GetNextValGTM(char *seqname,
					GTM_Sequence range, GTM_Sequence *rangemax);
extern int SetValGTM(char *seqname, GTM_Sequence nextval, bool iscalled);
extern int CreateSequenceGTM(char *seqname, GTM_Sequence increment, 
		GTM_Sequence minval, GTM_Sequence maxval, GTM_Sequence startval,
		bool cycle);
extern int AlterSequenceGTM(char *seqname, GTM_Sequence increment,
		GTM_Sequence minval, GTM_Sequence maxval, GTM_Sequence startval,
							GTM_Sequence lastval, bool cycle, bool is_restart);
extern int DropSequenceGTM(char *name, GTM_SequenceKeyType type);
extern int RenameSequenceGTM(char *seqname, const char *newseqname);
/* Barrier */
extern int ReportBarrierGTM(const char *barrier_id);
extern int ReportGlobalXmin(GlobalTransactionId gxid,
		GlobalTransactionId *global_xmin,
		GlobalTransactionId *latest_completed_xid);
#endif /* ACCESS_GTM_H */
