/*-------------------------------------------------------------------------
 *
 * poolmgr.h
 *
 *	  Definitions for the Datanode connection pool.
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/poolmgr.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POOLMGR_H
#define POOLMGR_H
#include <sys/time.h>
#include "nodes/nodes.h"
#include "pgxcnode.h"
#include "poolcomm.h"
#include "storage/pmsignal.h"
#include "utils/guc.h"
#include "utils/hsearch.h"

#define MAX_IDLE_TIME 60

/* Connection pool entry */
typedef struct
{
	time_t		released;
	NODE_CONNECTION *conn;
	NODE_CANCEL	*xc_cancelConn;
} PGXCNodePoolSlot;

/* Pool of connections to specified pgxc node */
typedef struct
{
	Oid			nodeoid;	/* Node Oid related to this pool */
	char	   *connstr;
	int			freeSize;	/* available connections */
	int			size;  		/* total pool size */
	PGXCNodePoolSlot **slot;
} PGXCNodePool;

/* All pools for specified database */
typedef struct databasepool
{
	char	   *database;
	char	   *user_name;
	char	   *pgoptions;		/* Connection options */
	HTAB	   *nodePools; 		/* Hashtable of PGXCNodePool, one entry for each
								 * Coordinator or DataNode */
	MemoryContext mcxt;
	struct databasepool *next; 	/* Reference to next to organize linked list */
	time_t		oldest_idle;
} DatabasePool;

/*
 * Agent of client session (Pool Manager side)
 * Acts as a session manager, grouping connections together
 * and managing session parameters
 */
typedef struct
{
	/* Process ID of postmaster child process associated to pool agent */
	int				pid;
	/* communication channel */
	PoolPort		port;
	DatabasePool   *pool;
	MemoryContext	mcxt;
	int				num_dn_connections;
	int				num_coord_connections;
	Oid		   	   *dn_conn_oids;		/* one for each Datanode */
	Oid		   	   *coord_conn_oids;	/* one for each Coordinator */
	PGXCNodePoolSlot **dn_connections; /* one for each Datanode */
	PGXCNodePoolSlot **coord_connections; /* one for each Coordinator */
} PoolAgent;
/*
 * Helper to poll for all pooler sockets
 */
typedef struct pollfd Pollfd;


extern int	PoolConnKeepAlive;
extern int	PoolMaintenanceTimeout;
extern int	MaxPoolSize;
extern int	PoolerPort;

extern bool PersistentConnections;

/* Status inquiry functions */
extern void PGXCPoolerProcessIam(void);
extern bool IsPGXCPoolerProcess(void);

/* Initialize internal structures */
extern int	PoolManagerInit(void);

/* Destroy internal structures */
extern int	PoolManagerDestroy(void);

/*
 * Gracefully close connection to the PoolManager
 */
extern void PoolManagerDisconnect(void);
extern char *session_options(void);

/*
 * Reconnect to pool manager
 * This simply does a disconnection followed by a reconnection.
 */
extern void PoolManagerReconnect(void);

/* Get pooled connections */
extern int *PoolManagerGetConnections(List *datanodelist, List *coordlist,
		int **pids);

/* Clean pool connections */
extern void PoolManagerCleanConnection(List *datanodelist, List *coordlist, char *dbname, char *username);

/* Check consistency of connection information cached in pooler with catalogs */
extern bool PoolManagerCheckConnectionInfo(void);

/* Reload connection data in pooler and drop all the existing connections of pooler */
extern void PoolManagerReloadConnectionInfo(void);

/* Send Abort signal to transactions being run */
extern int	PoolManagerAbortTransactions(char *dbname, char *username, int **proc_pids);

/* Return connections back to the pool, for both Coordinator and Datanode connections */
extern void PoolManagerReleaseConnections(bool destroy);

/* Cancel a running query on Datanodes as well as on other Coordinators */
extern void PoolManagerCancelQuery(int dn_count, int* dn_list, int co_count, int* co_list);

/* Lock/unlock pool manager */
extern void PoolManagerLock(bool is_lock);

/* Do pool health check activity */
extern void PoolPingNodes(void);
extern void PoolPingNodeRecheck(Oid nodeoid);

extern bool check_persistent_connections(bool *newval, void **extra,
		GucSource source);
#endif
