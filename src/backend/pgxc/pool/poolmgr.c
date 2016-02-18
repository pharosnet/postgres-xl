/*-------------------------------------------------------------------------
 *
 * poolmgr.c
 *
 *	  Connection pool manager handles connections to Datanodes
 *
 * The pooler runs as a separate process and is forked off from a
 * Coordinator postmaster. If the Coordinator needs a connection from a
 * Datanode, it asks for one from the pooler, which maintains separate
 * pools for each Datanode. A group of connections can be requested in
 * a single request, and the pooler returns a list of file descriptors
 * to use for the connections.
 *
 * Note the current implementation does not yet shrink the pool over time
 * as connections are idle.  Also, it does not queue requests; if a
 * connection is unavailable, it will simply fail. This should be implemented
 * one day, although there is a chance for deadlocks. For now, limiting
 * connections should be done between the application and Coordinator.
 * Still, this is useful to avoid having to re-establish connections to the
 * Datanodes all the time for multiple Coordinator backend sessions.
 *
 * The term "agent" here refers to a session manager, one for each backend
 * Coordinator connection to the pooler. It will contain a list of connections
 * allocated to a session, at most one per Datanode.
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <signal.h>
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "catalog/pgxc_node.h"
#include "commands/dbcommands.h"
#include "nodes/nodes.h"
#include "pgxc/poolmgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/resowner.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolutils.h"
#include "../interfaces/libpq/libpq-fe.h"
#include "../interfaces/libpq/libpq-int.h"
#include "postmaster/postmaster.h"		/* For UnixSocketDir */
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <poll.h>
#include "pgxc/pause.h"
#include "storage/procarray.h"

/* Configuration options */
int			PoolConnKeepAlive = 600;
int			PoolMaintenanceTimeout = 30;
int			MaxPoolSize = 100;
int			PoolerPort = 6667;

bool			PersistentConnections = false;

/* Flag to tell if we are Postgres-XC pooler process */
static bool am_pgxc_pooler = false;

/* Connection information cached */
typedef struct
{
	Oid	nodeoid;
	char	*host;
	int	port;
} PGXCNodeConnectionInfo;

/* Handle to the pool manager (Session's side) */
typedef struct
{
	/* communication channel */
	PoolPort	port;
} PoolHandle;

/* The root memory context */
static MemoryContext PoolerMemoryContext = NULL;
/*
 * Allocations of core objects: Datanode connections, upper level structures,
 * connection strings, etc.
 */
static MemoryContext PoolerCoreContext = NULL;
/*
 * Memory to store Agents
 */
static MemoryContext PoolerAgentContext = NULL;

/* Pool to all the databases (linked list) */
static DatabasePool *databasePools = NULL;

/* PoolAgents and the poll array*/
static int	agentCount = 0;
static PoolAgent **poolAgents;

static PoolHandle *poolHandle = NULL;

static int	is_pool_locked = false;
static int	server_fd = -1;

static int	node_info_check(PoolAgent *agent);
static void agent_init(PoolAgent *agent, const char *database, const char *user_name,
	                   const char *pgoptions);
static void agent_destroy(PoolAgent *agent);
static void agent_create(void);
static void agent_handle_input(PoolAgent *agent, StringInfo s);
static DatabasePool *create_database_pool(const char *database, const char *user_name, const char *pgoptions);
static void insert_database_pool(DatabasePool *pool);
static int	destroy_database_pool(const char *database, const char *user_name);
static void reload_database_pools(PoolAgent *agent);
static DatabasePool *find_database_pool(const char *database, const char *user_name, const char *pgoptions);
static DatabasePool *remove_database_pool(const char *database, const char *user_name);
static int *agent_acquire_connections(PoolAgent *agent, List *datanodelist,
		List *coordlist, int **connectionpids);
static int cancel_query_on_connections(PoolAgent *agent, List *datanodelist, List *coordlist);
static PGXCNodePoolSlot *acquire_connection(DatabasePool *dbPool, Oid node);
static void agent_release_connections(PoolAgent *agent, bool force_destroy);
static void release_connection(DatabasePool *dbPool, PGXCNodePoolSlot *slot,
							   Oid node, bool force_destroy);
static void destroy_slot(PGXCNodePoolSlot *slot);
static PGXCNodePool *grow_pool(DatabasePool *dbPool, Oid node);
static void destroy_node_pool(PGXCNodePool *node_pool);
static void PoolerLoop(void);
static int clean_connection(List *node_discard,
							const char *database,
							const char *user_name);
static int *abort_pids(int *count,
					   int pid,
					   const char *database,
					   const char *user_name);
static char *build_node_conn_str(Oid node, DatabasePool *dbPool);
/* Signal handlers */
static void pooler_die(SIGNAL_ARGS);
static void pooler_quickdie(SIGNAL_ARGS);
static void PoolManagerConnect(const char *database, const char *user_name,
		const char *pgoptions);
static void pooler_sighup(SIGNAL_ARGS);
static bool shrink_pool(DatabasePool *pool);
static void pools_maintenance(void);
static void TryPingUnhealthyNode(Oid nodeoid);

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t shutdown_requested = false;

void
PGXCPoolerProcessIam(void)
{
	am_pgxc_pooler = true;
}

bool
IsPGXCPoolerProcess(void)
{
    return am_pgxc_pooler;
}

/*
 * Initialize internal structures
 */
int
PoolManagerInit()
{
	elog(DEBUG1, "Pooler process is started: %d", getpid());

	/*
	 * Set up memory contexts for the pooler objects
	 */
	PoolerMemoryContext = AllocSetContextCreate(TopMemoryContext,
												"PoolerMemoryContext",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);
	PoolerCoreContext = AllocSetContextCreate(PoolerMemoryContext,
											  "PoolerCoreContext",
											  ALLOCSET_DEFAULT_MINSIZE,
											  ALLOCSET_DEFAULT_INITSIZE,
											  ALLOCSET_DEFAULT_MAXSIZE);
	PoolerAgentContext = AllocSetContextCreate(PoolerMemoryContext,
											   "PoolerAgentContext",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	ForgetLockFiles();	

	/*
	 * Properly accept or ignore signals the postmaster might send us
	 */
	pqsignal(SIGINT, pooler_die);
	pqsignal(SIGTERM, pooler_die);
	pqsignal(SIGQUIT, pooler_quickdie);
	pqsignal(SIGHUP, pooler_sighup);
	/* TODO other signal handlers */

	/* We allow SIGQUIT (quickdie) at all times */
#ifdef HAVE_SIGPROCMASK
	sigdelset(&BlockSig, SIGQUIT);
#else
	BlockSig &= ~(sigmask(SIGQUIT));
#endif

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	/* Allocate pooler structures in the Pooler context */
	MemoryContextSwitchTo(PoolerMemoryContext);

	poolAgents = (PoolAgent **) palloc(MaxConnections * sizeof(PoolAgent *));
	if (poolAgents == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory while initializing pool agents")));
	}

	PoolerLoop();
	return 0;
}


/*
 * Check connection info consistency with system catalogs
 */
static int
node_info_check(PoolAgent *agent)
{
	DatabasePool   *dbPool = databasePools;
	List 		   *checked = NIL;
	int 			res = POOL_CHECK_SUCCESS;
	Oid			   *coOids;
	Oid			   *dnOids;
	int				numCo;
	int				numDn;

	/*
	 * First check if agent's node information matches to current content of the
	 * shared memory table.
	 */
	PgxcNodeGetOids(&coOids, &dnOids, &numCo, &numDn, false);

	if (agent->num_coord_connections != numCo ||
			agent->num_dn_connections != numDn ||
			memcmp(agent->coord_conn_oids, coOids, numCo * sizeof(Oid)) ||
			memcmp(agent->dn_conn_oids, dnOids, numDn * sizeof(Oid)))
		res = POOL_CHECK_FAILED;

	/* Release palloc'ed memory */
	pfree(coOids);
	pfree(dnOids);

	/*
	 * Iterate over all dbnode pools and check if connection strings
	 * are matching node definitions.
	 */
	while (res == POOL_CHECK_SUCCESS && dbPool)
	{
		HASH_SEQ_STATUS hseq_status;
		PGXCNodePool   *nodePool;

		hash_seq_init(&hseq_status, dbPool->nodePools);
		while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
		{
			char 		   *connstr_chk;

			/* No need to check same Datanode twice */
			if (list_member_oid(checked, nodePool->nodeoid))
				continue;
			checked = lappend_oid(checked, nodePool->nodeoid);

			connstr_chk = build_node_conn_str(nodePool->nodeoid, dbPool);
			if (connstr_chk == NULL)
			{
				/* Problem of constructing connection string */
				hash_seq_term(&hseq_status);
				res = POOL_CHECK_FAILED;
				break;
			}
			/* return error if there is difference */
			if (strcmp(connstr_chk, nodePool->connstr))
			{
				pfree(connstr_chk);
				hash_seq_term(&hseq_status);
				res = POOL_CHECK_FAILED;
				break;
			}

			pfree(connstr_chk);
		}
		dbPool = dbPool->next;
	}
	list_free(checked);
	return res;
}

/*
 * Destroy internal structures
 */
int
PoolManagerDestroy(void)
{
	int			status = 0;

	if (PoolerMemoryContext)
	{
		MemoryContextDelete(PoolerMemoryContext);
		PoolerMemoryContext = NULL;
	}

	return status;
}

/*
 * Connect to the pooler process
 */
static void
GetPoolManagerHandle(void)
{
	PoolHandle *handle;
	int			fdsock;

	if (poolHandle)
		/* already connected */
		return;

#ifdef HAVE_UNIX_SOCKETS
	if (Unix_socket_directories)
	{
		char	   *rawstring;
		List	   *elemlist;
		ListCell   *l;
		int			success = 0;

		/* Need a modifiable copy of Unix_socket_directories */
		rawstring = pstrdup(Unix_socket_directories);

		/* Parse string into list of directories */
		if (!SplitDirectoriesString(rawstring, ',', &elemlist))
		{
			/* syntax error in list */
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid list syntax in parameter \"%s\"",
							"unix_socket_directories")));
		}

		foreach(l, elemlist)
		{
			char	   *socketdir = (char *) lfirst(l);
			int			saved_errno;

			/* Connect to the pooler */
			fdsock = pool_connect(PoolerPort, socketdir);
			if (fdsock < 0)
			{
				saved_errno = errno;
				ereport(WARNING,
						(errmsg("could not create Unix-domain socket in directory \"%s\", errno: %d",
								socketdir, saved_errno)));
			}
			else
			{
				success++;
				break;
			}
		}

		if (!success && elemlist != NIL)
			ereport(ERROR,
					(errmsg("failed to connect to pool manager: %m")));

		list_free_deep(elemlist);
		pfree(rawstring);
	}
#endif

	/* Allocate handle */
	/*
	 * XXX we may change malloc here to palloc but first ensure
	 * the CurrentMemoryContext is properly set.
	 * The handle allocated just before new session is forked off and
	 * inherited by the session process. It should remain valid for all
	 * the session lifetime.
	 */
	handle = (PoolHandle *) malloc(sizeof(PoolHandle));
	if (!handle)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	handle->port.fdsock = fdsock;
	handle->port.RecvLength = 0;
	handle->port.RecvPointer = 0;
	handle->port.SendPointer = 0;

	poolHandle = handle;
}

/*
 * Create agent
 */
static void
agent_create(void)
{
	MemoryContext oldcontext;
	int			new_fd;
	PoolAgent  *agent;

	new_fd = accept(server_fd, NULL, NULL);
	if (new_fd < 0)
	{
		int			saved_errno = errno;

		ereport(LOG,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("pool manager failed to accept connection: %m")));
		errno = saved_errno;
		return;
	}

	oldcontext = MemoryContextSwitchTo(PoolerAgentContext);

	/* Allocate agent */
	agent = (PoolAgent *) palloc(sizeof(PoolAgent));
	if (!agent)
	{
		close(new_fd);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		return;
	}

	agent->port.fdsock = new_fd;
	agent->port.RecvLength = 0;
	agent->port.RecvPointer = 0;
	agent->port.SendPointer = 0;
	agent->pool = NULL;
	agent->mcxt = AllocSetContextCreate(CurrentMemoryContext,
										"Agent",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);
	agent->num_dn_connections = 0;
	agent->num_coord_connections = 0;
	agent->dn_conn_oids = NULL;
	agent->coord_conn_oids = NULL;
	agent->dn_connections = NULL;
	agent->coord_connections = NULL;
	agent->pid = 0;

	/* Append new agent to the list */
	poolAgents[agentCount++] = agent;

	MemoryContextSwitchTo(oldcontext);
}


/*
 * session_options
 * Returns the pgoptions string generated using a particular
 * list of parameters that are required to be propagated to Datanodes.
 * These parameters then become default values for the pooler sessions.
 * For e.g., a psql user sets PGDATESTYLE. This value should be set
 * as the default connection parameter in the pooler session that is
 * connected to the Datanodes. There are various parameters which need to
 * be analysed individually to determine whether these should be set on
 * Datanodes.
 *
 * Note: These parameters values are the default values of the particular
 * Coordinator backend session, and not the new values set by SET command.
 *
 */

char *session_options(void)
{
	int				 i;
	char			*pgoptions[] = {"DateStyle", "timezone", "geqo", "intervalstyle", "lc_monetary"};
	StringInfoData	 options;
	List			*value_list;
	ListCell		*l;

	initStringInfo(&options);

	for (i = 0; i < sizeof(pgoptions)/sizeof(char*); i++)
	{
		const char		*value;

		appendStringInfo(&options, " -c %s=", pgoptions[i]);

		value = GetConfigOptionResetString(pgoptions[i]);

		/* lc_monetary does not accept lower case values */
		if (strcmp(pgoptions[i], "lc_monetary") == 0)
		{
			appendStringInfoString(&options, value);
			continue;
		}

		SplitIdentifierString(strdup(value), ',', &value_list);
		foreach(l, value_list)
		{
			char *value = (char *) lfirst(l);
			appendStringInfoString(&options, value);
			if (lnext(l))
				appendStringInfoChar(&options, ',');
		}
	}

	return options.data;
}


/*
 * Associate session with specified database and respective connection pool
 * Invoked from Session process
 */
static void
PoolManagerConnect(const char *database, const char *user_name,
		const char *pgoptions)
{
	int 	n32;
	char 	msgtype = 'c';
	int 	unamelen = strlen(user_name);
	int 	dbnamelen = strlen(database);
	int		pgoptionslen = strlen(pgoptions);
	char	atchar = ' ';

	/* Connect to the pooler process if not yet connected */
	GetPoolManagerHandle();
	if (poolHandle == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to connect to the pooler process")));

	elog(DEBUG1, "Connecting to PoolManager (user_name %s, database %s, "
			"pgoptions %s", user_name, database, pgoptions);

	/*
	 * Special handling for db_user_namespace=on
	 * We need to handle per-db users and global users. The per-db users will
	 * arrive with @dbname and global users just as username. Handle both of
	 * them appropriately
	 */
	if (strcmp(GetConfigOption("db_user_namespace", false, false), "on") == 0)
	{
		if (strchr(user_name, '@') != NULL)
		{
			Assert(unamelen > dbnamelen + 1);
			unamelen -= (dbnamelen + 1);
		}
		else
		{
			atchar = '@';
			unamelen++;
		}
	}

	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	n32 = htonl(dbnamelen + unamelen + pgoptionslen + 23);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* PID number */
	n32 = htonl(MyProcPid);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Length of Database string */
	n32 = htonl(dbnamelen + 1);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send database name followed by \0 terminator */
	pool_putbytes(&poolHandle->port, database, dbnamelen);
	pool_putbytes(&poolHandle->port, "\0", 1);

	/* Length of user name string */
	n32 = htonl(unamelen + 1);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send user name followed by \0 terminator */
	/* Send the '@' char if needed. Already accounted for in len */
	if (atchar == '@')
	{
		pool_putbytes(&poolHandle->port, user_name, unamelen - 1);
		pool_putbytes(&poolHandle->port, "@", 1);
	}
	else
		pool_putbytes(&poolHandle->port, user_name, unamelen);
	pool_putbytes(&poolHandle->port, "\0", 1);

	/* Length of pgoptions string */
	n32 = htonl(pgoptionslen + 1);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send pgoptions followed by \0 terminator */
	pool_putbytes(&poolHandle->port, pgoptions, pgoptionslen);
	pool_putbytes(&poolHandle->port, "\0", 1);
	pool_flush(&poolHandle->port);
}

/*
 * Reconnect to pool manager
 * It simply does a disconnection and a reconnection.
 */
void
PoolManagerReconnect(void)
{
	elog(DEBUG1, "Reconnecting to PoolManager");

	/* Connected, disconnect */
	if (poolHandle)
		PoolManagerDisconnect();

	PoolManagerConnect(get_database_name(MyDatabaseId), GetClusterUserName(),
			session_options());
}

/*
 * Lock/unlock pool manager
 * During locking, the only operations not permitted are abort, connection and
 * connection obtention.
 */
void
PoolManagerLock(bool is_lock)
{
	char msgtype = 'o';
	int n32;
	int msglen = 8;
	if (poolHandle == NULL)
		PoolManagerConnect(get_database_name(MyDatabaseId),
						   GetClusterUserName(), "");

	elog(DEBUG1, "Locking PoolManager");

	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	n32 = htonl(msglen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Lock information */
	n32 = htonl((int) is_lock);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);
	pool_flush(&poolHandle->port);
}

/*
 * Init PoolAgent
 */
static void
agent_init(PoolAgent *agent, const char *database, const char *user_name,
           const char *pgoptions)
{
	MemoryContext oldcontext;

	Assert(agent);
	Assert(database);
	Assert(user_name);

	/* disconnect if we are still connected */
	if (agent->pool)
		agent_release_connections(agent, false);

	oldcontext = MemoryContextSwitchTo(agent->mcxt);

	/* Get needed info and allocate memory */
	PgxcNodeGetOids(&agent->coord_conn_oids, &agent->dn_conn_oids,
					&agent->num_coord_connections, &agent->num_dn_connections, false);

	agent->coord_connections = (PGXCNodePoolSlot **)
			palloc0(agent->num_coord_connections * sizeof(PGXCNodePoolSlot *));
	agent->dn_connections = (PGXCNodePoolSlot **)
			palloc0(agent->num_dn_connections * sizeof(PGXCNodePoolSlot *));
	/* find database */
	agent->pool = find_database_pool(database, user_name, pgoptions);

	/* create if not found */
	if (agent->pool == NULL)
		agent->pool = create_database_pool(database, user_name, pgoptions);

	MemoryContextSwitchTo(oldcontext);

	return;
}

/*
 * Destroy PoolAgent
 */
static void
agent_destroy(PoolAgent *agent)
{
	int	i;

	Assert(agent);

	close(Socket(agent->port));

	/* Discard connections if any remaining */
	if (agent->pool)
	{
		/*
		 * If session is disconnecting while there are active connections
		 * we can not know if they clean or not, so force destroy them
		 */
		agent_release_connections(agent, true);
	}

	/* find agent in the list */
	for (i = 0; i < agentCount; i++)
	{
		if (poolAgents[i] == agent)
		{
			/* Free memory. All connection slots are NULL at this point */
			MemoryContextDelete(agent->mcxt);

			pfree(agent);
			/* shrink the list and move last agent into the freed slot */
			if (i < --agentCount)
				poolAgents[i] = poolAgents[agentCount];
			/* only one match is expected so exit */
			break;
		}
	}
}

/*
 * Ping an UNHEALTHY node and if it succeeds, update SHARED node
 * information
 */
static void
TryPingUnhealthyNode(Oid nodeoid)
{
	int status;
	NodeDefinition *nodeDef;
	char connstr[MAXPGPATH * 2 + 256];

	nodeDef = PgxcNodeGetDefinition(nodeoid);
	if (nodeDef == NULL)
	{
		/* No such definition, node dropped? */
		elog(DEBUG1, "Could not find node (%u) definition,"
			 " skipping health check", nodeoid);
		return;
	}
	if (nodeDef->nodeishealthy)
	{
		/* hmm, can this happen? */
		elog(DEBUG1, "node (%u) healthy!"
			 " skipping health check", nodeoid);
		return;
	}

	elog(LOG, "node (%s:%u) down! Trying ping",
		 NameStr(nodeDef->nodename), nodeoid);
	sprintf(connstr,
			"host=%s port=%d", NameStr(nodeDef->nodehost),
			nodeDef->nodeport);
	status = PGXCNodePing(connstr);
	if (status != 0)
	{
		pfree(nodeDef);
		return;
	}

	elog(DEBUG1, "Node (%s) back online!", NameStr(nodeDef->nodename));
	if (!PgxcNodeUpdateHealth(nodeoid, true))
		elog(WARNING, "Could not update health status of node (%s)",
			 NameStr(nodeDef->nodename));
	else
		elog(LOG, "Health map updated to reflect HEALTHY node (%s)",
			 NameStr(nodeDef->nodename));
	pfree(nodeDef);

	return;
}

/*
 * Check if a node is indeed down and if it is update its UNHEALTHY
 * status
 */
void
PoolPingNodeRecheck(Oid nodeoid)
{
	int status;
	NodeDefinition *nodeDef;
	char connstr[MAXPGPATH * 2 + 256];
	bool	healthy;

	nodeDef = PgxcNodeGetDefinition(nodeoid);
	if (nodeDef == NULL)
	{
		/* No such definition, node dropped? */
		elog(DEBUG1, "Could not find node (%u) definition,"
			 " skipping health check", nodeoid);
		return;
	}

	sprintf(connstr,
			"host=%s port=%d", NameStr(nodeDef->nodehost),
			nodeDef->nodeport);
	status = PGXCNodePing(connstr);
	healthy = (status == 0);

	/* if no change in health bit, return */
	if (healthy == nodeDef->nodeishealthy)
	{
		pfree(nodeDef);
		return;
	}

	if (!PgxcNodeUpdateHealth(nodeoid, healthy))
		elog(WARNING, "Could not update health status of node (%s)",
			 NameStr(nodeDef->nodename));
	else
		elog(LOG, "Health map updated to reflect (%s) node (%s)",
			 healthy ? "HEALTHY" : "UNHEALTHY", NameStr(nodeDef->nodename));
	pfree(nodeDef);

	return;
}

/*
 * Ping UNHEALTHY nodes as part of the maintenance window
 */
void
PoolPingNodes()
{
	Oid				coOids[MaxCoords];
	Oid				dnOids[MaxDataNodes];
	bool			coHealthMap[MaxCoords];
	bool			dnHealthMap[MaxDataNodes];
	int				numCo;
	int				numDn;
	int				i;

	PgxcNodeGetHealthMap(coOids, dnOids, &numCo, &numDn,
						 coHealthMap, dnHealthMap);

	/*
	 * Find unhealthy datanodes and try to re-ping them
	 */
	for (i = 0; i < numDn; i++)
	{
		if (!dnHealthMap[i])
		{
			Oid	 nodeoid = dnOids[i];
			TryPingUnhealthyNode(nodeoid);
		}
	}
	/*
	 * Find unhealthy coordinators and try to re-ping them
	 */
	for (i = 0; i < numCo; i++)
	{
		if (!coHealthMap[i])
		{
			Oid	 nodeoid = coOids[i];
			TryPingUnhealthyNode(nodeoid);
		}
	}
}

/*
 * Release handle to pool manager
 */
void
PoolManagerDisconnect(void)
{
	if (!poolHandle)
		return; /* not even connected */

	pool_putmessage(&poolHandle->port, 'd', NULL, 0);
	pool_flush(&poolHandle->port);

	close(Socket(poolHandle->port));
	free(poolHandle);
	poolHandle = NULL;
}


/*
 * Get pooled connections
 */
int *
PoolManagerGetConnections(List *datanodelist, List *coordlist, int **pids)
{
	int			i;
	ListCell   *nodelist_item;
	int		   *fds;
	int			totlen = list_length(datanodelist) + list_length(coordlist);
	int			nodes[totlen + 2];

	if (poolHandle == NULL)
		PoolManagerConnect(get_database_name(MyDatabaseId),
						   GetClusterUserName(), session_options());

	/*
	 * Prepare end send message to pool manager.
	 * First with Datanode list.
	 * This list can be NULL for a query that does not need
	 * Datanode Connections (Sequence DDLs)
	 */
	nodes[0] = htonl(list_length(datanodelist));
	i = 1;
	if (list_length(datanodelist) != 0)
	{
		foreach(nodelist_item, datanodelist)
		{
			nodes[i++] = htonl(lfirst_int(nodelist_item));
		}
	}
	/* Then with Coordinator list (can be nul) */
	nodes[i++] = htonl(list_length(coordlist));
	if (list_length(coordlist) != 0)
	{
		foreach(nodelist_item, coordlist)
		{
			nodes[i++] = htonl(lfirst_int(nodelist_item));
		}
	}

	pool_putmessage(&poolHandle->port, 'g', (char *) nodes, sizeof(int) * (totlen + 2));
	pool_flush(&poolHandle->port);

	/* Receive response */
	fds = (int *) palloc(sizeof(int) * totlen);
	if (fds == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}
	if (pool_recvfds(&poolHandle->port, fds, totlen))
	{
		pfree(fds);
		return NULL;
	}

	if (pool_recvpids(&poolHandle->port, pids) != totlen)
	{
		pfree(*pids);
		*pids = NULL;
		return NULL;
	}

	return fds;
}

/*
 * Abort active transactions using pooler.
 * Take a lock forbidding access to Pooler for new transactions.
 */
int
PoolManagerAbortTransactions(char *dbname, char *username, int **proc_pids)
{
	int		num_proc_ids = 0;
	int		n32, msglen;
	char		msgtype = 'a';
	int		dblen = dbname ? strlen(dbname) + 1 : 0;
	int		userlen = username ? strlen(username) + 1 : 0;

	/*
	 * New connection may be established to clean connections to
	 * specified nodes and databases.
	 */
	if (poolHandle == NULL)
		PoolManagerConnect(get_database_name(MyDatabaseId),
						   GetClusterUserName(), session_options());

	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	msglen = dblen + userlen + 12;
	n32 = htonl(msglen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Length of Database string */
	n32 = htonl(dblen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send database name, followed by \0 terminator if necessary */
	if (dbname)
		pool_putbytes(&poolHandle->port, dbname, dblen);

	/* Length of Username string */
	n32 = htonl(userlen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send user name, followed by \0 terminator if necessary */
	if (username)
		pool_putbytes(&poolHandle->port, username, userlen);

	pool_flush(&poolHandle->port);

	/* Then Get back Pids from Pooler */
	num_proc_ids = pool_recvpids(&poolHandle->port, proc_pids);

	return num_proc_ids;
}


/*
 * Clean up Pooled connections
 */
void
PoolManagerCleanConnection(List *datanodelist, List *coordlist, char *dbname, char *username)
{
	int			totlen = list_length(datanodelist) + list_length(coordlist);
	int			nodes[totlen + 2];
	ListCell		*nodelist_item;
	int			i, n32, msglen;
	char			msgtype = 'f';
	int			userlen = username ? strlen(username) + 1 : 0;
	int			dblen = dbname ? strlen(dbname) + 1 : 0;

	/*
	 * New connection may be established to clean connections to
	 * specified nodes and databases.
	 */
	if (poolHandle == NULL)
		PoolManagerConnect(get_database_name(MyDatabaseId),
						   GetClusterUserName(), session_options());

	nodes[0] = htonl(list_length(datanodelist));
	i = 1;
	if (list_length(datanodelist) != 0)
	{
		foreach(nodelist_item, datanodelist)
		{
			nodes[i++] = htonl(lfirst_int(nodelist_item));
		}
	}
	/* Then with Coordinator list (can be nul) */
	nodes[i++] = htonl(list_length(coordlist));
	if (list_length(coordlist) != 0)
	{
		foreach(nodelist_item, coordlist)
		{
			nodes[i++] = htonl(lfirst_int(nodelist_item));
		}
	}

	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	msglen = sizeof(int) * (totlen + 2) + dblen + userlen + 12;
	n32 = htonl(msglen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send list of nodes */
	pool_putbytes(&poolHandle->port, (char *) nodes, sizeof(int) * (totlen + 2));

	/* Length of Database string */
	n32 = htonl(dblen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send database name, followed by \0 terminator if necessary */
	if (dbname)
		pool_putbytes(&poolHandle->port, dbname, dblen);

	/* Length of Username string */
	n32 = htonl(userlen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send user name, followed by \0 terminator if necessary */
	if (username)
		pool_putbytes(&poolHandle->port, username, userlen);

	pool_flush(&poolHandle->port);

	/* Receive result message */
	if (pool_recvres(&poolHandle->port) != CLEAN_CONNECTION_COMPLETED)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Clean connections not completed")));
}


/*
 * Check connection information consistency cached in pooler with catalog information
 */
bool
PoolManagerCheckConnectionInfo(void)
{
	int res;

	/*
	 * New connection may be established to clean connections to
	 * specified nodes and databases.
	 */
	if (poolHandle == NULL)
		PoolManagerConnect(get_database_name(MyDatabaseId),
						   GetClusterUserName(), session_options());
	PgxcNodeListAndCount();
	pool_putmessage(&poolHandle->port, 'q', NULL, 0);
	pool_flush(&poolHandle->port);

	res = pool_recvres(&poolHandle->port);

	if (res == POOL_CHECK_SUCCESS)
		return true;

	return false;
}


/*
 * Reload connection data in pooler and drop all the existing connections of pooler
 */
void
PoolManagerReloadConnectionInfo(void)
{
	Assert(poolHandle);
	PgxcNodeListAndCount();
	pool_putmessage(&poolHandle->port, 'p', NULL, 0);
	pool_flush(&poolHandle->port);
}


/*
 * Handle messages to agent
 */
static void
agent_handle_input(PoolAgent * agent, StringInfo s)
{
	int			qtype;

	qtype = pool_getbyte(&agent->port);
	/*
	 * We can have multiple messages, so handle them all
	 */
	for (;;)
	{
		const char *database = NULL;
		const char *user_name = NULL;
		const char *pgoptions = NULL;
		int			datanodecount;
		int			coordcount;
		List	   *nodelist = NIL;
		List	   *datanodelist = NIL;
		List	   *coordlist = NIL;
		int		   *fds;
		int		   *pids;
		int			i, len, res;

		/*
		 * During a pool cleaning, Abort, Connect and Get Connections messages
		 * are not allowed on pooler side.
		 * It avoids to have new backends taking connections
		 * while remaining transactions are aborted during FORCE and then
		 * Pools are being shrinked.
		 */
		if (is_pool_locked && (qtype == 'a' || qtype == 'c' || qtype == 'g'))
			elog(WARNING,"Pool operation cannot run during pool lock");

		elog(DEBUG1, "Pooler is handling command %c from %d", (char) qtype, agent->pid);

		switch (qtype)
		{
			case 'a':			/* ABORT */
				pool_getmessage(&agent->port, s, 0);
				len = pq_getmsgint(s, 4);
				if (len > 0)
					database = pq_getmsgbytes(s, len);

				len = pq_getmsgint(s, 4);
				if (len > 0)
					user_name = pq_getmsgbytes(s, len);

				pq_getmsgend(s);

				pids = abort_pids(&len, agent->pid, database, user_name);

				pool_sendpids(&agent->port, pids, len);
				if (pids)
					pfree(pids);
				break;
			case 'c':			/* CONNECT */
				pool_getmessage(&agent->port, s, 0);
				agent->pid = pq_getmsgint(s, 4);
				len = pq_getmsgint(s, 4);
				database = pq_getmsgbytes(s, len);
				len = pq_getmsgint(s, 4);
				user_name = pq_getmsgbytes(s, len);
				len = pq_getmsgint(s, 4);
				pgoptions = pq_getmsgbytes(s, len);
				/*
				 * Coordinator pool is not initialized.
				 * With that it would be impossible to create a Database by default.
				 */
				agent_init(agent, database, user_name, pgoptions);
				pq_getmsgend(s);
				break;
			case 'd':			/* DISCONNECT */
				pool_getmessage(&agent->port, s, 4);
				agent_destroy(agent);
				pq_getmsgend(s);
				break;
			case 'f':			/* CLEAN CONNECTION */
				pool_getmessage(&agent->port, s, 0);
				datanodecount = pq_getmsgint(s, 4);
				/* It is possible to clean up only Coordinators connections */
				for (i = 0; i < datanodecount; i++)
				{
					/* Translate index to Oid */
					int index = pq_getmsgint(s, 4);
					Oid node = agent->dn_conn_oids[index];
					nodelist = lappend_oid(nodelist, node);
				}
				coordcount = pq_getmsgint(s, 4);
				/* It is possible to clean up only Datanode connections */
				for (i = 0; i < coordcount; i++)
				{
					/* Translate index to Oid */
					int index = pq_getmsgint(s, 4);
					Oid node = agent->coord_conn_oids[index];
					nodelist = lappend_oid(nodelist, node);
				}
				len = pq_getmsgint(s, 4);
				if (len > 0)
					database = pq_getmsgbytes(s, len);
				len = pq_getmsgint(s, 4);
				if (len > 0)
					user_name = pq_getmsgbytes(s, len);

				pq_getmsgend(s);

				/* Clean up connections here */
				res = clean_connection(nodelist, database, user_name);

				list_free(nodelist);

				/* Send success result */
				pool_sendres(&agent->port, res);
				break;
			case 'g':			/* GET CONNECTIONS */
				/*
				 * Length of message is caused by:
				 * - Message header = 4bytes
				 * - List of Datanodes = NumPoolDataNodes * 4bytes (max)
				 * - List of Coordinators = NumPoolCoords * 4bytes (max)
				 * - Number of Datanodes sent = 4bytes
				 * - Number of Coordinators sent = 4bytes
				 * It is better to send in a same message the list of Co and Dn at the same
				 * time, this permits to reduce interactions between postmaster and pooler
				 */
				pool_getmessage(&agent->port, s, 4 * agent->num_dn_connections + 4 * agent->num_coord_connections + 12);
				datanodecount = pq_getmsgint(s, 4);
				for (i = 0; i < datanodecount; i++)
					datanodelist = lappend_int(datanodelist, pq_getmsgint(s, 4));
				coordcount = pq_getmsgint(s, 4);
				/* It is possible that no Coordinators are involved in the transaction */
				for (i = 0; i < coordcount; i++)
					coordlist = lappend_int(coordlist, pq_getmsgint(s, 4));
				pq_getmsgend(s);

				/*
				 * In case of error agent_acquire_connections will log
				 * the error and return NULL
				 */
				fds = agent_acquire_connections(agent, datanodelist, coordlist,
						&pids);
				list_free(datanodelist);
				list_free(coordlist);

				pool_sendfds(&agent->port, fds, fds ? datanodecount + coordcount : 0);
				if (fds)
					pfree(fds);

				/*
				 * Also send the PIDs of the remote backend processes serving
				 * these connections
				 */
				pool_sendpids(&agent->port, pids, pids ? datanodecount + coordcount : 0);
				if (pids)
					pfree(pids);
				break;

			case 'h':			/* Cancel SQL Command in progress on specified connections */
				/*
				 * Length of message is caused by:
				 * - Message header = 4bytes
				 * - List of Datanodes = NumPoolDataNodes * 4bytes (max)
				 * - List of Coordinators = NumPoolCoords * 4bytes (max)
				 * - Number of Datanodes sent = 4bytes
				 * - Number of Coordinators sent = 4bytes
				 */
				pool_getmessage(&agent->port, s, 4 * agent->num_dn_connections + 4 * agent->num_coord_connections + 12);
				datanodecount = pq_getmsgint(s, 4);
				for (i = 0; i < datanodecount; i++)
					datanodelist = lappend_int(datanodelist, pq_getmsgint(s, 4));
				coordcount = pq_getmsgint(s, 4);
				/* It is possible that no Coordinators are involved in the transaction */
				for (i = 0; i < coordcount; i++)
					coordlist = lappend_int(coordlist, pq_getmsgint(s, 4));
				pq_getmsgend(s);

				cancel_query_on_connections(agent, datanodelist, coordlist);
				list_free(datanodelist);
				list_free(coordlist);

				/* Send success result */
				pool_sendres(&agent->port, QUERY_CANCEL_COMPLETED);
				break;
			case 'o':			/* Lock/unlock pooler */
				pool_getmessage(&agent->port, s, 8);
				is_pool_locked = pq_getmsgint(s, 4);
				pq_getmsgend(s);
				break;
			case 'p':			/* Reload connection info */
				/*
				 * Connection information reloaded concerns all the database pools.
				 * A database pool is reloaded as follows for each remote node:
				 * - node pool is deleted if the node has been deleted from catalog.
				 *   Subsequently all its connections are dropped.
				 * - node pool is deleted if its port or host information is changed.
				 *   Subsequently all its connections are dropped.
				 * - node pool is kept unchanged with existing connection information
				 *   is not changed. However its index position in node pool is changed
				 *   according to the alphabetical order of the node name in new
				 *   cluster configuration.
				 * Backend sessions are responsible to reconnect to the pooler to update
				 * their agent with newest connection information.
				 * The session invocating connection information reload is reconnected
				 * and uploaded automatically after database pool reload.
				 * Other server sessions are signaled to reconnect to pooler and update
				 * their connection information separately.
				 * During reload process done internally on pooler, pooler is locked
				 * to forbid new connection requests.
				 */
				pool_getmessage(&agent->port, s, 4);
				pq_getmsgend(s);

				/* First update all the pools */
				reload_database_pools(agent);
				break;
			case 'P':			/* Ping connection info */
				/*
				 * Ping unhealthy nodes in the pools. If any of the
				 * nodes come up, update SHARED memory to
				 * indicate the same.
				 */
				pool_getmessage(&agent->port, s, 4);
				pq_getmsgend(s);

				/* Ping all the pools */
				PoolPingNodes();

				break;
			case 'q':			/* Check connection info consistency */
				pool_getmessage(&agent->port, s, 4);
				pq_getmsgend(s);

				/* Check cached info consistency */
				res = node_info_check(agent);

				/* Send result */
				pool_sendres(&agent->port, res);
				break;
			case 'r':			/* RELEASE CONNECTIONS */
				{
					bool destroy;

					pool_getmessage(&agent->port, s, 8);
					destroy = (bool) pq_getmsgint(s, 4);
					pq_getmsgend(s);
					agent_release_connections(agent, destroy);
				}
				break;
			default:			/* EOF or protocol violation */
				agent_destroy(agent);
				return;
		}
		/* avoid reading from connection */
		if ((qtype = pool_pollbyte(&agent->port)) == EOF)
			break;
	}
}

/*
 * acquire connection
 */
static int *
agent_acquire_connections(PoolAgent *agent, List *datanodelist,
		List *coordlist, int **pids)
{
	int			i;
	int		   *result;
	ListCell   *nodelist_item;
	MemoryContext oldcontext;

	Assert(agent);

	/* Check if pooler can accept those requests */
	if (list_length(datanodelist) > agent->num_dn_connections ||
			list_length(coordlist) > agent->num_coord_connections)
	{
		elog(LOG, "agent_acquire_connections called with invalid arguments -"
				"list_length(datanodelist) %d, num_dn_connections %d,"
				"list_length(coordlist) %d, num_coord_connections %d",
				list_length(datanodelist), agent->num_dn_connections,
				list_length(coordlist), agent->num_coord_connections);
		return NULL;
	}

	/*
	 * Allocate memory
	 * File descriptors of Datanodes and Coordinators are saved in the same array,
	 * This array will be sent back to the postmaster.
	 * It has a length equal to the length of the Datanode list
	 * plus the length of the Coordinator list.
	 * Datanode fds are saved first, then Coordinator fds are saved.
	 */
	result = (int *) palloc((list_length(datanodelist) + list_length(coordlist)) * sizeof(int));
	if (result == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	*pids = (int *) palloc((list_length(datanodelist) + list_length(coordlist)) * sizeof(int));
	if (*pids == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	/*
	 * There are possible memory allocations in the core pooler, we want
	 * these allocations in the contect of the database pool
	 */
	oldcontext = MemoryContextSwitchTo(agent->pool->mcxt);


	/* Initialize result */
	i = 0;
	/* Save in array fds of Datanodes first */
	foreach(nodelist_item, datanodelist)
	{
		int			node = lfirst_int(nodelist_item);

		/* Acquire from the pool if none */
		if (agent->dn_connections[node] == NULL)
		{
			PGXCNodePoolSlot *slot = acquire_connection(agent->pool,
														agent->dn_conn_oids[node]);

			/* Handle failure */
			if (slot == NULL)
			{
				pfree(result);
				MemoryContextSwitchTo(oldcontext);
				elog(LOG, "Pooler could not open a connection to node %d",
						agent->dn_conn_oids[node]);
				return NULL;
			}

			/* Store in the descriptor */
			agent->dn_connections[node] = slot;

			/*
			 * Update newly-acquired slot with session parameters.
			 * Local parameters are fired only once BEGIN has been launched on
			 * remote nodes.
			 */
		}

		result[i] = PQsocket((PGconn *) agent->dn_connections[node]->conn);
		(*pids)[i++] = ((PGconn *) agent->dn_connections[node]->conn)->be_pid;
	}

	/* Save then in the array fds for Coordinators */
	foreach(nodelist_item, coordlist)
	{
		int			node = lfirst_int(nodelist_item);

		/* Acquire from the pool if none */
		if (agent->coord_connections[node] == NULL)
		{
			PGXCNodePoolSlot *slot = acquire_connection(agent->pool, agent->coord_conn_oids[node]);

			/* Handle failure */
			if (slot == NULL)
			{
				pfree(result);
				MemoryContextSwitchTo(oldcontext);
				elog(LOG, "Pooler could not open a connection to node %d",
						agent->coord_conn_oids[node]);
				return NULL;
			}

			/* Store in the descriptor */
			agent->coord_connections[node] = slot;

			/*
			 * Update newly-acquired slot with session parameters.
			 * Local parameters are fired only once BEGIN has been launched on
			 * remote nodes.
			 */
		}

		result[i] = PQsocket((PGconn *) agent->coord_connections[node]->conn);
		(*pids)[i++] = ((PGconn *) agent->coord_connections[node]->conn)->be_pid;
	}

	MemoryContextSwitchTo(oldcontext);

	return result;
}

/*
 * Cancel query
 */
static int
cancel_query_on_connections(PoolAgent *agent, List *datanodelist, List *coordlist)
{
	ListCell	*nodelist_item;
	char		errbuf[256];
	int		nCount;
	bool		bRet;

	nCount = 0;

	if (agent == NULL)
		return nCount;

	/* Send cancel on Datanodes first */
	foreach(nodelist_item, datanodelist)
	{
		int	node = lfirst_int(nodelist_item);

		if(node < 0 || node >= agent->num_dn_connections)
			continue;

		if (agent->dn_connections == NULL)
			break;

		if (!agent->dn_connections[node])
			continue;

		elog(DEBUG1, "Canceling query on connection to remote node %d, remote pid %d",
				agent->dn_conn_oids[node],
				((PGconn *) agent->dn_connections[node]->conn)->be_pid);
		bRet = PQcancel((PGcancel *) agent->dn_connections[node]->xc_cancelConn, errbuf, sizeof(errbuf));
		if (bRet != false)
		{
			elog(DEBUG1, "Cancelled query on connection to remote node %d, remote pid %d",
					agent->dn_conn_oids[node],
					((PGconn *) agent->dn_connections[node]->conn)->be_pid);
			nCount++;
		}
	}

	/* Send cancel to Coordinators too, e.g. if DDL was in progress */
	foreach(nodelist_item, coordlist)
	{
		int	node = lfirst_int(nodelist_item);

		if(node < 0 || node >= agent->num_coord_connections)
			continue;

		if (agent->coord_connections == NULL)
			break;

		if (!agent->coord_connections[node])
			continue;

		elog(DEBUG1, "Canceling query on connection to remote node %d, remote pid %d",
				agent->coord_conn_oids[node],
				((PGconn *) agent->coord_connections[node]->conn)->be_pid);
		bRet = PQcancel((PGcancel *) agent->coord_connections[node]->xc_cancelConn, errbuf, sizeof(errbuf));
		if (bRet != false)
		{
			elog(DEBUG1, "Cancelled query on connection to remote node %d, remote pid %d",
					agent->coord_conn_oids[node],
					((PGconn *) agent->coord_connections[node]->conn)->be_pid);
			nCount++;
		}
	}

	return nCount;
}

/*
 * Return connections back to the pool
 */
void
PoolManagerReleaseConnections(bool force)
{
	char msgtype = 'r';
	int n32;
	int msglen = 8;

	/* If disconnected from pooler all the connections already released */
	if (!poolHandle)
		return;

	elog(DEBUG1, "Returning connections back to the pool");

	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	n32 = htonl(msglen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Lock information */
	n32 = htonl((int) force);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);
	pool_flush(&poolHandle->port);
}

/*
 * Cancel Query
 */
void
PoolManagerCancelQuery(int dn_count, int* dn_list, int co_count, int* co_list)
{
	uint32		n32;
	/*
	 * Buffer contains the list of both Coordinator and Datanodes, as well
	 * as the number of connections
	 */
	uint32 		buf[2 + dn_count + co_count];
	int 		i;

	if (poolHandle == NULL)
		return;

	if (dn_count == 0 && co_count == 0)
		return;

	if (dn_count != 0 && dn_list == NULL)
		return;

	if (co_count != 0 && co_list == NULL)
		return;

	/* Insert the list of Datanodes in buffer */
	n32 = htonl((uint32) dn_count);
	buf[0] = n32;

	for (i = 0; i < dn_count;)
	{
		n32 = htonl((uint32) dn_list[i++]);
		buf[i] = n32;
	}

	/* Insert the list of Coordinators in buffer */
	n32 = htonl((uint32) co_count);
	buf[dn_count + 1] = n32;

	/* Not necessary to send to pooler a request if there is no Coordinator */
	if (co_count != 0)
	{
		for (i = dn_count + 1; i < (dn_count + co_count + 1);)
		{
			n32 = htonl((uint32) co_list[i - (dn_count + 1)]);
			buf[++i] = n32;
		}
	}
	pool_putmessage(&poolHandle->port, 'h', (char *) buf, (2 + dn_count + co_count) * sizeof(uint32));
	pool_flush(&poolHandle->port);

	/* Receive result message */
	if (pool_recvres(&poolHandle->port) != QUERY_CANCEL_COMPLETED)
		ereport(WARNING,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Query cancel not completed")));
}

/*
 * Release connections for Datanodes and Coordinators
 */
static void
agent_release_connections(PoolAgent *agent, bool force_destroy)
{
	MemoryContext oldcontext;
	int			i;

	if (!agent->dn_connections && !agent->coord_connections)
		return;
	if (!force_destroy && cluster_ex_lock_held)
	{
		elog(LOG, "Not releasing connection with cluster lock");
		return;
	}

	/*
	 * There are possible memory allocations in the core pooler, we want
	 * these allocations in the contect of the database pool
	 */
	oldcontext = MemoryContextSwitchTo(agent->pool->mcxt);

	/*
	 * Remaining connections are assumed to be clean.
	 * First clean up for Datanodes
	 */
	for (i = 0; i < agent->num_dn_connections; i++)
	{
		PGXCNodePoolSlot *slot = agent->dn_connections[i];

		/*
		 * Release connection.
		 * If connection has temporary objects on it, destroy connection slot.
		 */
		if (slot)
			release_connection(agent->pool, slot, agent->dn_conn_oids[i], force_destroy);
		agent->dn_connections[i] = NULL;
		elog(DEBUG1, "Released connection to node %d", agent->dn_conn_oids[i]);
	}
	/* Then clean up for Coordinator connections */
	for (i = 0; i < agent->num_coord_connections; i++)
	{
		PGXCNodePoolSlot *slot = agent->coord_connections[i];

		/*
		 * Release connection.
		 * If connection has temporary objects on it, destroy connection slot.
		 */
		if (slot)
			release_connection(agent->pool, slot, agent->coord_conn_oids[i], force_destroy);
		agent->coord_connections[i] = NULL;
		elog(DEBUG1, "Released connection to node %d", agent->coord_conn_oids[i]);
	}

	/*
	 * Released connections are now in the pool and we may want to close
	 * them eventually. Update the oldest_idle value to reflect the latest
	 * last access time if not already updated..
	 */
	if (!force_destroy && agent->pool->oldest_idle == (time_t) 0)
		agent->pool->oldest_idle = time(NULL);

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Create new empty pool for a database.
 * By default Database Pools have a size null so as to avoid interactions
 * between PGXC nodes in the cluster (Co/Co, Dn/Dn and Co/Dn).
 * Pool is increased at the first GET_CONNECTION message received.
 * Returns POOL_OK if operation succeed POOL_FAIL in case of OutOfMemory
 * error and POOL_WEXIST if poll for this database already exist.
 */
static DatabasePool *
create_database_pool(const char *database, const char *user_name, const char *pgoptions)
{
	MemoryContext	oldcontext;
	MemoryContext	dbcontext;
	DatabasePool   *databasePool;
	HASHCTL			hinfo;
	int				hflags;

	elog(DEBUG1, "Creating a connection pool for database %s, user %s,"
			" with pgoptions %s", database, user_name, pgoptions);

	dbcontext = AllocSetContextCreate(PoolerCoreContext,
									  "DB Context",
									  ALLOCSET_DEFAULT_MINSIZE,
									  ALLOCSET_DEFAULT_INITSIZE,
									  ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(dbcontext);
	/* Allocate memory */
	databasePool = (DatabasePool *) palloc(sizeof(DatabasePool));
	if (!databasePool)
	{
		/* out of memory */
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		return NULL;
	}

	databasePool->mcxt = dbcontext;
	 /* Copy the database name */
	databasePool->database = pstrdup(database);
	 /* Copy the user name */
	databasePool->user_name = pstrdup(user_name);
	/* Reset the oldest_idle value */
	databasePool->oldest_idle = (time_t) 0;
	 /* Copy the pgoptions */
	databasePool->pgoptions = pstrdup(pgoptions);

	if (!databasePool->database)
	{
		/* out of memory */
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		pfree(databasePool);
		return NULL;
	}

	/* Init next reference */
	databasePool->next = NULL;

	/* Init node hashtable */
	MemSet(&hinfo, 0, sizeof(hinfo));
	hflags = 0;

	hinfo.keysize = sizeof(Oid);
	hinfo.entrysize = sizeof(PGXCNodePool);
	hflags |= HASH_ELEM;

	hinfo.hcxt = dbcontext;
	hflags |= HASH_CONTEXT;

	databasePool->nodePools = hash_create("Node Pool", MaxDataNodes + MaxCoords,
										  &hinfo, hflags);

	MemoryContextSwitchTo(oldcontext);

	/* Insert into the list */
	insert_database_pool(databasePool);

	return databasePool;
}


/*
 * Destroy the pool and free memory
 */
static int
destroy_database_pool(const char *database, const char *user_name)
{
	DatabasePool *databasePool;

	elog(DEBUG1, "Destroy a connection pool to database %s, user %s",
			database, user_name);

	/* Delete from the list */
	databasePool = remove_database_pool(database, user_name);
	if (databasePool)
	{
		HASH_SEQ_STATUS hseq_status;
		PGXCNodePool   *nodePool;

		hash_seq_init(&hseq_status, databasePool->nodePools);
		while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
		{
			destroy_node_pool(nodePool);
		}
		/* free allocated memory */
		MemoryContextDelete(databasePool->mcxt);
		return 1;
	}
	return 0;
}


/*
 * Insert new database pool to the list
 */
static void
insert_database_pool(DatabasePool *databasePool)
{
	Assert(databasePool);

	/* Reference existing list or null the tail */
	if (databasePools)
		databasePool->next = databasePools;
	else
		databasePool->next = NULL;

	/* Update head pointer */
	databasePools = databasePool;
}

/*
 * Rebuild information of database pools
 */
static void
reload_database_pools(PoolAgent *agent)
{
	DatabasePool *databasePool;

	elog(DEBUG1, "Reloading database pools");

	/*
	 * Release node connections if any held. It is not guaranteed client session
	 * does the same so don't ever try to return them to pool and reuse
	 */
	agent_release_connections(agent, true);

	/* Forget previously allocated node info */
	MemoryContextReset(agent->mcxt);

	/* and allocate new */
	PgxcNodeGetOids(&agent->coord_conn_oids, &agent->dn_conn_oids,
					&agent->num_coord_connections, &agent->num_dn_connections, false);

	agent->coord_connections = (PGXCNodePoolSlot **)
			palloc0(agent->num_coord_connections * sizeof(PGXCNodePoolSlot *));
	agent->dn_connections = (PGXCNodePoolSlot **)
			palloc0(agent->num_dn_connections * sizeof(PGXCNodePoolSlot *));

	/*
	 * Scan the list and destroy any altered pool. They will be recreated
	 * upon subsequent connection acquisition.
	 */
	databasePool = databasePools;
	while (databasePool)
	{
		/* Update each database pool slot with new connection information */
		HASH_SEQ_STATUS hseq_status;
		PGXCNodePool   *nodePool;

		hash_seq_init(&hseq_status, databasePool->nodePools);
		while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
		{
			char *connstr_chk = build_node_conn_str(nodePool->nodeoid, databasePool);

			if (connstr_chk == NULL || strcmp(connstr_chk, nodePool->connstr))
			{
				/* Node has been removed or altered */
				destroy_node_pool(nodePool);
				hash_search(databasePool->nodePools, &nodePool->nodeoid,
							HASH_REMOVE, NULL);
			}

			if (connstr_chk)
				pfree(connstr_chk);
		}

		databasePool = databasePool->next;
	}
}


/*
 * Find pool for specified database and username in the list
 */
static DatabasePool *
find_database_pool(const char *database, const char *user_name, const char *pgoptions)
{
	DatabasePool *databasePool;

	/* Scan the list */
	databasePool = databasePools;
	while (databasePool)
	{
		if (strcmp(database, databasePool->database) == 0 &&
			strcmp(user_name, databasePool->user_name) == 0 &&
			strcmp(pgoptions, databasePool->pgoptions) == 0)
			break;
		databasePool = databasePool->next;
	}
	return databasePool;
}


/*
 * Remove pool for specified database from the list
 */
static DatabasePool *
remove_database_pool(const char *database, const char *user_name)
{
	DatabasePool *databasePool,
			   *prev;

	/* Scan the list */
	databasePool = databasePools;
	prev = NULL;
	while (databasePool)
	{

		/* if match break the loop and return */
		if (strcmp(database, databasePool->database) == 0 &&
			strcmp(user_name, databasePool->user_name) == 0)
			break;
		prev = databasePool;
		databasePool = databasePool->next;
	}

	/* if found */
	if (databasePool)
	{

		/* Remove entry from chain or update head */
		if (prev)
			prev->next = databasePool->next;
		else
			databasePools = databasePool->next;


		databasePool->next = NULL;
	}
	return databasePool;
}

/*
 * Acquire connection
 */
static PGXCNodePoolSlot *
acquire_connection(DatabasePool *dbPool, Oid node)
{
	PGXCNodePool	   *nodePool;
	PGXCNodePoolSlot   *slot;

	Assert(dbPool);

	nodePool = (PGXCNodePool *) hash_search(dbPool->nodePools, &node, HASH_FIND,
											NULL);

	/*
	 * When a Coordinator pool is initialized by a Coordinator Postmaster,
	 * it has a NULL size and is below minimum size that is 1
	 * This is to avoid problems of connections between Coordinators
	 * when creating or dropping Databases.
	 */
	if (nodePool == NULL || nodePool->freeSize == 0)
		nodePool = grow_pool(dbPool, node);

	slot = NULL;
	/* Check available connections */
	while (nodePool && nodePool->freeSize > 0)
	{
		int			poll_result;

		slot = nodePool->slot[--(nodePool->freeSize)];

	retry:
		if (PQsocket((PGconn *) slot->conn) > 0)
		{
			/*
			 * Make sure connection is ok, destroy connection slot if there is a
			 * problem.
			 */
			poll_result = pqReadReady((PGconn *) slot->conn);

			if (poll_result == 0)
				break; 		/* ok, no data */
			else if (poll_result < 0)
			{
				if (errno == EAGAIN || errno == EINTR)
					goto retry;

				elog(WARNING, "Error in checking connection, errno = %d", errno);
			}
			else
				elog(WARNING, "Unexpected data on connection, cleaning.");
		}

		destroy_slot(slot);
		slot = NULL;

		/* Decrement current max pool size */
		(nodePool->size)--;
		/* Ensure we are not below minimum size */
		nodePool = grow_pool(dbPool, node);
	}

	if (slot == NULL)
	{
		elog(WARNING, "can not connect to node %u", node);

		/*
		 * before returning, also update the shared health
		 * status field to indicate that this node is down
		 */
		if (!PgxcNodeUpdateHealth(node, false))
			elog(WARNING, "Could not update health status of node %u", node);
		else
			elog(WARNING, "Health map updated to reflect DOWN node (%u)", node);
	}
	else
		PgxcNodeUpdateHealth(node, true);

	return slot;
}


/*
 * release connection from specified pool and slot
 */
static void
release_connection(DatabasePool *dbPool, PGXCNodePoolSlot *slot,
				   Oid node, bool force_destroy)
{
	PGXCNodePool *nodePool;

	Assert(dbPool);
	Assert(slot);

	nodePool = (PGXCNodePool *) hash_search(dbPool->nodePools, &node, HASH_FIND,
											NULL);
	if (nodePool == NULL)
	{
		/*
		 * The node may be altered or dropped.
		 * In any case the slot is no longer valid.
		 */
		destroy_slot(slot);
		return;
	}

	/* return or discard */
	if (!force_destroy)
	{
		/* Insert the slot into the array and increase pool size */
		nodePool->slot[(nodePool->freeSize)++] = slot;
		slot->released = time(NULL);
	}
	else
	{
		elog(DEBUG1, "Cleaning up connection from pool %s, closing", nodePool->connstr);
		destroy_slot(slot);
		/* Decrement pool size */
		(nodePool->size)--;
		/* Ensure we are not below minimum size */
		grow_pool(dbPool, node);
	}
}


/*
 * Increase database pool size, create new if does not exist
 */
static PGXCNodePool *
grow_pool(DatabasePool *dbPool, Oid node)
{
	/* if error try to release idle connections and try again */
	bool 			tryagain = true;
	PGXCNodePool   *nodePool;
	bool			found;

	Assert(dbPool);

	nodePool = (PGXCNodePool *) hash_search(dbPool->nodePools, &node,
											HASH_ENTER, &found);
	nodePool->connstr = build_node_conn_str(node, dbPool);
	if (!nodePool->connstr)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not build connection string for node %u", node)));
	}

	if (!found)
	{
		nodePool->slot = (PGXCNodePoolSlot **) palloc0(MaxPoolSize * sizeof(PGXCNodePoolSlot *));
		if (!nodePool->slot)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		}
		nodePool->freeSize = 0;
		nodePool->size = 0;
	}

	while (nodePool->freeSize == 0 && nodePool->size < MaxPoolSize)
	{
		PGXCNodePoolSlot *slot;

		/* Allocate new slot */
		slot = (PGXCNodePoolSlot *) palloc(sizeof(PGXCNodePoolSlot));
		if (slot == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		}

		/* If connection fails, be sure that slot is destroyed cleanly */
		slot->xc_cancelConn = NULL;

		/* Establish connection */
		slot->conn = PGXCNodeConnect(nodePool->connstr);
		if (!PGXCNodeConnected(slot->conn))
		{
			destroy_slot(slot);
			ereport(LOG,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("failed to connect to Datanode")));
			/*
			 * If we failed to connect probably number of connections on the
			 * target node reached max_connections. Try and release idle
			 * connections and try again.
			 * We do not want to enter endless loop here and run maintenance
			 * procedure only once.
			 * It is not safe to run the maintenance procedure if no connections
			 * from that pool currently in use - the node pool may be destroyed
			 * in that case.
			 */
			if (tryagain && nodePool->size > nodePool->freeSize)
			{
				pools_maintenance();
				tryagain = false;
				continue;
			}
			break;
		}

		slot->xc_cancelConn = (NODE_CANCEL *) PQgetCancel((PGconn *)slot->conn);
		slot->released = time(NULL);
		if (dbPool->oldest_idle == (time_t) 0)
			dbPool->oldest_idle = slot->released;

		/* Insert at the end of the pool */
		nodePool->slot[(nodePool->freeSize)++] = slot;

		/* Increase count of pool size */
		(nodePool->size)++;
		elog(DEBUG1, "Pooler: increased pool size to %d for pool %s",
			 nodePool->size,
			 nodePool->connstr);
	}
	return nodePool;
}


/*
 * Destroy pool slot
 */
static void
destroy_slot(PGXCNodePoolSlot *slot)
{
	if (!slot)
		return;

	PQfreeCancel((PGcancel *)slot->xc_cancelConn);
	PGXCNodeClose(slot->conn);
	pfree(slot);
}


/*
 * Destroy node pool
 */
static void
destroy_node_pool(PGXCNodePool *node_pool)
{
	int			i;

	if (!node_pool)
		return;

	/*
	 * At this point all agents using connections from this pool should be already closed
	 * If this not the connections to the Datanodes assigned to them remain open, this will
	 * consume Datanode resources.
	 */
	elog(DEBUG1, "About to destroy node pool %s, current size is %d, %d connections are in use",
		 node_pool->connstr, node_pool->freeSize, node_pool->size - node_pool->freeSize);
	if (node_pool->connstr)
		pfree(node_pool->connstr);

	if (node_pool->slot)
	{
		for (i = 0; i < node_pool->freeSize; i++)
			destroy_slot(node_pool->slot[i]);
		pfree(node_pool->slot);
	}
}


/*
 * Main handling loop
 */
static void
PoolerLoop(void)
{
	StringInfoData 	input_message;
	time_t			last_maintenance = (time_t) 0;
	int				maintenance_timeout;
	struct pollfd	*pool_fd;

#ifdef HAVE_UNIX_SOCKETS
	if (Unix_socket_directories)
	{
		char	   *rawstring;
		List	   *elemlist;
		ListCell   *l;
		int			success = 0;

		/* Need a modifiable copy of Unix_socket_directories */
		rawstring = pstrdup(Unix_socket_directories);

		/* Parse string into list of directories */
		if (!SplitDirectoriesString(rawstring, ',', &elemlist))
		{
			/* syntax error in list */
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid list syntax in parameter \"%s\"",
							"unix_socket_directories")));
		}

		foreach(l, elemlist)
		{
			char	   *socketdir = (char *) lfirst(l);
			int			saved_errno;

			/* Connect to the pooler */
			server_fd = pool_listen(PoolerPort, socketdir);
			if (server_fd < 0)
			{
				saved_errno = errno;
				ereport(WARNING,
						(errmsg("could not create Unix-domain socket in directory \"%s\", errno %d, server_fd %d",
								socketdir, saved_errno, server_fd)));
			}
			else
			{
				success++;
			}
		}

		if (!success && elemlist != NIL)
			ereport(ERROR,
					(errmsg("failed to start listening on Unix-domain socket for pooler: %m")));

		list_free_deep(elemlist);
		pfree(rawstring);
	}
#endif

	pool_fd = (struct pollfd *) palloc((MaxConnections + 1) * sizeof(struct pollfd));

	if (server_fd == -1)
	{
		/* log error */
		return;
	}

	initStringInfo(&input_message);

	pool_fd[0].fd = server_fd;
	pool_fd[0].events = POLLIN; 

	for (;;)
	{

		int			retval;
		int			i;

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (!PostmasterIsAlive())
			exit(1);

		/* watch for incoming messages */
		for (i = 1; i <= agentCount; i++)
		{
			PoolAgent *agent = poolAgents[i - 1];
			int sockfd = Socket(agent->port);
			pool_fd[i].fd = sockfd;
			pool_fd[i].events = POLLIN;
		}

		if (PoolMaintenanceTimeout > 0)
		{
			int				timeout_val;
			double			timediff;

			/*
			 * Decide the timeout value based on when the last
			 * maintenance activity was carried out. If the last
			 * maintenance was done quite a while ago schedule the select
			 * with no timeout. It will serve any incoming activity
			 * and if there's none it will cause the maintenance
			 * to be scheduled as soon as possible
			 */
			timediff = difftime(time(NULL), last_maintenance);

			if (timediff > PoolMaintenanceTimeout)
				timeout_val = 0;
			else
				timeout_val = PoolMaintenanceTimeout - rint(timediff);

			maintenance_timeout = timeout_val * 1000;
		}
		else
			maintenance_timeout = -1;
		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (!PostmasterIsAlive())
			exit(1);

		/*
		 * Process any requests or signals received recently.
		 */
		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (shutdown_requested)
		{
			for (i = agentCount - 1; agentCount > 0 && i >= 0; i--)
			{
				PoolAgent  *agent = poolAgents[i];
				agent_destroy(agent);
			}

			while (databasePools)
				if (destroy_database_pool(databasePools->database,
										  databasePools->user_name) == 0)
					break;
			
			close(server_fd);
			exit(0);
		}

		/* wait for event */
		retval = poll(pool_fd, agentCount + 1, maintenance_timeout);
		if (retval < 0)
		{
			if (errno == EINTR || errno == EAGAIN)
				continue;
			elog(FATAL, "poll returned with error %d", retval);
		}

		if (retval > 0)
		{
			/*
			 * Agent may be removed from the array while processing
			 * and trailing items are shifted, so scroll downward
			 * to avoid problem
			 */
			for (i = agentCount - 1; agentCount > 0 && i >= 0; i--)
			{
				PoolAgent *agent = poolAgents[i];
				int sockfd = Socket(agent->port);

				if ((sockfd == pool_fd[i + 1].fd) && 
						(pool_fd[i + 1].revents & POLLIN))
					agent_handle_input(agent, &input_message);
			}

			if (pool_fd[0].revents & POLLIN)
				agent_create();
		}
		else if (retval == 0)
		{
			/* maintenance timeout */
			pools_maintenance();
			PoolPingNodes();
			last_maintenance = time(NULL);
		}
	}
}

/*
 * Clean Connection in all Database Pools for given Datanode and Coordinator list
 */
int
clean_connection(List *node_discard, const char *database, const char *user_name)
{
	DatabasePool *databasePool;
	int			res = CLEAN_CONNECTION_COMPLETED;

	databasePool = databasePools;

	while (databasePool)
	{
		ListCell *lc;

		if ((database && strcmp(database, databasePool->database)) ||
				(user_name && strcmp(user_name, databasePool->user_name)))
		{
			/* The pool does not match to request, skip */
			databasePool = databasePool->next;
			continue;
		}

		/*
		 * Clean each requested node pool
		 */
		foreach(lc, node_discard)
		{
			PGXCNodePool *nodePool;
			Oid node = lfirst_oid(lc);

			nodePool = hash_search(databasePool->nodePools, &node, HASH_FIND,
								   NULL);

			if (nodePool)
			{
				/* Check if connections are in use */
				if (nodePool->freeSize < nodePool->size)
				{
					elog(WARNING, "Pool of Database %s is using Datanode %u connections",
								databasePool->database, node);
					res = CLEAN_CONNECTION_NOT_COMPLETED;
				}

				/* Destroy connections currently in Node Pool */
				if (nodePool->slot)
				{
					int i;
					for (i = 0; i < nodePool->freeSize; i++)
						destroy_slot(nodePool->slot[i]);
				}
				nodePool->size -= nodePool->freeSize;
				nodePool->freeSize = 0;
			}
		}

		databasePool = databasePool->next;
	}

	/* Release lock on Pooler, to allow transactions to connect again. */
	is_pool_locked = false;
	return res;
}

/*
 * Take a Lock on Pooler.
 * Abort PIDs registered with the agents for the given database.
 * Send back to client list of PIDs signaled to watch them.
 */
int *
abort_pids(int *len, int pid, const char *database, const char *user_name)
{
	int *pids = NULL;
	int i = 0;
	int count;

	Assert(!is_pool_locked);
	Assert(agentCount > 0);

	is_pool_locked = true;

	pids = (int *) palloc((agentCount - 1) * sizeof(int));

	/* Send a SIGTERM signal to all processes of Pooler agents except this one */
	for (count = 0; count < agentCount; count++)
	{
		if (poolAgents[count]->pid == pid)
			continue;

		if (database && strcmp(poolAgents[count]->pool->database, database) != 0)
			continue;

		if (user_name && strcmp(poolAgents[count]->pool->user_name, user_name) != 0)
			continue;

		if (kill(poolAgents[count]->pid, SIGTERM) < 0)
			elog(ERROR, "kill(%ld,%d) failed: %m",
						(long) poolAgents[count]->pid, SIGTERM);

		pids[i++] = poolAgents[count]->pid;
	}

	*len = i;

	return pids;
}

/*
 *
 */
static void
pooler_die(SIGNAL_ARGS)
{
	shutdown_requested = true;
}


/*
 *
 */
static void
pooler_quickdie(SIGNAL_ARGS)
{
	PG_SETMASK(&BlockSig);
	exit(2);
}


static void
pooler_sighup(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}

/*
 * Given node identifier, dbname and user name build connection string.
 * Get node connection details from the shared memory node table
 */
static char *
build_node_conn_str(Oid node, DatabasePool *dbPool)
{
	NodeDefinition *nodeDef;
	char 		   *connstr;

	nodeDef = PgxcNodeGetDefinition(node);
	if (nodeDef == NULL)
	{
		/* No such definition, node is dropped? */
		return NULL;
	}

	connstr = PGXCNodeConnStr(NameStr(nodeDef->nodehost),
							  nodeDef->nodeport,
							  dbPool->database,
							  dbPool->user_name,
							  dbPool->pgoptions,
							  IS_PGXC_COORDINATOR ? "coordinator" : "datanode",
							  PGXCNodeName);
	pfree(nodeDef);

	return connstr;
}

/*
 * Check all pooled connections, and close which have been released more then
 * PooledConnKeepAlive seconds ago.
 * Return true if shrink operation closed all the connections and pool can be
 * ddestroyed, false if there are still connections or pool is in use.
 */
static bool
shrink_pool(DatabasePool *pool)
{
	time_t 			now = time(NULL);
	HASH_SEQ_STATUS hseq_status;
	PGXCNodePool   *nodePool;
	int 			i;
	bool			empty = true;

	/* Negative PooledConnKeepAlive disables automatic connection cleanup */
	if (PoolConnKeepAlive < 0)
		return false;

	pool->oldest_idle = (time_t) 0;
	hash_seq_init(&hseq_status, pool->nodePools);
	while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
	{
		/* Go thru the free slots and destroy those that are free too long */
		for (i = 0; i < nodePool->freeSize; )
		{
			PGXCNodePoolSlot *slot = nodePool->slot[i];

			if (difftime(now, slot->released) > PoolConnKeepAlive)
			{
				/* connection is idle for long, close it */
				destroy_slot(slot);
				/* reduce pool size and total number of connections */
				(nodePool->freeSize)--;
				(nodePool->size)--;
				/* move last connection in place, if not at last already */
				if (i < nodePool->freeSize)
					nodePool->slot[i] = nodePool->slot[nodePool->freeSize];
			}
			else
			{
				if (pool->oldest_idle == (time_t) 0 ||
						difftime(pool->oldest_idle, slot->released) > 0)
					pool->oldest_idle = slot->released;

				i++;
			}
		}
		if (nodePool->size > 0)
			empty = false;
		else
		{
			destroy_node_pool(nodePool);
			hash_search(pool->nodePools, &nodePool->nodeoid, HASH_REMOVE, NULL);
		}
	}

	/*
	 * Last check, if any active agent is referencing the pool do not allow to
	 * destroy it, because there will be a problem if session wakes up and try
	 * to get a connection from non existing pool.
	 * If all such sessions will eventually disconnect the pool will be
	 * destroyed during next maintenance procedure.
	 */
	if (empty)
	{
		for (i = 0; i < agentCount; i++)
		{
			if (poolAgents[i]->pool == pool)
				return false;
		}
	}

	return empty;
}


/*
 * Scan connection pools and release connections which are idle for long.
 * If pool gets empty after releasing connections it is destroyed.
 */
static void
pools_maintenance(void)
{
	DatabasePool   *prev = NULL;
	DatabasePool   *curr = databasePools;
	time_t			now = time(NULL);
	int				count = 0;

	/* Iterate over the pools */
	while (curr)
	{
		/*
		 * If current pool has connections to close and it is emptied after
		 * shrink remove the pool and free memory.
		 * Otherwithe move to next pool.
		 */
		if (curr->oldest_idle != (time_t) 0 &&
				difftime(now, curr->oldest_idle) > PoolConnKeepAlive &&
				shrink_pool(curr))
		{
			MemoryContext mem = curr->mcxt;
			curr = curr->next;
			if (prev)
				prev->next = curr;
			else
				databasePools = curr;
			MemoryContextDelete(mem);
			count++;
		}
		else
		{
			prev = curr;
			curr = curr->next;
		}
	}
	elog(DEBUG1, "Pool maintenance, done in %f seconds, removed %d pools",
			difftime(time(NULL), now), count);
}

bool
check_persistent_connections(bool *newval, void **extra, GucSource source)
{
	if (*newval && IS_PGXC_DATANODE)
	{
		elog(WARNING, "persistent_datanode_connections = ON is currently not "
				"supported on datanodes - ignoring");
		*newval = false;
	}
	return true;
}
