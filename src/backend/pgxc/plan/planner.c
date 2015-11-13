/*-------------------------------------------------------------------------
 *
 * planner.c
 *
 *	  Functions for generating a PGXC style plan.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "access/transam.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/tlist.h"
#include "parser/parse_agg.h"
#include "parser/parse_func.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "parser/parse_oper.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/planner.h"
#include "pgxc/postgresql_fdw.h"
#include "tcop/pquery.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/portal.h"
#include "utils/syscache.h"
#include "utils/numeric.h"
#include "utils/memutils.h"
#include "access/hash.h"
#include "commands/tablecmds.h"
#include "utils/timestamp.h"
#include "utils/date.h"

/* Forbid unsafe SQL statements */
bool		StrictStatementChecking = true;

#ifdef XCP
/*
 * AddRemoteQueryNode
 *
 * Add a Remote Query node to launch on Datanodes.
 * This can only be done for a query a Top Level to avoid
 * duplicated queries on Datanodes.
 */
List *
AddRemoteQueryNode(List *stmts, const char *queryString, RemoteQueryExecType remoteExecType)
{
	List *result = stmts;

	/* If node is appplied on EXEC_ON_NONE, simply return the list unchanged */
	if (remoteExecType == EXEC_ON_NONE)
		return result;

	/* Only a remote Coordinator is allowed to send a query to backend nodes */
	if (remoteExecType == EXEC_ON_CURRENT ||
			(IS_PGXC_LOCAL_COORDINATOR))
	{
		RemoteQuery *step = makeNode(RemoteQuery);
		step->combine_type = COMBINE_TYPE_SAME;
		step->sql_statement = (char *) queryString;
		step->exec_type = remoteExecType;
		result = lappend(result, step);
	}

	return result;
}
#endif


/*
 * pgxc_direct_planner
 * The routine tries to see if the statement can be completely evaluated on the
 * datanodes. In such cases coordinator is not needed to evaluate the statement,
 * and just acts as a proxy. A statement can be completely shipped to the remote
 * node if every row of the result can be evaluated on a single datanode.
 * For example:
 *
 * Only EXECUTE DIRECT statements are sent directly as of now
 */
PlannedStmt *
pgxc_direct_planner(Query *query, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result;
	RemoteQuery *query_step = NULL;

	/* build the PlannedStmt result */
	result = makeNode(PlannedStmt);

	/* Try and set what we can */
	result->commandType = query->commandType;
	result->canSetTag = query->canSetTag;
	result->utilityStmt = query->utilityStmt;
	result->rtable = query->rtable;

	/* EXECUTE DIRECT statements have their RemoteQuery node already built when analyzing */
	if (query->utilityStmt
		&& IsA(query->utilityStmt, RemoteQuery))
	{
		RemoteQuery *stmt = (RemoteQuery *) query->utilityStmt;
		if (stmt->exec_direct_type != EXEC_DIRECT_NONE)
		{
			query_step = stmt;
			query->utilityStmt = NULL;
			result->utilityStmt = NULL;
		}
	}

	Assert(query_step);
	/* Optimize multi-node handling */
	query_step->read_only = query->commandType == CMD_SELECT;

	result->planTree = (Plan *) query_step;

	query_step->scan.plan.targetlist = query->targetList;

	return result;
}

