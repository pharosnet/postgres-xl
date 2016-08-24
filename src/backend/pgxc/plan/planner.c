/*-------------------------------------------------------------------------
 *
 * planner.c
 *
 *	  Functions for generating a PGXC style plan.
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
#include "optimizer/cost.h"
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


static bool contains_temp_tables(List *rtable);
static PlannedStmt *pgxc_FQS_planner(Query *query, ParamListInfo boundParams);
static RemoteQuery *pgxc_FQS_create_remote_plan(Query *query,
												ExecNodes *exec_nodes,
												bool is_exec_direct);
static CombineType get_plan_combine_type(CmdType commandType, char baselocatortype);

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

/*
 * Returns true if at least one temporary table is in use
 * in query (and its subqueries)
 */
static bool
contains_temp_tables(List *rtable)
{
	ListCell *item;

	foreach(item, rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(item);

		if (rte->rtekind == RTE_RELATION)
		{
			if (IsTempTable(rte->relid))
				return true;
		}
		else if (rte->rtekind == RTE_SUBQUERY &&
				 contains_temp_tables(rte->subquery->rtable))
			return true;
	}

	return false;
}

/*
 * get_plan_combine_type - determine combine type
 *
 * COMBINE_TYPE_SAME - for replicated updates
 * COMBINE_TYPE_SUM - for hash and round robin updates
 * COMBINE_TYPE_NONE - for operations where row_count is not applicable
 *
 * return NULL if it is not safe to be done in a single step.
 */
static CombineType
get_plan_combine_type(CmdType commandType, char baselocatortype)
{

	switch (commandType)
	{
		case CMD_INSERT:
		case CMD_UPDATE:
		case CMD_DELETE:
			return baselocatortype == LOCATOR_TYPE_REPLICATED ?
					COMBINE_TYPE_SAME : COMBINE_TYPE_SUM;

		default:
			return COMBINE_TYPE_NONE;
	}
	/* quiet compiler warning */
	return COMBINE_TYPE_NONE;
}


/*
 * Build up a QueryPlan to execute on.
 *
 * This functions tries to find out whether
 * 1. The statement can be shipped to the Datanode and Coordinator is needed
 *    only as a proxy - in which case, it creates a single node plan.
 * 2. The statement can be evaluated on the Coordinator completely - thus no
 *    query shipping is involved and standard_planner() is invoked to plan the
 *    statement
 * 3. The statement needs Coordinator as well as Datanode for evaluation -
 *    again we use standard_planner() to plan the statement.
 *
 * The plan generated in either of the above cases is returned.
 */
PlannedStmt *
pgxc_planner(Query *query, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result;

	/* see if can ship the query completely */
	result = pgxc_FQS_planner(query, boundParams);
	if (result)
		return result;

	/* we need Coordinator for evaluation, invoke standard planner */
	result = standard_planner(query, cursorOptions, boundParams);
	return result;
}

/*
 * pgxc_FQS_planner
 * The routine tries to see if the statement can be completely evaluated on the
 * Datanodes. In such cases Coordinator is not needed to evaluate the statement,
 * and just acts as a proxy. A statement can be completely shipped to the remote
 * node if every row of the result can be evaluated on a single Datanode.
 * For example:
 *
 * 1. SELECT * FROM tab1; where tab1 is a distributed table - Every row of the
 * result set can be evaluated at a single Datanode. Hence this statement is
 * completely shippable even though many Datanodes are involved in evaluating
 * complete result set. In such case Coordinator will be able to gather rows
 * arisign from individual Datanodes and proxy the result to the client.
 *
 * 2. SELECT count(*) FROM tab1; where tab1 is a distributed table - there is
 * only one row in the result but it needs input from all the Datanodes. Hence
 * this is not completely shippable.
 *
 * 3. SELECT count(*) FROM tab1; where tab1 is replicated table - since result
 * can be obtained from a single Datanode, this is a completely shippable
 * statement.
 *
 * fqs in the name of function is acronym for fast query shipping.
 */
static PlannedStmt *
pgxc_FQS_planner(Query *query, ParamListInfo boundParams)
{
	PlannedStmt		*result;
	PlannerGlobal	*glob;
	PlannerInfo		*root;
	ExecNodes		*exec_nodes;
	Plan			*top_plan;

	/* Try by-passing standard planner, if fast query shipping is enabled */
	if (!enable_fast_query_shipping)
		return NULL;

	/* Do not FQS cursor statements */
	if (query->utilityStmt &&
		IsA(query->utilityStmt, DeclareCursorStmt))
		return NULL;

	/* Do not FQS EXEC DIRECT statements */
	if (query->utilityStmt && IsA(query->utilityStmt, RemoteQuery))
	{
		RemoteQuery *stmt = (RemoteQuery *) query->utilityStmt;
		if (stmt->exec_direct_type != EXEC_DIRECT_NONE)
			return NULL;
	}

	/*
	 * If the query can not be or need not be shipped to the Datanodes, don't
	 * create any plan here. standard_planner() will take care of it.
	 */
	exec_nodes = pgxc_is_query_shippable(query, 0);
	if (exec_nodes == NULL)
		return NULL;

	glob = makeNode(PlannerGlobal);
	glob->boundParams = boundParams;
	/* Create a PlannerInfo data structure, usually it is done for a subquery */
	root = makeNode(PlannerInfo);
	root->parse = query;
	root->glob = glob;
	root->query_level = 1;
	root->planner_cxt = CurrentMemoryContext;

	/*
	 * We decided to ship the query to the Datanode/s, create a RemoteQuery node
	 * for the same.
	 */
	top_plan = (Plan *)pgxc_FQS_create_remote_plan(query, exec_nodes, false);

	/*
	 * Just before creating the PlannedStmt, do some final cleanup
	 * We need to save plan dependencies, so that dropping objects will
	 * invalidate the cached plan if it depends on those objects. Table
	 * dependencies are available in glob->relationOids and all other
	 * dependencies are in glob->invalItems. These fields can be retrieved
	 * through set_plan_references().
	 */
	top_plan = set_plan_references(root, top_plan);

	/* build the PlannedStmt result */
	result = makeNode(PlannedStmt);
	/* Try and set what we can, rest must have been zeroed out by makeNode() */
	result->commandType = query->commandType;
	result->canSetTag = query->canSetTag;
	result->utilityStmt = query->utilityStmt;

	/* Set result relations */
	if (query->commandType != CMD_SELECT)
		result->resultRelations = list_make1_int(query->resultRelation);
	result->planTree = top_plan;
	result->rtable = query->rtable;
	result->relationOids = glob->relationOids;
	result->invalItems = glob->invalItems;

	return result;
}

static RemoteQuery *
pgxc_FQS_create_remote_plan(Query *query, ExecNodes *exec_nodes, bool is_exec_direct)
{
	RemoteQuery *query_step;
	StringInfoData buf;
	RangeTblEntry	*dummy_rte;

	/* EXECUTE DIRECT statements have their RemoteQuery node already built when analyzing */
	if (is_exec_direct)
	{
		Assert(IsA(query->utilityStmt, RemoteQuery));
		query_step = (RemoteQuery *)query->utilityStmt;
		query->utilityStmt = NULL;
	}
	else
	{
		query_step = makeNode(RemoteQuery);
		query_step->combine_type = COMBINE_TYPE_NONE;
		query_step->exec_type = EXEC_ON_DATANODES;
		query_step->exec_direct_type = EXEC_DIRECT_NONE;
		query_step->exec_nodes = exec_nodes;
	}

	Assert(query_step->exec_nodes);

	/* Deparse query tree to get step query. */
	if (query_step->sql_statement == NULL)
	{
		initStringInfo(&buf);
		/*
		 * We always finalise aggregates on datanodes for FQS.
		 * Use the expressions for ORDER BY or GROUP BY clauses.
		 */
		deparse_query(query, &buf, NIL, true, false);
		query_step->sql_statement = pstrdup(buf.data);
		pfree(buf.data);
	}

	/* Optimize multi-node handling */
	query_step->read_only = (query->commandType == CMD_SELECT && !query->hasForUpdate);
	query_step->has_row_marks = query->hasForUpdate;

	/* Check if temporary tables are in use in query */
	/* PGXC_FQS_TODO: scanning the rtable again for the queries should not be
	 * needed. We should be able to find out if the query has a temporary object
	 * while finding nodes for the objects. But there is no way we can convey
	 * that information here. Till such a connection is available, this is it.
	 */
	if (contains_temp_tables(query->rtable))
		query_step->is_temp = true;

	/*
	 * We need to evaluate some expressions like the ExecNodes->en_expr at
	 * Coordinator, prepare those for evaluation. Ideally we should call
	 * preprocess_expression, but it needs PlannerInfo structure for the same
	 */
	fix_opfuncids((Node *)(query_step->exec_nodes->en_expr));
	/*
	 * PGXCTODO
	 * When Postgres runs insert into t (a) values (1); against table
	 * defined as create table t (a int, b int); the plan is looking
	 * like insert into t (a,b) values (1,null);
	 * Later executor is verifying plan, to make sure table has not
	 * been altered since plan has been created and comparing table
	 * definition with plan target list and output error if they do
	 * not match.
	 * I could not find better way to generate targetList for pgxc plan
	 * then call standard planner and take targetList from the plan
	 * generated by Postgres.
	 */
	query_step->combine_type = get_plan_combine_type(
				query->commandType, query_step->exec_nodes->baselocatortype);

	/*
	 * Create a dummy RTE for the remote query being created. Append the dummy
	 * range table entry to the range table. Note that this modifies the master
	 * copy the caller passed us, otherwise e.g EXPLAIN VERBOSE will fail to
	 * find the rte the Vars built below refer to. Also create the tuple
	 * descriptor for the result of this query from the base_tlist (targetlist
	 * we used to generate the remote node query).
	 */
	dummy_rte = makeNode(RangeTblEntry);
	dummy_rte->rtekind = RTE_REMOTE_DUMMY;
	/* Use a dummy relname... */
	if (is_exec_direct)
		dummy_rte->relname = "__EXECUTE_DIRECT__";
	else
		dummy_rte->relname	   = "__REMOTE_FQS_QUERY__";
	dummy_rte->eref		   = makeAlias("__REMOTE_FQS_QUERY__", NIL);
	/* Rest will be zeroed out in makeNode() */

	query->rtable = lappend(query->rtable, dummy_rte);
	query_step->scan.scanrelid 	= list_length(query->rtable);
	query_step->scan.plan.targetlist = query->targetList;
	query_step->base_tlist = query->targetList;

	return query_step;
}
