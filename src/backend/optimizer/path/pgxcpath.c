/*-------------------------------------------------------------------------
 *
 * pgxcpath.c
 *	  Routines to find possible remote query paths for various relation types.
 *
 * Portions Copyright (c) 2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/pgxcpath.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "commands/tablecmds.h"
#include "optimizer/cost.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/pgxcship.h"
#include "optimizer/restrictinfo.h"
#include "parser/parsetree.h"
#include "pgxc/pgxc.h"
#include "optimizer/pgxcplan.h"

static RemoteQueryPath *create_remotequery_path(PlannerInfo *root, RelOptInfo *rel,
								ExecNodes *exec_nodes, RemoteQueryPath *leftpath,
								RemoteQueryPath *rightpath, JoinType jointype,
								List *join_restrictlist);
/*
 * create_remotequery_path
 *	  Creates a RemoteQuery path for a given RelOptInfo.
 *
 * The path buils the RelOptInfo data by querying datanode(s). For RelOptInfo
 * representing a JOIN, the left/right paths represent the RemoteQuery paths
 * for left and right relations, the jointype identifies the type of JOIN, and
 * join_restrictlist contains the restrictinfo list for the JOIN.
 *
 * For a base relation, these parameters should be NULL.
 *
 * ExecNodes is the set of datanodes to which the query should be sent to.
 *
 * This function also marks the path with shippability of the quals.
 *
 * If any of the relations involved in this path is a temporary relation,
 * record that fact.
 */
static RemoteQueryPath *
create_remotequery_path(PlannerInfo *root, RelOptInfo *rel, ExecNodes *exec_nodes,
						RemoteQueryPath *leftpath, RemoteQueryPath *rightpath,
						JoinType jointype, List *join_restrictlist)
{
	RemoteQueryPath	*rqpath = makeNode(RemoteQueryPath);
	bool			unshippable_quals;

	if (rel->reloptkind == RELOPT_JOINREL && (!leftpath || !rightpath))
		elog(ERROR, "a join rel requires both the left path and right path");

	rqpath->path.pathtype = T_RemoteQuery;
	rqpath->path.parent = rel;
	/* PGXC_TODO: do we want to care about it */
	rqpath->path.param_info = NULL;
	rqpath->path.pathkeys = NIL;	/* result is always unsorted */
	rqpath->rqpath_en = exec_nodes;
	rqpath->leftpath = leftpath;
	rqpath->rightpath = rightpath;
	rqpath->jointype = jointype;
	rqpath->join_restrictlist = join_restrictlist;

	switch (rel->reloptkind)
	{
		case RELOPT_BASEREL:

			/*
			 * For baserels, the left/right path and restrictlist should be NULL.
			 *
			 * XXX I'm not sure if the same is true for other non-join rels, so
			 * let's only add it for the RELOPT_BASEREL case.
			 */
			Assert(leftpath == NULL && rightpath == NULL && join_restrictlist == NIL);

			/* fall-through */

		case RELOPT_OTHER_MEMBER_REL:
		{
			RangeTblEntry *rte = rt_fetch(rel->relid, root->parse->rtable);
			if (rte->rtekind != RTE_RELATION)
				elog(ERROR, "can not create remote path for ranges of type %d",
							rte->rtekind);
			rqpath->has_temp_rel = IsTempTable(rte->relid);
			unshippable_quals = !pgxc_is_expr_shippable((Expr *)extract_actual_clauses(rel->baserestrictinfo, false),
														NULL);
		}
		break;

		case RELOPT_JOINREL:
		{
			rqpath->has_temp_rel = leftpath->has_temp_rel ||
									rightpath->has_temp_rel;
			unshippable_quals = !pgxc_is_expr_shippable((Expr *)extract_actual_clauses(join_restrictlist, false),
														NULL);
		}
		break;

		default:
			elog(ERROR, "can not create remote path for relation of type %d",
							rel->reloptkind);
	}

	rqpath->has_unshippable_qual = unshippable_quals;

	/* PGXCTODO - set cost properly */
	cost_remotequery(rqpath, root, rel);

	return rqpath;
}
