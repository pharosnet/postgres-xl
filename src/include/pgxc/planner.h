/*-------------------------------------------------------------------------
 *
 * planner.h
 *		Externally declared locator functions
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/planner.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGXCPLANNER_H
#define PGXCPLANNER_H

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "pgxc/locator.h"
#include "tcop/dest.h"
#include "nodes/relation.h"


typedef enum
{
	COMBINE_TYPE_NONE,			/* it is known that no row count, do not parse */
	COMBINE_TYPE_SUM,			/* sum row counts (partitioned, round robin) */
	COMBINE_TYPE_SAME			/* expect all row counts to be the same (replicated write) */
}	CombineType;

/* For sorting within RemoteQuery handling */
/*
 * It is pretty much like Sort, but without Plan. We may use Sort later.
 */
typedef struct
{
	NodeTag		type;
	int			numCols;		/* number of sort-key columns */
	AttrNumber *sortColIdx;		/* their indexes in the target list */
	Oid		   *sortOperators;	/* OIDs of operators to sort them by */
	Oid		   *sortCollations;
	bool	   *nullsFirst;		/* NULLS FIRST/LAST directions */
} SimpleSort;

/*
 * Determines if query has to be launched
 * on Coordinators only (SEQUENCE DDL),
 * on Datanodes (normal Remote Queries),
 * or on all Postgres-XC nodes (Utilities and DDL).
 */
typedef enum
{
	EXEC_ON_CURRENT,
	EXEC_ON_DATANODES,
	EXEC_ON_COORDS,
	EXEC_ON_ALL_NODES,
	EXEC_ON_NONE
} RemoteQueryExecType;

typedef enum
{
	EXEC_DIRECT_NONE,
	EXEC_DIRECT_LOCAL,
	EXEC_DIRECT_LOCAL_UTILITY,
	EXEC_DIRECT_UTILITY,
	EXEC_DIRECT_SELECT,
	EXEC_DIRECT_INSERT,
	EXEC_DIRECT_UPDATE,
	EXEC_DIRECT_DELETE
} ExecDirectType;

/*
 * Contains instructions on processing a step of a query.
 * In the prototype this will be simple, but it will eventually
 * evolve into a GridSQL-style QueryStep.
 */
typedef struct
{
	Scan			scan;
	ExecDirectType		exec_direct_type;	/* track if remote query is execute direct and what type it is */
	char			*sql_statement;
	ExecNodes		*exec_nodes;		/* List of Datanodes where to launch query */
	CombineType		combine_type;
	SimpleSort		*sort;
	bool			read_only;		/* do not use 2PC when committing read only steps */
	bool			force_autocommit;	/* some commands like VACUUM require autocommit mode */
	char			*statement;		/* if specified use it as a PreparedStatement name on Datanodes */
	char			*cursor;		/* if specified use it as a Portal name on Datanodes */
	int             rq_num_params;      /* number of parameters present in
										   remote statement */
	Oid             *rq_param_types;    /* parameter types for the remote
										   statement */
	RemoteQueryExecType	exec_type;
	int			reduce_level;		/* in case of reduced JOIN, it's level    */
	char			*outer_alias;
	char			*inner_alias;
	int			outer_reduce_level;
	int			inner_reduce_level;
	Relids			outer_relids;
	Relids			inner_relids;
	char			*inner_statement;
	char			*outer_statement;
	char			*join_condition;
	bool			has_row_marks;		/* Did SELECT had FOR UPDATE/SHARE? */
	bool			has_ins_child_sel_parent;	/* This node is part of an INSERT SELECT that
								 * inserts into child by selecting from its parent */

	bool            rq_finalise_aggs;   /* Aggregates should be finalised at
										   the 
										 * Datanode */
	bool            rq_sortgroup_colno; /* Use resno for sort group references
										 * instead of expressions */
	Query           *remote_query;  /* Query structure representing the query
									   to be
									 * sent to the datanodes */
	List            *base_tlist;    /* the targetlist representing the result
									   of 
									 * the query to be sent to the datanode */

	/*
	 * Reference targetlist of Vars to match the Vars in the plan nodes on
	 * coordinator to the corresponding Vars in the remote_query.  These
	 * targetlists are used to while replacing/adding targetlist and quals in
	 * the remote_query.
	 */ 
	List            *coord_var_tlist;
	List            *query_var_tlist;
	bool			is_temp;

} RemoteQuery;

/*
 * Going to be a RemoteQuery replacement.
 * Submit left subplan to the nodes defined by the Distribution and combine
 * results.
 */
typedef struct
{
	Scan		scan;
	char 		distributionType;
	AttrNumber	distributionKey;
	List 	   *distributionNodes;
	List 	   *distributionRestrict;
	List 	   *nodeList;
	bool 		execOnAll;
	SimpleSort *sort;
	char	   *cursor;
	int			unique;
} RemoteSubplan;

/*
 * FQS_context
 * This context structure is used by the Fast Query Shipping walker, to gather
 * information during analysing query for Fast Query Shipping.
 */
typedef struct
{
	bool		sc_for_expr;		/* if false, the we are checking shippability
									 * of the Query, otherwise, we are checking
									 * shippability of a stand-alone expression.
									 */
	Bitmapset	*sc_shippability;	/* The conditions for (un)shippability of the
									 * query.
									 */
	Query		*sc_query;			/* the query being analysed for FQS */
	int			sc_query_level;		/* level of the query */
	int			sc_max_varlevelsup;	/* maximum upper level referred to by any
									 * variable reference in the query. If this
									 * value is greater than 0, the query is not
									 * shippable, if shipped alone.
									 */
	ExecNodes	*sc_exec_nodes;		/* nodes where the query should be executed */
	ExecNodes	*sc_subquery_en;	/* ExecNodes produced by merging the ExecNodes
									 * for individual subqueries. This gets
									 * ultimately merged with sc_exec_nodes.
									 */
} Shippability_context;

/* enum for reasons as to why a query/expression is not FQSable */
typedef enum
{
	SS_UNSHIPPABLE_EXPR = 0,	/* it has unshippable expression */
	SS_NEED_SINGLENODE,			/* Has expressions which can be evaluated when
								 * there is only a single node involved.
								 * Athought aggregates too fit in this class, we
								 * have a separate status to report aggregates,
								 * see below.
								 */
	SS_NEEDS_COORD,				/* the query needs Coordinator */
	SS_VARLEVEL,				/* one of its subqueries has a VAR
								 * referencing an upper level query
								 * relation
								 */
	SS_NO_NODES,				/* no suitable nodes can be found to ship
								 * the query
								 */
	SS_UNSUPPORTED_EXPR,		/* it has expressions currently unsupported
								 * by FQS, but such expressions might be
								 * supported by FQS in future
								 */
	SS_HAS_AGG_EXPR,			/* it has aggregate expressions */
	SS_UPDATES_DISTRIBUTION_COLUMN	/* query updates distribution column */
} ShippabilityStat;

/* forbid SQL if unsafe, useful to turn off for development */
extern bool StrictStatementChecking;

extern bool pgxc_shippability_walker(Node *node, Shippability_context *sc_context);
extern bool pgxc_test_shippability_reason(Shippability_context *context,
											ShippabilityStat reason);
extern PlannedStmt *pgxc_direct_planner(Query *query, int cursorOptions,
										ParamListInfo boundParams);
extern List *AddRemoteQueryNode(List *stmts, const char *queryString,
								RemoteQueryExecType remoteExecType);
extern PlannedStmt *pgxc_planner(Query *query, int cursorOptions,
		                                 ParamListInfo boundParams);
extern ExecNodes *pgxc_is_query_shippable(Query *query, int query_level);


#endif   /* PGXCPLANNER_H */
