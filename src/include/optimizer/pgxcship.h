/*-------------------------------------------------------------------------
 *
 * pgxcship.h
 *		Functionalities for the evaluation of expression shippability
 *		to remote nodes
 *
 *
 * Portions Copyright (c) 1996-2012 PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/optimizer/pgxcship.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGXCSHIP_H
#define PGXCSHIP_H

#include "nodes/parsenodes.h"
#include "nodes/relation.h"
#include "pgxc/locator.h"


/* Determine if query is shippable */
extern ExecNodes *pgxc_is_query_shippable(Query *query, int query_level);
/* Determine if an expression is shippable */
extern bool pgxc_is_expr_shippable(Expr *node, bool *has_aggs);

#endif
