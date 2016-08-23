/*-------------------------------------------------------------------------
 *
 * nodemgr.h
 *  Routines for node management
 *
 *
 * Portions Copyright (c) 1996-2011  PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/nodemgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEMGR_H
#define NODEMGR_H

#include "nodes/parsenodes.h"

#define PGXC_NODENAME_LENGTH	64

/* Compile time max limits on number of coordinators and datanodes */
#define MAX_COORDINATORS		64
#define MAX_DATANODES			256

/* GUC parameters, limit for number of nodes */
extern int 	MaxDataNodes;
extern int 	MaxCoords;
/* Global number of nodes */
extern int 	NumDataNodes;
extern int 	NumCoords;

/* Node definition */
typedef struct
{
	Oid 		nodeoid;
	NameData	nodename;
	NameData	nodehost;
	int			nodeport;
	bool		nodeisprimary;
	bool 		nodeispreferred;
	bool		nodeishealthy;
} NodeDefinition;

extern void NodeTablesShmemInit(void);
extern Size NodeTablesShmemSize(void);

extern void PgxcNodeListAndCount(void);
extern void PgxcNodeGetOids(Oid **coOids, Oid **dnOids,
							int *num_coords, int *num_dns,
							bool update_preferred);
extern void PgxcNodeGetHealthMap(Oid *coOids, Oid *dnOids,
				int *num_coords, int *num_dns, bool *coHealthMap,
				bool *dnHealthMap);
extern NodeDefinition *PgxcNodeGetDefinition(Oid node);
extern void PgxcNodeAlter(AlterNodeStmt *stmt);
extern void PgxcNodeCreate(CreateNodeStmt *stmt);
extern void PgxcNodeRemove(DropNodeStmt *stmt);
extern void PgxcNodeDnListHealth(List *nodeList, bool *dnhealth);
extern bool PgxcNodeUpdateHealth(Oid node, bool status);

#endif	/* NODEMGR_H */
