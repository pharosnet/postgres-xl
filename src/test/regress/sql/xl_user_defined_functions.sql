--User defined functions have several limitations 
--Basic insert, update, delete test on multiple datanodes using plpgsql function is passing.

--default distributed by HASH(slotname)

create function xl_nodename_from_id1(integer) returns name as $$
declare 
	n name;
BEGIN
	select node_name into n from pgxc_node where node_id = $1;
	RETURN n;
END;$$ language plpgsql;

create table xl_Pline1 (
    slotname	char(20),
    phonenumber	char(20),
    comment	text,
    backlink	char(20)
);

create unique index xl_Pline1_name on xl_Pline1 using btree (slotname bpchar_ops);

--insert Plines
insert into  xl_Pline1 values ('PL.001', '-0', 'Central call', 'PS.base.ta1');
insert into  xl_Pline1 values ('PL.002', '-101', '', 'PS.base.ta2');
insert into  xl_Pline1 values ('PL.003', '-102', '', 'PS.base.ta3');
insert into  xl_Pline1 values ('PL.004', '-103', '', 'PS.base.ta5');
insert into  xl_Pline1 values ('PL.005', '-104', '', 'PS.base.ta6');
insert into  xl_Pline1 values ('PL.006', '-106', '', 'PS.base.tb2');
insert into  xl_Pline1 values ('PL.007', '-108', '', 'PS.base.tb3');
insert into  xl_Pline1 values ('PL.008', '-109', '', 'PS.base.tb4');
insert into  xl_Pline1 values ('PL.009', '-121', '', 'PS.base.tb5');
insert into  xl_Pline1 values ('PL.010', '-122', '', 'PS.base.tb6');
insert into  xl_Pline1 values ('PL.011', '-122', '', 'PS.base.tb6');
insert into  xl_Pline1 values ('PL.012', '-122', '', 'PS.base.tb6');
insert into  xl_Pline1 values ('PL.013', '-122', '', 'PS.base.tb6');
insert into  xl_Pline1 values ('PL.014', '-122', '', 'PS.base.tb6');
insert into  xl_Pline1 values ('PL.015', '-134', '', 'PS.first.ta1');
insert into  xl_Pline1 values ('PL.016', '-137', '', 'PS.first.ta3');
insert into  xl_Pline1 values ('PL.017', '-139', '', 'PS.first.ta4');
insert into  xl_Pline1 values ('PL.018', '-362', '', 'PS.first.tb1');
insert into  xl_Pline1 values ('PL.019', '-363', '', 'PS.first.tb2');
insert into  xl_Pline1 values ('PL.020', '-364', '', 'PS.first.tb3');
insert into  xl_Pline1 values ('PL.021', '-365', '', 'PS.first.tb5');
insert into  xl_Pline1 values ('PL.022', '-367', '', 'PS.first.tb6');
insert into  xl_Pline1 values ('PL.023', '-367', '', 'PS.first.tb6');
insert into  xl_Pline1 values ('PL.024', '-367', '', 'PS.first.tb6');
insert into  xl_Pline1 values ('PL.025', '-367', '', 'PS.first.tb6');
insert into  xl_Pline1 values ('PL.026', '-367', '', 'PS.first.tb6');
insert into  xl_Pline1 values ('PL.027', '-367', '', 'PS.first.tb6');
insert into  xl_Pline1 values ('PL.028', '-501', 'Fax entrance', 'PS.base.ta2');
insert into  xl_Pline1 values ('PL.029', '-502', 'Fax first floor', 'PS.first.ta1');

create function xl_insert_Pline_test(int) returns boolean as $$
BEGIN
	IF $1 < 20 THEN
		insert into  xl_Pline1 values ('PL.030', '-367', '', 'PS.first.tb6');
		insert into  xl_Pline1 values ('PL.031', '-367', '', 'PS.first.tb6');
		insert into  xl_Pline1 values ('PL.032', '-367', '', 'PS.first.tb6');
		insert into  xl_Pline1 values ('PL.033', '-367', '', 'PS.first.tb6');
		insert into  xl_Pline1 values ('PL.034', '-367', '', 'PS.first.tb6');
		insert into  xl_Pline1 values ('PL.035', '-367', '', 'PS.first.tb6');
		insert into  xl_Pline1 values ('PL.036', '-367', '', 'PS.first.tb6');
		RETURN TRUE;
	ELSE
		RETURN FALSE;
	END IF;
END;$$ language plpgsql;

select xl_insert_Pline_test(1);

select xl_nodename_from_id1(xc_node_id), * from xl_Pline1 order by slotname;


create function xl_update_Pline_test(int) returns boolean as $$
BEGIN
	IF $1 < 20 THEN
		update xl_Pline1 set phonenumber = '400' where slotname = 'PL.030';
		update xl_Pline1 set phonenumber = '400' where slotname = 'PL.031';
		update xl_Pline1 set phonenumber = '400' where slotname = 'PL.032';
		update xl_Pline1 set phonenumber = '400' where slotname = 'PL.033';
		update xl_Pline1 set phonenumber = '400' where slotname = 'PL.034';
		update xl_Pline1 set phonenumber = '400' where slotname = 'PL.035';
		update xl_Pline1 set phonenumber = '400' where slotname = 'PL.036';
		RETURN TRUE;
	ELSE
		RETURN FALSE;
	END IF;
END;$$ language plpgsql;

select xl_update_Pline_test(1);

select xl_nodename_from_id1(xc_node_id), * from xl_Pline1 order by slotname;

create function xl_delete_Pline_test(int) returns boolean as $$
BEGIN
	IF $1 < 20 THEN
		delete from xl_Pline1 where slotname = 'PL.030';
		delete from xl_Pline1 where slotname = 'PL.031';
		delete from xl_Pline1 where slotname = 'PL.032';
		delete from xl_Pline1 where slotname = 'PL.033';
		delete from xl_Pline1 where slotname = 'PL.034';
		delete from xl_Pline1 where slotname = 'PL.035';
		delete from xl_Pline1 where slotname = 'PL.036';

		RETURN TRUE;
	ELSE
		RETURN FALSE;
	END IF;
END;$$ language plpgsql;

select xl_delete_Pline_test(1);

select xl_nodename_from_id1(xc_node_id), * from xl_Pline1 order by slotname;

-- other user-defined-functions:
--
-- Test the FOUND magic variable
--
-- table distributed by hash on a by default
CREATE TABLE xl_found_test_tbl (a int, b int) ;

create function xl_test_found()
  returns boolean as '
  declare
  begin
  insert into xl_found_test_tbl values (1, 1);
  if FOUND then
     insert into xl_found_test_tbl values (2, 2);
  end if;

  update xl_found_test_tbl set b = 100 where a = 1;
  if FOUND then
    insert into xl_found_test_tbl values (3, 3);
  end if;

  delete from xl_found_test_tbl where a = 9999; -- matches no rows
  if not FOUND then
    insert into xl_found_test_tbl values (4, 4);
  end if;

  for i in 1 .. 10 loop
    -- no need to do anything
  end loop;
  if FOUND then
    insert into xl_found_test_tbl values (5, 5);
  end if;

  -- never executes the loop
  for i in 2 .. 1 loop
    -- no need to do anything
  end loop;
  if not FOUND then
    insert into xl_found_test_tbl values (6, 6);
  end if;
  return true;
  end;' language plpgsql;

select xl_test_found();
select * from xl_found_test_tbl order by 1;

--
-- Test set-returning functions for PL/pgSQL
--

create function xl_test_table_func_rec() returns setof xl_found_test_tbl as '
DECLARE
	rec RECORD;
BEGIN
	FOR rec IN select * from xl_found_test_tbl LOOP
		RETURN NEXT rec;
	END LOOP;
	RETURN;
END;' language plpgsql;

select * from xl_test_table_func_rec() order by 1;


-- reads from just 1 node, reads from multiple nodes, writes to just node, writes to multiple nodes, writes to one node and then read again, do a join which requires data from one node, multiple nodes, use ddl etc

--reads from multiple nodes
create function xl_test_table_func_row() returns setof xl_found_test_tbl as '
DECLARE
	row xl_found_test_tbl%ROWTYPE;
BEGIN
	FOR row IN select * from xl_found_test_tbl LOOP
		RETURN NEXT row;
	END LOOP;
	RETURN;
END;' language plpgsql;

select * from xl_test_table_func_row() order by 1;

select xl_nodename_from_id1(xc_node_id), * from xl_found_test_tbl;


--reads from just 1 node
create function xl_read_from_one_node(name) returns setof xl_found_test_tbl as '
DECLARE
	row xl_found_test_tbl%ROWTYPE;
BEGIN
	FOR row IN select * from xl_found_test_tbl where a in (select a from xl_found_test_tbl where xl_nodename_from_id1(xc_node_id) = $1) LOOP
		RETURN NEXT row;
	END LOOP;
	RETURN;
END;' language plpgsql;

select xl_read_from_one_node('datanode_1');
select xl_read_from_one_node('datanode_2');

update xl_found_test_tbl set b = a;--re-set
 
--writes to just node
create function xl_write_to_one_node(name) returns void as '
BEGIN
	update xl_found_test_tbl set b = b * 100 where a in (select a from xl_found_test_tbl where xl_nodename_from_id1(xc_node_id) = $1);
	RETURN;
END;' language plpgsql;

select xl_write_to_one_node('datanode_1');

select xl_nodename_from_id1(xc_node_id), * from xl_found_test_tbl;

update xl_found_test_tbl set b = a;--re-set

--writes to multiple nodes
create function xl_write_to_multiple_nodes() returns void as '
BEGIN
	update xl_found_test_tbl set b = b * 100;
	RETURN;
END;' language plpgsql;

select xl_write_to_multiple_nodes();

select xl_nodename_from_id1(xc_node_id), * from xl_found_test_tbl;

update xl_found_test_tbl set b = a;--re-set

--writes to one node and then read again,

create function xl_write_read_from_one_node(name) returns setof xl_found_test_tbl as '
DECLARE
	row xl_found_test_tbl%ROWTYPE;
BEGIN
	update xl_found_test_tbl set b = b * 100 where a in (select a from xl_found_test_tbl where xl_nodename_from_id1(xc_node_id) = $1);
	FOR row IN select * from xl_found_test_tbl where a in (select a from xl_found_test_tbl where xl_nodename_from_id1(xc_node_id) = $1) LOOP
		RETURN NEXT row;
	END LOOP;
	RETURN;
END;' language plpgsql;

select xl_write_read_from_one_node('datanode_1');

select xl_nodename_from_id1(xc_node_id), * from xl_found_test_tbl;

update xl_found_test_tbl set b = a;--re-set

-- do a join which requires data from one node,
-- table is replicated on both nodes.
CREATE TABLE xl_join1_tbl (c int, d int) distribute by replication;

insert into xl_join1_tbl values (1, 100);
insert into xl_join1_tbl values (2, 200);
insert into xl_join1_tbl values (3, 300);
insert into xl_join1_tbl values (4, 400);
insert into xl_join1_tbl values (5, 500);
insert into xl_join1_tbl values (6, 600);

create function xl_join_using_1node(name) returns void as '
BEGIN
	update xl_found_test_tbl set b = xl_join1_tbl.d from xl_join1_tbl
	where xl_join1_tbl.c in (select a from xl_found_test_tbl where xl_nodename_from_id1(xc_node_id) = $1) 
	and xl_found_test_tbl.a = xl_join1_tbl.c
	and xl_join1_tbl.c <= 3;
	RETURN;
END;' language plpgsql;

select xl_join_using_1node('datanode_1');

select xl_nodename_from_id1(xc_node_id), * from xl_found_test_tbl;

update xl_found_test_tbl set b = a;--re-set

-- do a join which requires data from multiple nodes
-- table distributed by hash(c) by default
CREATE TABLE xl_join2_tbl (c int, d int);

insert into xl_join2_tbl values (1, 100);
insert into xl_join2_tbl values (2, 200);
insert into xl_join2_tbl values (3, 300);
insert into xl_join2_tbl values (4, 400);
insert into xl_join2_tbl values (5, 500);
insert into xl_join2_tbl values (6, 600);


create function xl_join_using_more_nodes(name) returns void as '
BEGIN
	update xl_found_test_tbl set b = xl_join2_tbl.d from xl_join2_tbl
	where xl_join2_tbl.c in (select a from xl_found_test_tbl where xl_nodename_from_id1(xc_node_id) = $1) 
	and xl_found_test_tbl.a = xl_join2_tbl.c
	and xl_join2_tbl.c >= 3;
	RETURN;
END;' language plpgsql;

select xl_join_using_more_nodes('datanode_1');

select xl_nodename_from_id1(xc_node_id), * from xl_found_test_tbl;

update xl_found_test_tbl set b = a;--re-set

-- use ddl etc
--DDL Commands - Create - Drop - Alter - Rename - Truncate

--
create function xl_ddl_commands(name) returns void as '
BEGIN
	/* table distributed by hash on a by default*/
	CREATE TABLE xl_ddl_tbl1 (a int, b int) ;
	insert into xl_ddl_tbl1 values (1,1);
	insert into xl_ddl_tbl1 values (2,2);
	insert into xl_ddl_tbl1 values (3,3);
	insert into xl_ddl_tbl1 values (4,4);
	insert into xl_ddl_tbl1 values (5,5);
	insert into xl_ddl_tbl1 values (6,6);
	
	drop table xl_join2_tbl;

	/*table distributed by hash(c) by default*/
	CREATE TABLE xl_join3_tbl (c int);

	insert into xl_join3_tbl values (1);	
	insert into xl_join3_tbl values (2);
	insert into xl_join3_tbl values (3);
	insert into xl_join3_tbl values (4);
	insert into xl_join3_tbl values (5);
	insert into xl_join3_tbl values (6);
	
	alter table xl_join3_tbl add column d int;

	update xl_join3_tbl set d = c * 100;

	alter table xl_ddl_tbl1 rename to xl_ddl_tbl2;

	alter table xl_join3_tbl rename to xl_join4_tbl;

	truncate table xl_join1_tbl;

	update xl_ddl_tbl2 set b = xl_join4_tbl.d from xl_join4_tbl
	where xl_join4_tbl.c in (select a from xl_ddl_tbl2 where xl_nodename_from_id1(xc_node_id) = $1) 
	and xl_ddl_tbl2.a = xl_join4_tbl.c
	and xl_join4_tbl.c >= 3;

	RETURN;
END;' language plpgsql;

select xl_ddl_commands('datanode_1');

select xl_nodename_from_id1(xc_node_id), * from xl_join1_tbl; --truncated

select xl_nodename_from_id1(xc_node_id), * from xl_join2_tbl; -- dropped

select xl_nodename_from_id1(xc_node_id), * from xl_join3_tbl; -- renamed to xl_join4_tbl

select xl_nodename_from_id1(xc_node_id), * from xl_join4_tbl;

select xl_nodename_from_id1(xc_node_id), * from xl_ddl_tbl2;

update xl_found_test_tbl set b = a;--re-set

drop table xl_Pline1;
drop function xl_nodename_from_id1(integer);
drop function xl_insert_Pline_test(int);
drop function xl_update_Pline_test(int);
drop function xl_delete_Pline_test(int);
drop function xl_test_table_func_row();
drop function xl_test_table_func_rec();
drop function xl_test_found();
drop function xl_read_from_one_node(name);
drop function xl_write_to_one_node(name);
drop function xl_write_to_multiple_nodes();
drop function xl_write_read_from_one_node(name);
drop function xl_join_using_1node(name);
drop function xl_join_using_more_nodes(name);
drop function xl_ddl_commands(name);
drop TABLE xl_found_test_tbl;
drop TABLE xl_ddl_tbl2;
drop TABLE xl_join1_tbl;
drop TABLE xl_join2_tbl;
drop TABLE xl_join4_tbl;

