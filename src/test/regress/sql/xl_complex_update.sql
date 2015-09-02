-- Some complex UPDATE/DELETE queries also known to have problems
-- Check UPDATE with inherited target and an inherited source table
create table xl_foo(f1 int, f2 int);
create table xl_foo2(f3 int) inherits (xl_foo);
create table xl_bar(f1 int, f2 int);
create table xl_bar2(f3 int) inherits (xl_bar);
create table xl_foo1(f1 int, f2 int);--non inherited or inheriting table

insert into xl_foo values(1,1);
insert into xl_foo values(3,3);
insert into xl_foo1 values(1,1);
insert into xl_foo1 values(3,3);
insert into xl_foo2 values(2,2,2);
insert into xl_foo2 values(3,3,3);
insert into xl_bar values(1,1);
insert into xl_bar values(2,2);
insert into xl_bar values(3,3);
insert into xl_bar values(4,4);
insert into xl_bar2 values(1,1,1);
insert into xl_bar2 values(2,2,2);
insert into xl_bar2 values(3,3,3);
insert into xl_bar2 values(4,4,4);

create function xl_cu_nodename_from_id(integer) returns name as $$
declare 
	n name;
BEGIN
	select node_name into n from pgxc_node where node_id = $1;
	RETURN n;
END;$$ language plpgsql;

select xl_cu_nodename_from_id(xc_node_id), * from xl_foo; 

select xl_cu_nodename_from_id(xc_node_id), * from only xl_foo; 

select xl_cu_nodename_from_id(xc_node_id), * from xl_foo2;

select xl_cu_nodename_from_id(xc_node_id), * from xl_bar;

select xl_cu_nodename_from_id(xc_node_id), * from only xl_bar;

select xl_cu_nodename_from_id(xc_node_id), * from xl_bar2;

update xl_bar set f2 = f2 + 100 where f1 in (select f1 from xl_foo);-- fail as complex query part has parent table

update xl_bar set f2 = f2 + 100 where f1 in (select f1 from only xl_foo);-- pass as complex query part has only parent table

select xl_cu_nodename_from_id(xc_node_id), * from xl_bar;

select xl_cu_nodename_from_id(xc_node_id), * from only xl_bar;

update only xl_bar set f2 = f2 + 100 where f1 in (select f1 from xl_foo);-- fail as complex query part has parent table

update only xl_bar set f2 = f2 + 100 where f1 in (select f1 from only xl_foo);-- pass as complex query part has only parent table

select xl_cu_nodename_from_id(xc_node_id), * from xl_bar;

select xl_cu_nodename_from_id(xc_node_id), * from only xl_bar;

-- checking independent working of union all
select f1 from only xl_foo;

select f1+3 from only xl_foo;

select f1 from only xl_foo union all select f1+3 from only xl_foo;

-- Check UPDATE with inherited target and an appendrel subquery
-- All below cases fail as union all in complex update subquery is not yet supported.
update xl_bar set f2 = f2 + 100
from
  ( select f1 from xl_foo union all select f1+3 from xl_foo ) ss
where xl_bar.f1 = ss.f1;--fail

update xl_bar set f2 = f2 + 100
from
  ( select f1 from only xl_foo union all select f1+3 from xl_foo ) ss
where xl_bar.f1 = ss.f1;--fail

update xl_bar set f2 = f2 + 100
from
  ( select f1 from only xl_foo union all select f1+3 from only xl_foo ) ss
where xl_bar.f1 = ss.f1;--pass

update only xl_bar set f2 = f2 + 100
from
  ( select f1 from only xl_foo union all select f1+3 from only xl_foo ) ss
where xl_bar.f1 = ss.f1;--pass

-- xl_foo1 is a non-inheriting or inherited table, still union in subquery fails
update xl_bar set f2 = f2 + 100
from
  ( select f1 from xl_foo1 union all select f1+3 from xl_foo1 ) ss
where xl_bar.f1 = ss.f1;--pass

update only xl_bar set f2 = f2 + 100
from
  ( select f1 from xl_foo1 union all select f1+3 from xl_foo1 ) ss
where xl_bar.f1 = ss.f1;--pass

--FROM is supported but only when the join is on the distribution column
-- distributed by default by HASH(tmpunique1)
CREATE TABLE xl_tmp (              
tmpunique1 int4,
stringu1 name,
stringu2 name,
string4 name
);
-- distributed by default by HASH(unique1)
CREATE TABLE xl_onek (    
unique1 int4,
unique2 int4,
two int4,
four int4,
stringu1 name,
stringu2 name,
string4 name
);


--Explicit join on distribution columns of table with similar distribution type is supported. 

UPDATE xl_tmp
   SET stringu1 = xl_onek.stringu1
   FROM xl_onek
   WHERE xl_onek.unique1 = xl_tmp.tmpunique1;--pass as join is on distribution column

UPDATE xl_tmp
   SET stringu1 = xl_onek.stringu1
   FROM xl_onek
   WHERE xl_onek.stringu1 = xl_tmp.stringu1;--fail as join is on normal non-distribution column


--With explicit join, other equation clauses supported:
	-- on distribution column of from-list table
	-- on non-distribution column of from-list table
	-- on distribution column of target table
	-- on non-distribution column of target table
	-- However currently ‘=’ for integer is not supported. (bug)

	-- on distribution column of from-list table


UPDATE xl_tmp
   SET stringu1 = xl_onek.stringu1
   FROM xl_onek
   WHERE xl_onek.unique1 <= 3 and
	  xl_onek.unique1 = xl_tmp.tmpunique1;


	-- on non-distribution column of from-list table
UPDATE xl_tmp
   SET stringu1 = xl_onek.stringu1
   FROM xl_onek
   WHERE xl_onek.stringu1 = 'test-string' and
	  xl_onek.unique1 = xl_tmp.tmpunique1;


	-- on distribution column of target table


UPDATE xl_tmp
   SET stringu1 = xl_onek.stringu1
   FROM xl_onek
   WHERE xl_tmp.tmpunique1 <= 3 and
	  xl_onek.unique1 = xl_tmp.tmpunique1;


	-- on non-distribution column of target table

UPDATE xl_tmp
   SET stringu1 = xl_onek.stringu1
   FROM xl_onek
   WHERE xl_tmp.stringu1 = 'test-string' and
	  xl_onek.unique1 = xl_tmp.tmpunique1;

	-- However currently ‘=’ for integer is not supported. (bug)

UPDATE xl_tmp
   SET stringu1 = xl_onek.stringu1
   FROM xl_onek
   WHERE xl_onek.unique1 = 3 and
	  xl_onek.unique1 = xl_tmp.tmpunique1;

UPDATE xl_tmp
   SET stringu1 = xl_onek.stringu1
   FROM xl_onek
   WHERE xl_tmp.tmpunique1 = 3 and
	  xl_onek.unique1 = xl_tmp.tmpunique1;

--Implicit / cartesian join with explicit equation clause not supported:
	-- on distribution columns of from-list table
	-- on non-distribution column of from-list table 
	-- on distribution column of target table
	-- on non-distribution column of target table

	-- on distribution columns of from-list table
UPDATE xl_tmp
   SET stringu1 = xl_onek.stringu1
   FROM xl_onek
   WHERE xl_onek.unique1 = 3;


UPDATE xl_tmp
   SET stringu1 = xl_onek.stringu1
   FROM xl_onek
   WHERE xl_onek.unique1 <= 3;

	-- on non-distribution column of from-list table 


UPDATE xl_tmp
   SET stringu1 = xl_onek.stringu1
   FROM xl_onek
   WHERE xl_onek.stringu1 = 'test-string';

	-- on distribution column of target table


UPDATE xl_tmp
   SET stringu1 = xl_onek.stringu1
   FROM xl_onek
   WHERE xl_tmp.tmpunique1 = 3;


UPDATE xl_tmp
   SET stringu1 = xl_onek.stringu1
   FROM xl_onek
   WHERE xl_tmp.tmpunique1 <= 3;

	-- on non-distribution column of target table

UPDATE xl_tmp
   SET stringu1 = xl_onek.stringu1
   FROM xl_onek
   WHERE xl_tmp.stringu1 = 'test-string';


drop function xl_cu_nodename_from_id(integer);
drop table xl_foo2;
drop table xl_bar2;
drop table xl_bar;
drop table xl_foo;
drop table xl_foo1;
drop table xl_tmp;
drop table xl_onek;
