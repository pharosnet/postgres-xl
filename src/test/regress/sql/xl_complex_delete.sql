-- Some complex delete from/DELETE queries also known to have problems
-- Check delete from with inherited target and an inherited source table
create table xl_d_foo(f1 int, f2 int);
create table xl_d_foo2(f3 int) inherits (xl_d_foo);
create table xl_d_bar(f1 int, f2 int);
create table xl_d_bar2(f3 int) inherits (xl_d_bar);
create table xl_d_foo1(f1 int, f2 int);--non inherited or inheriting table

insert into xl_d_foo values(1,1);
insert into xl_d_foo values(3,3);
insert into xl_d_foo1 values(1,1);
insert into xl_d_foo1 values(3,3);
insert into xl_d_foo2 values(2,2,2);
insert into xl_d_foo2 values(3,3,3);
insert into xl_d_bar values(1,1);
insert into xl_d_bar values(2,2);
insert into xl_d_bar values(3,3);
insert into xl_d_bar values(4,4);
insert into xl_d_bar2 values(1,1,1);
insert into xl_d_bar2 values(2,2,2);
insert into xl_d_bar2 values(3,3,3);
insert into xl_d_bar2 values(4,4,4);

create function xl_cd_nodename_from_id(integer) returns name as $$
declare 
	n name;
BEGIN
	select node_name into n from pgxc_node where node_id = $1;
	RETURN n;
END;$$ language plpgsql;

select xl_cd_nodename_from_id(xc_node_id), * from xl_d_foo; 

select xl_cd_nodename_from_id(xc_node_id), * from only xl_d_foo; 

select xl_cd_nodename_from_id(xc_node_id), * from xl_d_foo2;

select xl_cd_nodename_from_id(xc_node_id), * from xl_d_bar;

select xl_cd_nodename_from_id(xc_node_id), * from only xl_d_bar;

select xl_cd_nodename_from_id(xc_node_id), * from xl_d_bar2;

delete from xl_d_bar where f1 in (select f1 from xl_d_foo);-- fail as complex query part has parent table

delete from xl_d_bar where f1 in (select f1 from only xl_d_foo);-- pass as complex query part has only parent table

select xl_cd_nodename_from_id(xc_node_id), * from xl_d_bar;

select xl_cd_nodename_from_id(xc_node_id), * from only xl_d_bar;

delete from only xl_d_bar where f1 in (select f1 from xl_d_foo);-- fail as complex query part has parent table

delete from only xl_d_bar where f1 in (select f1 from only xl_d_foo);-- pass as complex query part has only parent table

select xl_cd_nodename_from_id(xc_node_id), * from xl_d_bar;

select xl_cd_nodename_from_id(xc_node_id), * from only xl_d_bar;

-- checking independent working of union all
select f1 from only xl_d_foo;

select f1+3 from only xl_d_foo;

select f1 from only xl_d_foo union all select f1+3 from only xl_d_foo;

-- Check delete from with inherited target and an appendrel subquery
-- All below cases fail as union all in complex delete from subquery is not yet supported.
delete from xl_d_bar
where f1 in ( select f1 from xl_d_foo union all select f1+3 from xl_d_foo );--fail

delete from xl_d_bar 
where f1 in ( select f1 from only xl_d_foo union all select f1+3 from xl_d_foo );--fail

delete from xl_d_bar 
where f1 in ( select f1 from only xl_d_foo union all select f1+3 from only xl_d_foo );--pass

delete from only xl_d_bar 
where f1 in ( select f1 from only xl_d_foo union all select f1+3 from only xl_d_foo );--pass

-- xl_d_foo1 is a non-inheriting or inherited table, still union in subquery fails
delete from xl_d_bar
where f1 in ( select f1 from xl_d_foo1 union all select f1+3 from xl_d_foo1 );--pass

delete from only xl_d_bar
where f1 in ( select f1 from xl_d_foo1 union all select f1+3 from xl_d_foo1 );--pass

drop function xl_cd_nodename_from_id(integer);
drop table xl_d_foo2;
drop table xl_d_bar2;
drop table xl_d_bar;
drop table xl_d_foo;
drop table xl_d_foo1;
