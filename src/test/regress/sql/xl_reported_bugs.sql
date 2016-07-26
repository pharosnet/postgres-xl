-- #9
-- Fails to see DDL's effect inside a function
create function xl_getint() returns integer as $$
declare
	i integer;
BEGIN
	create table inttest(a int, b int);
	insert into inttest values (1,1);
	select a into i from inttest limit 1;
	RETURN i;
END;$$ language plpgsql;

select xl_getint();
select * from inttest;

create function xl_cleanup() returns integer as $$
declare
	i integer;
BEGIN
	drop function xl_getint();
	drop table inttest;
	select a into i from inttest limit 1;
	RETURN i;
END;$$ language plpgsql;

select xl_cleanup();

drop function xl_cleanup();

-- #4
-- Tableoid to relation name mapping broken
create table cities (
	name		text,
	population	float8,
	altitude	int		-- (in ft)
);

create table capitals (
	state		char(2)
) inherits (cities);

-- Create unique indexes.  Due to a general limitation of inheritance,
-- uniqueness is only enforced per-relation.  Unique index inference
-- specification will do the right thing, though.
create unique index cities_names_unique on cities (name);
create unique index capitals_names_unique on capitals (name);

-- prepopulate the tables.
insert into cities values ('San Francisco', 7.24E+5, 63);
insert into cities values ('Las Vegas', 2.583E+5, 2174);
insert into cities values ('Mariposa', 1200, 1953);

insert into capitals values ('Sacramento', 3.694E+5, 30, 'CA');
insert into capitals values ('Madison', 1.913E+5, 845, 'WI');

-- Tests proper for inheritance:
select * from capitals;

-- Succeeds:
insert into cities values ('Las Vegas', 2.583E+5, 2174) on conflict do nothing;
insert into capitals values ('Sacramento', 4664.E+5, 30, 'CA') on conflict (name) do update set population = excluded.population;
-- Wrong "Sacramento", so do nothing:
insert into capitals values ('Sacramento', 50, 2267, 'NE') on conflict (name) do nothing;
select * from capitals;
insert into cities values ('Las Vegas', 5.83E+5, 2001) on conflict (name) do update set population = excluded.population, altitude = excluded.altitude;
select tableoid::regclass, * from cities;
insert into capitals values ('Las Vegas', 5.83E+5, 2222, 'NV') on conflict (name) do update set population = excluded.population;
-- Capitals will contain new capital, Las Vegas:
select * from capitals;
-- Cities contains two instances of "Las Vegas", since unique constraints don't
-- work across inheritance:
select tableoid::regclass, * from cities;
-- This only affects "cities" version of "Las Vegas":
insert into cities values ('Las Vegas', 5.86E+5, 2223) on conflict (name) do update set population = excluded.population, altitude = excluded.altitude;
select tableoid::regclass, * from cities;

-- clean up
drop table capitals;
drop table cities;

-- #16
-- Windowing function throws an error when subquery has ORDER BY clause
CREATE TABLE test (a int, b int);
EXPLAIN SELECT last_value(a) OVER (PARTITION by b) FROM (SELECT * FROM test) AS s ORDER BY a;
EXPLAIN SELECT last_value(a) OVER (PARTITION by b) FROM (SELECT * FROM test  ORDER BY a) AS s ORDER BY a;
DROP TABLE test;

-- #5
-- Type corresponding to a view does not exist on datanode
CREATE TABLE test (a int, b int);
CREATE VIEW v AS SELECT * FROM test;

-- Using view type throws an error
CREATE FUNCTION testf (x v) RETURNS INTEGER AS $$ SELECT 1; $$ LANGUAGE SQL;

-- Same works for table type though
CREATE FUNCTION testf (x test) RETURNS INTEGER AS $$ SELECT 1; $$ LANGUAGE SQL;

DROP FUNCTION testf (x test);
DROP VIEW v;
DROP TABLE test;


-- #7
-- "cache lookup failed" error
CREATE TABLE test (a int, b int);
CREATE VIEW v AS SELECT * FROM test;
SELECT v FROM v;
DROP VIEW v;
DROP TABLE test;
