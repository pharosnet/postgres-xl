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
