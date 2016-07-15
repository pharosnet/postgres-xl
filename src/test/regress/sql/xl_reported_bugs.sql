-- #16
-- Windowing function throws an error when subquery has ORDER BY clause
CREATE TABLE test (a int, b int);
EXPLAIN SELECT last_value(a) OVER (PARTITION by b) FROM (SELECT * FROM test) AS s ORDER BY a;
EXPLAIN SELECT last_value(a) OVER (PARTITION by b) FROM (SELECT * FROM test  ORDER BY a) AS s ORDER BY a;
DROP TABLE test;
