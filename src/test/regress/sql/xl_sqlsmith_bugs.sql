-- #102 Issue
CREATE TABLE xl_join_t1 (val1 int, val2 int);
CREATE TABLE xl_join_t2 (val1 int, val2 int);
CREATE TABLE xl_join_t3 (val1 int, val2 int);
INSERT INTO xl_join_t1 VALUES (1,10),(2,20);
INSERT INTO xl_join_t2 VALUES (3,30),(4,40);
INSERT INTO xl_join_t3 VALUES (5,50),(6,60);
EXPLAIN SELECT * FROM xl_join_t1
        INNER JOIN xl_join_t2 ON xl_join_t1.val1 = xl_join_t2.val2
        INNER JOIN xl_join_t3 ON xl_join_t1.val1 = xl_join_t3.val1;
SELECT * FROM xl_join_t1
        INNER JOIN xl_join_t2 ON xl_join_t1.val1 = xl_join_t2.val2
        INNER JOIN xl_join_t3 ON xl_join_t1.val1 = xl_join_t3.val1;

select
  cast(coalesce(8,
    (select pid from pg_catalog.pg_stat_activity limit 1 offset 29)
) as integer) as c0,
  33 as c1,
  (select objsubid from pg_catalog.pg_seclabel limit 1 offset 34)
 as c2,
  (select ordinal_position from information_schema.parameters limit 1 offset 1)
 as c3,
  cast(coalesce(pg_catalog.pg_postmaster_start_time(),
      pg_catalog.min(
        cast(null as timestamp with time zone)) over (partition by subq_1.c1 order by subq_1.c1,subq_1.c5)) as timestamp with time zone) as c4,
  (select character_maximum_length from information_schema.domains limit 1 offset 36)
 as c5
from
  (select
        (select character_maximum_length from information_schema.user_defined_types limit 1 offset 6)
 as c0,
        sample_7.conpfeqop as c1,
        sample_6.amprocfamily as c2,
          pg_catalog.string_agg(
            cast(sample_7.consrc as text),
            cast(sample_7.consrc as text)) over (partition by sample_6.amprocrighttype order by sample_6.amprocfamily,sample_6.amprocrighttype) as c3,
        34 as c4,
        pg_catalog.abs(
          cast(cast(coalesce((select total_time from pg_catalog.pg_stat_user_functions limit 1 offset 6)
,
            null) as double precision) as double precision)) as c5
      from
        pg_catalog.pg_amop as ref_11
            left join pg_catalog.pg_amproc as sample_6 tablesample system (7)
            on (ref_11.amopmethod = sample_6.amprocfamily )
          inner join pg_catalog.pg_constraint as sample_7 tablesample system (9.5)
          on (ref_11.amopsortfamily = sample_7.connamespace ),
        lateral (select
              ref_11.amopmethod as c0
            from
              pg_catalog.pg_user_mapping as ref_12
            where ((sample_7.confdeltype is NULL)
                and (((select character_maximum_length from information_schema.element_types limit 1 offset 16)
 >= 39)
                  and (99 > (select objsubid from pg_catalog.pg_depend limit 1 offset 10)
)))
              or (59 <> 82)
            limit 75) as subq_0
      where EXISTS (
        select
            76 as c0,
            ref_13.val2 as c1,
            7 as c2,
            pg_catalog.has_server_privilege(
              cast(null as oid),
              cast(pg_catalog.to_hex(
                cast(null as bigint)) as text),
              cast(null as text)) as c3,
              pg_catalog.bit_out(
              cast(null as bit)) as c4,
              pg_catalog.int82ge(
              cast(pg_catalog.pg_stat_get_db_conflict_snapshot(
                cast(pg_catalog.pg_my_temp_schema() as oid)) as bigint),
              cast(null as smallint)) as c5,
              ref_13.val1 as c6
          from
              public.xl_join_t3 as ref_13
          where (ref_13.val2 = pg_catalog.pg_stat_get_xact_tuples_inserted(
                cast(pg_catalog.lo_import(
                  cast((select name from pg_catalog.pg_cursors limit 1 offset 21)
 as text)) as oid)))
              or (25 > (select ordinal_position from information_schema.attributes limit 1 offset 17)
))
      limit 87) as subq_1
where
cast(coalesce((select node_port from pg_catalog.pgxc_node limit 1 offset 9)
,
    cast(coalesce(pg_catalog.pg_backend_pid(),
      subq_1.c0) as integer)) as integer) is NULL;


DROP TABLE xl_join_t1;
DROP TABLE xl_join_t2;
DROP TABLE xl_join_t3;


