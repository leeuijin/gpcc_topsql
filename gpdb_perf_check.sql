# 데이터베이스 리스트 & 데이터 베이스 사이즈 

select t1.datname AS db_name,pg_size_pretty(pg_database_size(t1.datname)) as db_size from pg_database t1 order by pg_database_size(t1.datname) desc;

#테이블 리스트

SELECT * FROM pg_catalog.pg_tables WHERE schemaname NOT IN ('gp_toolkit', 'information_schema', 'pg_catalog','pg_aoseg','pg_toast') order by 1,2;

SELECT b.nspname , relname, c.relkind,c.relstorage FROM pg_class c,pg_namespace b WHERE c.relnamespace=b.oid and b.nspname NOT IN ('gp_toolkit', 'information_schema', 'pg_catalog','pg_aoseg','pg_toast') AND relname NOT LIKE '%_1_prt_%' ORDER BY b.nspname,c.relkind,c.relstorage;

# 스키마별  사이즈
select schemaname ,round(sum(pg_total_relation_size(schemaname||'.'||tablename))/1024/1024) "schema_size_MB" from pg_tables WHERE schemaname NOT in('gp_toolkit','pg_catalog','gpmetrics','dba','information_schema','gpcc_schema','gpexpand')  group by 1;

# 테이블 사이즈 (gpperfmon)
SELECT SCHEMA,table_name,relkind,relstorage,SIZE/1024/1024 AS size_mb FROM gpmetrics.gpcc_size_ext_table
WHERE SCHEMA NOT in('gp_toolkit','pg_catalog','gpmetrics','dba','information_schema','gpcc_schema','gpexpand')
ORDER BY 1,2,3,4,5 DESC;

#엑세스가 빈번한 테이블
SELECT
    schemaname AS schema_name,
    relname AS table_name,
    seq_scan,
    idx_scan
FROM
    pg_stat_all_tables
WHERE schemaname NOT in ('information_schema','pg_toast','pg_catalog','gp_toolkit','gpmetrics','gpcc_schema')
ORDER BY
    3 DESC LIMIT 100;

# 디스크 사용량 1시간 단위 (gpperfmon)
SELECT to_timestamp(floor((extract('epoch' from ctime) / 3600 )) * 3600) AT TIME ZONE 'Asia/Seoul' as interval_alias,
hostname,
filesystem,
round (MIN(bytes_used) / AVG(total_bytes) * 100 ,2) AS min_disk_usage_per,
round (AVG(bytes_used) / AVG(total_bytes) * 100 ,2) AS avg_disk_usage_per,
round (MAX(bytes_used) / AVG(total_bytes) * 100 ,2)  AS max_disk_usage_per
FROM  gpmetrics.gpcc_disk_history
GROUP BY interval_alias, hostname, filesystem
ORDER BY 1,2,3;

#에러 메세지 종류 & 건수 (gpperfmon)
SELECT logmessage, count(*) FROM gpmetrics.gpcc_pg_log_history GROUP BY 1 ORDER BY 2 DESC LIMIT 30;

#최근 많이 수행한 쿼리 (gpperfmon)
SELECT query_text,count(*) FROM gpmetrics.gpcc_queries_history WHERE ctime >= CURRENT_DATE - INTERVAL '7 days'
  AND ctime < CURRENT_DATE GROUP BY query_text ORDER BY 2 DESC LIMIT 50;

#수행 시간 기준 (gpperfmon)
SELECT db,username,query_text,avg (tfinish-tstart),max (tfinish-tstart) FROM gpmetrics.gpcc_queries_history WHERE ctime >= CURRENT_DATE - INTERVAL '7 days'
and db not in('gpperfmon','template1')
GROUP BY db,username,query_text ORDER BY 4 DESC LIMIT 100;

#파티션 테이블 
select schemaname,tablename,partitiontype,count(partitiontablename) as total_no_of_partitions from pg_partitions group by tablename, schemaname,partitiontype ORDER BY 1,2;

#시스템 리소스 15초

COPY(
SELECT *
FROM gpmetrics.gpcc_system_history 
where ctime >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY 1) TO '/home/gpadmin/sys_all_15s.csv' WITH CSV HEADER ;

#시스템 리소스 1분

COPY(
SELECT to_timestamp(floor((extract('epoch' from ctime) / 60 )) * 60) AT TIME ZONE 'Asia/Seoul' as interval_alias,
-- hostname,
round(avg(cpu_user)) AS max_cpu_user,
round(avg(cpu_sys)) AS max_cpu_sys,
round(avg(round(100 - cpu_idle))) AS avg_total_cpu_usage,
max(round(100 - cpu_idle)) AS max_total_cpu_usage,
round(avg(mem_used/1024/1024)) AS avg_mem_used_mb,
max(mem_used/1024/1024) AS max_mem_used_mb,
max(mem_total/1024/1024) AS mem_total_mb,
round(avg(swap_used/1024/1024)) AS avg_swap_used_mb,
max(swap_used/1024/1024) AS max_swap_used_mb,
max(swap_total/1024/1024) AS swap_total_mb,
round(avg(disk_rb_rate/1024/1024)) AS avg_disk_read_mb,
max(disk_rb_rate/1024/1024) AS max_disk_read_mb,
round(avg(disk_wb_rate/1024/1024)) AS avg_disk_write_mb,
max(disk_wb_rate/1024/1024) AS max_disk_write_mb,
round(avg(net_rb_rate/1024/1024)) AS avg_nw_read_mb,
max(net_rb_rate/1024/1024) AS max_nw_read_mb,
round(avg(net_wb_rate/1024/1024)) AS avg_nw_write_mb,
max(net_wb_rate/1024/1024) AS max_nw_write_mb
FROM gpmetrics.gpcc_system_history 
WHERE hostname NOT IN ('mdw','smdw')
AND ctime >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY 1
ORDER BY 1) TO '/home/gpadmin/sys_seg_1M.csv' WITH CSV HEADER ;

#시스템 리소스 10분

COPY(
SELECT to_timestamp(floor((extract('epoch' from ctime) / 600 )) * 600) AT TIME ZONE 'Asia/Seoul' as interval_alias,
-- hostname,
round(avg(cpu_user)) AS max_cpu_user,
round(avg(cpu_sys)) AS max_cpu_sys,
round(avg(round(100 - cpu_idle))) AS avg_total_cpu_usage,
max(round(100 - cpu_idle)) AS max_total_cpu_usage,
round(avg(mem_used/1024/1024)) AS avg_mem_used_mb,
max(mem_used/1024/1024) AS max_mem_used_mb,
max(mem_total/1024/1024) AS mem_total_mb,
round(avg(swap_used/1024/1024)) AS avg_swap_used_mb,
max(swap_used/1024/1024) AS max_swap_used_mb,
max(swap_total/1024/1024) AS swap_total_mb,
round(avg(disk_rb_rate/1024/1024)) AS avg_disk_read_mb,
max(disk_rb_rate/1024/1024) AS max_disk_read_mb,
round(avg(disk_wb_rate/1024/1024)) AS avg_disk_write_mb,
max(disk_wb_rate/1024/1024) AS max_disk_write_mb,
round(avg(net_rb_rate/1024/1024)) AS avg_nw_read_mb,
max(net_rb_rate/1024/1024) AS max_nw_read_mb,
round(avg(net_wb_rate/1024/1024)) AS avg_nw_write_mb,
max(net_wb_rate/1024/1024) AS max_nw_write_mb
FROM gpmetrics.gpcc_system_history 
WHERE hostname NOT IN ('mdw','smdw')
AND ctime >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY 1
ORDER BY 1) TO '/home/gpadmin/sys_seg_10M.csv' WITH CSV HEADER ;

#리소스 그룹 맵핑
SELECT rolname, rsgname FROM pg_roles, pg_resgroup  WHERE pg_roles.rolresgroup=pg_resgroup.oid;

#리소스 그룹#15초
COPY(SELECT *
FROM gpmetrics.gpcc_resgroup_history
WHERE ctime >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY 1,2) TO '/home/gpadmin/rsg_all_15s.csv' WITH CSV HEADER ;

#리소스 그룹#1분

COPY(
SELECT to_timestamp(floor((extract('epoch' from ctime) / 60 )) * 60) AT TIME ZONE 'Asia/Seoul' as interval_alias,
rsgname, 
avg(cpu_usage_percent) AS cpu_avg_per,
max(cpu_usage_percent) AS cpu_max_per,
max(concurrency_limit) AS concurrency_limit,
avg(num_queueing) AS avg_num_queue,
max(num_queueing) AS max_num_queue,
avg(mem_used_mb) AS avg_used_mb,
max(mem_used_mb) AS max_used_mb
FROM gpmetrics.gpcc_resgroup_history
WHERE ctime >= CURRENT_DATE - INTERVAL '7 days'
and segid != '-1'
GROUP BY 1,2
ORDER BY 1,2 ) TO '/home/gpadmin/rsg_seg_1M.csv' WITH CSV HEADER ;

#리소스 그룹#10분

SELECT to_timestamp(floor((extract('epoch' from ctime) / 600 )) * 600) AT TIME ZONE 'Asia/Seoul' as interval_alias,
rsgname, 
avg(cpu_usage_percent) AS cpu_avg_per,
max(cpu_usage_percent) AS cpu_max_per,
max(concurrency_limit) AS concurrency_limit,
avg(num_queueing) AS avg_num_queue,
max(num_queueing) AS max_num_queue,
avg(mem_used_mb) AS avg_used_mb,
max(mem_used_mb) AS max_used_mb
FROM gpmetrics.gpcc_resgroup_history
WHERE ctime >= CURRENT_DATE - INTERVAL '7 days'
and segid != '-1'
GROUP BY 1,2
ORDER BY 1,2) TO '/home/gpadmin/rsg_seg_10M.csv' WITH CSV HEADER ;

#리소스 큐
select rsqname, rsqcountlimit cntlimit, rsqcountvalue cntval, rsqcostlimit costlimit, rsqcostvalue costval, rsqmemorylimit memlimit, rsqmemoryvalue memval, rsqwaiters waiters, rsqholders holders from gp_toolkit.gp_resqueue_status;

#need_analyze
psql -c "SELECT relname from pg_class where reltuples=0 and relpages=0 and relkind='r' and relname not like 't%' and relname not like 'err%';"

#replication (mirror)
SELECT gp_segment_id,client_addr,client_port,backend_start,state,sync_state,sync_error FROM pg_catalog.gp_stat_replication ORDER BY 1;

#bloat
psql -c "select bdinspname schema_nm, bdirelname tb_nm, bdirelpages*32.0/1024.0 real_size_mb, bdiexppages*32.0/1024.0 exp_size_mb from gp_toolkit.gp_bloat_diag where bdirelpages*32.0/1024.0 > 100;"

#skew function 생성 후 진행


DROP FUNCTION IF EXISTS public.greenplum_check_skew();
CREATE FUNCTION public.greenplum_check_skew()
    RETURNS TABLE (
    	relation text,
    	vtotal_size_gb numeric,
    	vseg_min_size_gb numeric,
    	vseg_max_size_gb numeric,
    	vseg_avg_size_gb numeric,
    	vseg_gap_min_max_percent numeric,
    	vseg_gap_min_max_gb numeric,
    	vnb_empty_seg bigint)
    LANGUAGE plpgsql
    AS $$
DECLARE
	v_function_name text := 'greenplum_check_skew';
    v_location int;
    v_sql text;
    v_db_oid text;
BEGIN

    v_location := 1000;
    SET client_min_messages TO WARNING;

    -- Get the database oid
    v_location := 2000;
    SELECT d.oid INTO v_db_oid
    FROM pg_database d
    WHERE datname = current_database();

    -- Drop the temp table if it exists
    v_location := 3000;
    v_sql := 'DROP TABLE IF EXISTS public.greenplum_get_refilenodes CASCADE';
    v_location := 3100;
    EXECUTE v_sql;

    -- Temp table to temporary store the relfile records
    v_location := 4000;
    v_sql := 'CREATE TABLE public.greenplum_get_refilenodes ('
    '    segment_id int,'
    '    o oid,'
    '    relname name,'
    '    relnamespace oid,'
    '    relkind char,'
    '    relfilenode bigint'
    ')';
    v_location := 4100;
    EXECUTE v_sql;

    -- Store all the data related to the relfilenodes from all
    -- the segments into the temp table
    v_location := 5000;
    v_sql := 'INSERT INTO public.greenplum_get_refilenodes SELECT '
	'  s.gp_segment_id segment_id, '
	'  s.oid o, '
	'  s.relname, '
	'  s.relnamespace,'
	'  s.relkind,'
	'  s.relfilenode '
	'FROM '
	'  gp_dist_random(''pg_class'') s ' -- all segment
	'UNION '
	'  SELECT '
	'  m.gp_segment_id segment_id, '
	'  m.oid o, '
	'  m.relname, '
	'  m.relnamespace,'
	'  m.relkind,'
	'  m.relfilenode '
	'FROM '
	'  pg_class m ';  -- relfiles from master
	v_location := 5100;
    EXECUTE v_sql;

	-- Drop the external table if it exists
    v_location := 6000;
    v_sql := 'DROP EXTERNAL WEB TABLE IF EXISTS public.greenplum_get_db_file_ext';
    v_location := 6100;
    EXECUTE v_sql;

	-- Create a external that runs a shell script to extract all the files 
	-- on the base directory
	v_location := 7000;
    v_sql := 'CREATE EXTERNAL WEB TABLE public.greenplum_get_db_file_ext ' ||
            '(segment_id int, relfilenode text, filename text, ' ||
            'size numeric) ' ||
            'execute E''ls -l $GP_SEG_DATADIR/base/' || v_db_oid ||
            ' | ' ||
            'grep gpadmin | ' ||
            E'awk {''''print ENVIRON["GP_SEGMENT_ID"] "\\t" $9 "\\t" ' ||
            'ENVIRON["GP_SEG_DATADIR"] "/base/' || v_db_oid ||
            E'/" $9 "\\t" $5''''}'' on all ' || 'format ''text''';

    v_location := 7100;
    EXECUTE v_sql;


    -- Drop the datafile statistics view if exists
	v_location := 8000;
	v_sql := 'DROP VIEW IF EXISTS public.greenplum_get_file_statistics';
	v_location := 8100;
    EXECUTE v_sql;

    -- Create a view to get all the datafile statistics
    v_location := 9000;
	v_sql :='CREATE VIEW public.greenplum_get_file_statistics AS '
			'SELECT '
			'  n.nspname || ''.'' || c.relname relation, '
			'  osf.segment_id, '
			'  split_part(osf.relfilenode, ''.'' :: text, 1) relfilenode, '
			'  c.relkind, '
			'  sum(osf.size) size '
			'FROM '
			'  public.greenplum_get_db_file_ext osf '
			'  JOIN public.greenplum_get_refilenodes c ON ('
			'    c.segment_id = osf.segment_id '
			'    AND split_part(osf.relfilenode, ''.'' :: text, 1) = c.relfilenode :: text'
			'  ) '
			'  JOIN pg_namespace n ON c.relnamespace = n.oid '
			'WHERE '
			'  osf.relfilenode ~ ''(\d+(?:\.\d+)?)'' '
			-- '  AND c.relkind = ''r'' :: char '
			'  AND n.nspname not in ('
			'    ''pg_catalog'', '
			'    ''information_schema'', '
			'    ''gp_toolkit'' '
			'  ) '
			'  AND not n.nspname like ''pg_temp%'' '
			'  GROUP BY 1,2,3,4';
	v_location := 9100;
    EXECUTE v_sql;

     -- Drop the skew report view view if exists
	v_location := 10000;
	v_sql := 'DROP VIEW IF EXISTS public.greenplum_get_skew_report';
	v_location := 10100;
    EXECUTE v_sql;

    -- Create a view to get all the table skew statistics
    v_location := 11100;
	v_sql :='CREATE VIEW public.greenplum_get_skew_report AS '
			'SELECT '
			'	sub.relation relation,'
			'	(sum(sub.size)/(1024^3))::numeric(15,2) AS vtotal_size_GB,'  --Size on segments
			'    (min(sub.size)/(1024^3))::numeric(15,2) AS vseg_min_size_GB,'
			'    (max(sub.size)/(1024^3))::numeric(15,2) AS vseg_max_size_GB,'
			'    (avg(sub.size)/(1024^3))::numeric(15,2) AS vseg_avg_size_GB,' --Percentage of gap between smaller segment and bigger segment
			'    (100*(max(sub.size) - min(sub.size))/greatest(max(sub.size),1))::numeric(6,2) AS vseg_gap_min_max_percent,'
			'    ((max(sub.size) - min(sub.size))/(1024^3))::numeric(15,2) AS vseg_gap_min_max_GB,'
			'    count(sub.size) filter (where sub.size = 0) AS vnb_empty_seg '
			'FROM '
			'public.greenplum_get_file_statistics sub'
			'  GROUP BY 1';
	v_location := 11100;
    EXECUTE v_sql;

    -- Return the data back
    RETURN query (
        SELECT
            *
        FROM public.greenplum_get_skew_report a);

    -- Throw the exception whereever it encounters one
    EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$;

SELECT * FROM public.greenplum_check_skew();
SELECT * FROM public.greenplum_get_file_statistics;
SELECT * FROM public.greenplum_get_skew_report ORDER BY vseg_gap_min_max_percent DESC LIMIT 50;

SELECT sub.relation relation,
(sum(sub.size)/(1024^3))::numeric(15,2) AS vtotal_size_GB,
 (min(sub.size)/(1024^3))::numeric(15,2) AS vseg_min_size_GB,
 (max(sub.size)/(1024^3))::numeric(15,2) AS vseg_max_size_GB,
 (avg(sub.size)/(1024^3))::numeric(15,2) AS vseg_avg_size_GB, --Percentage of gap between smaller segment and bigger segment
 (100*(max(sub.size) - min(sub.size))/greatest(max(sub.size),1))::numeric(6,2) AS vseg_gap_min_max_percent,
 ((max(sub.size) - min(sub.size))/(1024^3))::numeric(15,2) AS vseg_gap_min_max_GB,
 count(sub.size) filter (where sub.size = 0) AS vnb_empty_seg 
FROM public.greenplum_get_file_statistics sub
WHERE sub.relkind = 'r'
  GROUP BY 1 ORDER BY vseg_gap_min_max_percent DESC;

#mirror
SELECT gp_segment_id,client_addr,client_port,backend_start,state,sync_state,sync_error FROM pg_catalog.gp_stat_replication ORDER BY 1;


