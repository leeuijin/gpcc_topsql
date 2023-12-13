SELECT tstart AS start_time
, db AS db_name
, username AS user_name
, COST
, ROUND(EXTRACT(EPOCH FROM (tfinish - tstart))) AS running_time
, query_hash
, query_text
, status
/*
, max(cpu_segs_percent)
, max(memory)
, max(disk_read_bytes)
, max(disk_write_bytes)
, max(lock_seconds)
*/
FROM (
		SELECT ROW_NUMBER() OVER(PARTITION BY gpmetrics.gpcc_queries_history.query_hash ORDER BY gpmetrics.gpcc_queries_history.tfinish - gpmetrics.gpcc_queries_history.tstart DESC ) AS RNUM, gpmetrics.gpcc_queries_history.*
		FROM gpmetrics.gpcc_queries_history 
	 ) AS T
WHERE RNUM =1
AND db NOT IN ('gpperfmon','template1','postgres')
AND ctime >= CURRENT_DATE - INTERVAL '1 days'
AND ctime < CURRENT_DATE
-- GROUP BY 1,2,3,4,5,6,7,8
ORDER BY 5 desc;


SELECT tstart AS start_time
, db AS db_name
, username AS user_name
, COST
, ROUND(EXTRACT(EPOCH FROM (tfinish - tstart))) AS running_time
, query_hash
, query_text
, status
, max(cpu_segs_percent)
, max(memory)
, max(disk_read_bytes)
, max(disk_write_bytes)
, max(lock_seconds)
FROM (
		SELECT ROW_NUMBER() OVER(PARTITION BY gpmetrics.gpcc_queries_history.query_hash ORDER BY gpmetrics.gpcc_queries_history.tfinish - gpmetrics.gpcc_queries_history.tstart DESC ) AS RNUM, gpmetrics.gpcc_queries_history.*
		FROM gpmetrics.gpcc_queries_history 
	 ) AS T
WHERE RNUM =1
AND db NOT IN ('gpperfmon','template1','postgres')
AND ctime >= CURRENT_DATE - INTERVAL '1 days'
AND ctime < CURRENT_DATE
GROUP BY 1,2,3,4,5,6,7,8
ORDER BY 5 desc;
