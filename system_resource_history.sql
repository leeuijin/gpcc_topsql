select substr(ctime, 1, 10) ctime
          --,hostname
      , max(mem_total_mb)*40 AS max_mem_total_mb -- 메모리 총량 * 40vm : VM환경에서 물리메모리 보다 작게 표시될 수 있습니다. 물리4G-> 조회 3.5G
      , round(max(mem_actual_used),0)* 40 max_mem_total_used_mb -- 최고 메모리 사용량 최대 * 40vm
      , round(avg(mem_actual_used),0)* 40 avg_mem_total_used_mb -- 평 메모리 사용량 최대 * 40vm
      , round((round(max(mem_actual_used),0)* 40) / (max(mem_total_mb)* 40) *100) AS "max_usage_per%" -- 최고 메모리 사용량(%) / 메모리 총량 * 100
      , max(cpu            )::int "max_cpu%"
      , avg(cpu            )::int "avg_cpu%"
      , max(cpu_user       )::int "max_cpu_user%"      
      , max(cpu_sys        )::int "max_cpu_sys%"           
from (
      SELECT substring(ctime::text,1,20) ctime
           , hostname
           , round(max(mem_total/1024/1024)      ,0) mem_total_m
           , round(max(mem_used/1024/1024)       ,0) mem_used
           , round(max(mem_actual_used/1024/1024),0) mem_actual_use
           , round(max(mem_actual_free/1024/1024),0) mem_actual_free
           , round(max(100-cpu_idle))                  cpu
           , round(max(cpu_user))                  cpu_use
           , round(max(cpu_sys)) cpu_sys
           FROM gpmetrics.gpcc_system_history
           where ctime >= '2023-10-01 00:00:00'::timestamp
           and hostname not in ('mdw', 'smdw') 
           group by 1,2--,2
           --ORDER BY 1
) a         
group by 1--, 2
order by 1--, 2

select substr(ctime, 1, 10) ctime
          --,hostname
      , max(mem_used)*40 AS max_mem_total_mb -- 메모리 총량 * 40vm : VM환경에서 물리메모리 보다 작게 표시될 수 있습니다. 물리4G-> 조회 3.5G
      , round(max(mem_used),0)* 40 max_mem_total_used_mb -- 최고 메모리 사용량 최대 * 40vm
      , round(avg(mem_used),0)* 40 avg_mem_total_used_mb -- 평 메모리 사용량 최대 * 40vm
      , round((round(max(mem_used),0)* 40) / (max(mem_total_m)* 40) *100) AS "max_usage_per%" -- 최고 메모리 사용량(%) / 메모리 총량 * 100
      , max(cpu            )::int "max_cpu%"
      , avg(cpu            )::int "avg_cpu%"
      , max(cpu_user       )::int "max_cpu_user%"      
      , max(cpu_sys        )::int "max_cpu_sys%"           
from (
      SELECT substring(ctime::text,1,20) ctime
           , hostname
           , round(max(mem_total/1024/1024)      ,0) mem_total_m
           , round(max(mem_used/1024/1024)       ,0) mem_used
           , round(max(mem_used/1024/1024),0) mem_actual_use
           , round(max(mem_used/1024/1024),0) mem_actual_free
           , round(max(100-cpu_idle))                  cpu
           , round(max(cpu_user))                  cpu_user
           , round(max(cpu_sys)) cpu_sys
           FROM gpmetrics.gpcc_system_history
           where ctime >= '2023-10-01 00:00:00'::timestamp
           and hostname not in ('mdw', 'smdw') 
           group by 1,2--,2
           --ORDER BY 1
) a         
group by 1--, 2
order by 1--, 2
