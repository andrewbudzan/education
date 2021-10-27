-- all databases
select a.table_catalog,
       sum(a.storage_used_gb) as storage_used,
       sum(a.time_travel_storage_used_gb) as storage_used_by_time_travel,
       sum(a.failsafe_storage_used_gb) as storage_used_by_failsafe
from (
         select table_catalog,
                round(active_bytes / (1024 * 1024 * 1024), 2)      as storage_used_gb,
                round(time_travel_bytes / (1024 * 1024 * 1024), 2) as time_travel_storage_used_gb,
                round(failsafe_bytes / (1024 * 1024 * 1024), 2)    as failsafe_storage_used_gb
         from SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
         order by storage_used_gb DESC, time_travel_storage_used_gb desc, failsafe_storage_used_gb desc
     ) a
group by TABLE_CATALOG
order by storage_used desc;


-- not deleted databases
-- select a.table_catalog,
--        sum(a.storage_used_gb) as storage_used,
--        sum(a.time_travel_storage_used_gb) as storage_used_by_time_travel,
--        sum(a.failsafe_storage_used_gb) as storage_used_by_failsafe
-- from (
--          select table_catalog,
--                 table_schema,
--                 table_name,
--                 round(active_bytes / (1024 * 1024 * 1024), 5)      as storage_used_gb,
--                 round(time_travel_bytes / (1024 * 1024 * 1024), 5) as time_travel_storage_used_gb,
--                 round(failsafe_bytes / (1024 * 1024 * 1024), 5)    as failsafe_storage_used_gb
--          from SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
--          where CATALOG_DROPPED is null
--          order by storage_used_gb DESC, time_travel_storage_used_gb desc, failsafe_storage_used_gb desc
--      ) a
-- group by TABLE_CATALOG
-- order by storage_used desc;
