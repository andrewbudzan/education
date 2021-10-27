select * from SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;


select id,
       table_name,
       table_schema,
       active_bytes / (1024*1024*1024) as storage_used_gb,
       time_travel_bytes / (1024*1024*1024) as time_travel_storage_used_gb,
       failsafe_bytes / (1024*1024*1024) as failsafe_storage_used_gb
from SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
order by storage_used_gb DESC, time_travel_storage_used_gb desc, failsafe_storage_used_gb desc;
