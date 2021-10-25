-- grant usage on database ingest_data to role public;
-- grant usage on schema ingest_data.PUBLIC to role public;
-- grant select on table ingest_data.PUBLIC.TRANSACTIONS to role public;

use role ACCOUNTADMIN;

-- create 2 wh - for data scientists (DS_WH) team and database administrators team (DBA_WH)
-- DS_WH
create warehouse DS_WH with warehouse_size = 'small' warehouse_type = 'standard' auto_suspend = 300 auto_resume = true;

-- DBA_WH
create warehouse DBA_WH with warehouse_size = 'xsmall' warehouse_type = 'standard' auto_suspend = 300 auto_resume = true;

-- create roles for groups
create role data_scientists;
grant usage on warehouse DS_WH to role data_scientists;

create role dbas;
grant usage on warehouse DBA_WH to role dbas;


-- create users
create user ds1 password = 'ds1' login_name = ds1 default_role = data_scientists default_warehouse = DS_WH must_change_password = false;
create user dba1 password = 'dba1' login_name = db1 default_role = dbas default_warehouse =DBA_WH must_change_password = false;

-- grant roles for created users
grant role data_scientists to user ds1;
grant role dbas to user dba1;
