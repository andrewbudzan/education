create warehouse common_wh with
    warehouse_size = 'SMALL'
    auto_suspend = 300
    auto_resume = true
    warehouse_type = 'STANDARD';

grant usage on warehouse common_wh to role PUBLIC;

create database temp_database;
grant usage on database temp_database to role PUBLIC;


create database mkt_db;
grant ownership on schema mkt_db.public to role MARKETING_DBA;
grant ownership on database mkt_db to role MARKETING_DBA;

create database fin_db;
grant ownership on schema fin_db.public to role FIN_DBA;
grant ownership on database fin_db to role FIN_DBA;
