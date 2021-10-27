alter session set timezone = 'UTC';


create database prod_crm;

create table customer (
                          name string,
                          email string,
                          job string,
                          phone string,
                          age number
);


create or replace stage tts url='s3://abdzn-snowflake-time-travel'

list @tts


copy into customer
    from @tts
    pattern = '.*.csv'
    file_format = (type = csv field_delimiter = '|' skip_header = 1);


select * from customer;


select current_timestamp;

-- current timestamp
-- 2021-10-26 19:59:39.627000000 +00:00


select * from customer;

update customer set Job = 'snowflake dev'


select * from customer;

-- time travel to specific timestamp
select * from customer before(timestamp => '2021-10-26 19:59:39.627000000'::timestamp);

-- time travel 10 minutes back from now
select * from customer at(offset => -60*10);


update customer set job = NULL;


select * from customer;

-- time travel to the time before query with some id was executed
select * from customer before (statement => '019fdfd3-3200-acbd-0000-e0310001ca0a')
