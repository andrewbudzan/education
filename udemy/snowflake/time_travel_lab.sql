create database time_travel_1;

create table item as

select * from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.ITEM
limit 10000;


select * from item limit 100;

select current_timestamp;
-- 2021-10-26 21:45:59.591000000 +00:00

update item
set I_CURRENT_PRICE = 0;

select distinct I_CURRENT_PRICE from item;


select distinct count(I_CURRENT_PRICE) from item at (timestamp => '2021-10-26 21:45:59.591000000'::timestamp_tz);


select * from item at (timestamp => '2021-10-26 21:45:59.591000000'::timestamp_tz);
