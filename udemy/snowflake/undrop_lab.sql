create database prod;
create schema crm;
use schema crm;

create table customer as
select * from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.CUSTOMER limit 1000;

use schema PUBLIC;
create table date_dim as
select * from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM;


use schema crm;
drop table customer;
select * from customer;

undrop table customer;
select * from customer;
