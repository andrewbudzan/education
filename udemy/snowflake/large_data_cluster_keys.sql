create database performance_test;
use database performance_test;

CREATE or replace TABLE transactions
(

    Transaction_Date DATE,
    Customer_ID NUMBER,
    Transaction_ID NUMBER,
    Amount NUMBER
);

CREATE OR REPLACE STAGE perf_stage url='s3://abdzn-snowflake-streaming/transactions';

list @perf_stage;

copy into transactions
    from @perf_stage
    file_format = (type = csv field_delimiter = '|' skip_header = 1);


CREATE or replace TABLE transactions_100
(

    Transaction_Date DATE,
    Customer_ID NUMBER,
    Transaction_ID NUMBER,
    Amount NUMBER
);

use warehouse as_wh;

select * from transactions;

select * from transactions_large;

insert into transactions_100
select * from transactions limit 100;

insert into transactions_large
select a.transaction_date + mod(random(), 1000), random(), a.customer_id, a.amount
from transactions_100 a
         cross join transactions_100 b
         cross join transactions_100 c
         cross join transactions_100 d;


select count(*) from transactions_large where Transaction_Date = '2018-12-18';


CREATE or replace TABLE transactions_100
(

    Transaction_Date DATE,
    Customer_ID NUMBER,
    Transaction_ID NUMBER,
    Amount NUMBER
) cluster by (Transaction_Date)
;


insert into t
