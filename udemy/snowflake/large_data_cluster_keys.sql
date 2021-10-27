create database perf_test;

use database perf_test;

create table transactions
(
    transaction_date date,
    transaction_id integer,
    customer_id string,
    amount integer
);

create or replace stage perf_stage url ='s3://snowflake-essentials/streaming_data_ingest/Transactions';

list @perf_stage;

copy into transactions
    from @perf_stage
    file_format = (type = csv field_delimiter = '|' skip_header = 1);


select count(*) from transactions;


create table transactions_large
(
    transaction_date date,
    transaction_id integer,
    customer_id string,
    amount integer
);


insert into transactions_large
select a.transaction_date + mod(random(), 2000), random(), a.customer_id, a.amount
from transactions a cross join transactions b cross join transactions c cross join transactions d;


select count(*) from transactions_large;


select count(*) from transactions_large where transaction_date = '2018-12-18';


create table transactions_clustered_date
(
    transaction_date date,
    transaction_id integer,
    customer_id string,
    amount integer
)
    cluster by (transaction_date);


insert into transactions_clustered_date select * from transactions_large;


select count(*) from transactions_clustered_date where transaction_date = '2018-12-18';

select count(*) from transactions_clustered_date where transaction_date = '2018-12-19';

select count(*) from transactions_clustered_date where transaction_date between '2019-12-01' and '2019-12-31';


create table transactions_clustered_month
(
    transaction_date date,
    transaction_id integer,
    customer_id string,
    amount integer
)
    cluster by (date_trunc('MONTH', transaction_date));

insert into transactions_clustered_month select * from transactions_large;

select count(*) from transactions_clustered_month where date_trunc('MONTH', transaction_date) = '2018-11-01';
