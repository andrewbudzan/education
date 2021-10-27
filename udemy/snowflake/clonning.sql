use database PROD_CRM;

select * from CUSTOMER;
select count(*) from CUSTOMER;

create table customer_copy clone CUSTOMER;


select * from CUSTOMER_COPY;
select count(*) from CUSTOMER_COPY;


update customer_copy
set JOB = 'snowflake dev'
where NAME = 'Jeffrey Garrett';

update customer
set AGE = 35
where NAME = 'Jeffrey Garrett';

-- original and copied tables shares most of their data except data, which was modified
select * from customer_copy where NAME = 'Jeffrey Garrett'
union
select * from customer where NAME = 'Jeffrey Garrett';

-- not only tables but databases or schemas can be cloned

drop table customer_copy;

create schema public_copy clone PUBLIC;
