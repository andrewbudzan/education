use database OUR_FIRST_DATABASE;


drop table CUSTOMER;
select * from CUSTOMER;

undrop table customer;
select * from CUSTOMER;
select count(*) from customer;

use database PROD_CRM;


drop schema PROD_CRM.PUBLIC;
select * from customer;

undrop schema PROD_CRM.public;
select * from customer;


drop database PROD_CRM;
select * from customer;


undrop database prod_crm;
use database prod_crm;
select * from customer;
