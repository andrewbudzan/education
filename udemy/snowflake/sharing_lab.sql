create database bank clone INGEST_DATA;
show tables;

drop table CUSTOMERS_CTAS;
drop table JSON_LAB_RAW;
drop table ORGANISATIONS_CTAS;
drop table ORGANISATIONS_JSON_RAW;

show tables;

use role ACCOUNTADMIN;

-- DATA PROVIDER PART
-- create share object
create share sh_bank;
-- grant usage permissions for share in next order:
grant usage on database bank to share sh_bank;
grant usage on schema bank.public to share sh_bank;
grant select on table bank.public.organisations to share sh_bank;
grant select on table bank.public.customer to share sh_bank;
-- grant created share to a specific user account - zn81888
alter share sh_bank add account = zn81888;

show grants to share sh_bank;

show shares;

-- DATA CONSUMER PART
-- show shares;
--
-- desc share jp48897.sh_bank;
--
-- -- create db based on shared
-- create database bank_database from share jp48897.sh_bank;
--
-- select * from bank_database.public.organisations;


-- PROVIDER
select *
from ORGANISATIONS;

insert into ORGANISATIONS (org_name, state, org_code, extract_date)
values ('ShareMyData', 'ATL', 589458534, '2019-10-26');

select * from ORGANISATIONS;

-- CONSUMER - dont work
-- insert into ORGANISATIONS (org_name, state, org_code, extract_date)
-- values ('DontShareYourData', 'VIS', 1457884684, '2019-10-26');

-- PROVIDER - dont work
-- grant insert on table bank.public.organisations to share sh_bank;
--
-- show shares;
--
-- show grants to share sh_bank;

-- CONSUMER - still dont work
-- insert into ORGANISATIONS (org_name, state, org_code, extract_date)
-- values ('DontShareYourData', 'VIS', 1457884684, '2019-10-26');
