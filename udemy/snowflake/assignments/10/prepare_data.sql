create or replace database party;


create or replace table supplier
(
    supp_id number,
    supp_name string
);


insert into supplier
select
seq8() as supp_id,
randstr(10, random()) as supp_name
from table (generator(rowcount => 1000));


create or replace database contact;


create or replace table supp_address
(
    supp_id number,
    supp_address string
);


insert into supp_address
select
seq8() as supp_id,
randstr(50, random()) as supp_address
from table (generator(rowcount => 1000));
