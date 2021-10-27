-- outbound share (account who sharing data)

create share share_customer_data;


grant usage on database crm_data to share share_customer_data;

grant usage on schema crm_data.public to share share_customer_data;

grant select on table crm_data.public.CUSTOMER to share share_customer_data;

show grants to share share_customer_data;

alter share share_customer_data add account=zn81888;

show grants of share share_customer_data;


-- inbound share (account who receive share)
show shares;

desc share JP48897.SHARE_CUSTOMER_DATA;

create database CUSTOMER_DATABASE from share JP48897.SHARE_CUSTOMER_DATA;

select * from customer_database.public.customer;
