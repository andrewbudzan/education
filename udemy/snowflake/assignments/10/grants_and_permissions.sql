-- create share object
create or replace share share_supp;

-- grant usage
grant usage on database view_db to share share_supp;
grant usage on schema view_db.public to share share_supp;

-- grant reference usage
grant reference_usage on database party to share share_supp;
grant reference_usage on database contact to share share_supp;

-- provide SELECT permissions
grant select on table view_db.public.supp_info to share share_supp;


-- add consumer account to share
-- alter share share_supp add  account = <account_number>;
alter share share_supp add  account = zn81888;
