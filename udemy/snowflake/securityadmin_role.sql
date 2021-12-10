create role marketing_dba;

create role marketing_analyst;

grant role marketing_analyst to role marketing_dba;

grant role marketing_dba to role SYSADMIN;

create user mkt_user_1 password = 'mktuser1' default_role = marketing_analyst must_change_password = false;
grant role marketing_analyst to user mkt_user_1;

create user mkt_dba_1 password = 'mktdba1' default_role = marketing_dba must_change_password = false;
grant role marketing_dba to user mkt_dba_1;


create role fin_dba;
create role fin_analyst;
grant role fin_analyst to role fin_dba;


create user fin_user_1 password = 'password' default_role = fin_analyst must_change_password = false;
grant role fin_analyst to user fin_user_1;

create user fin_dba_1 password = 'password' default_role = fin_dba must_change_password = false;
grant role fin_dba to user fin_dba_1;
