-- create accountadmin user
create or replace user jake password = 'password' default_role = ACCOUNTADMIN must_change_password = false;
grant role ACCOUNTADMIN to user jake;

-- create securityadmin user
-- can create roles and users
-- can grant or revoke privileges
-- can assign privileges to roles
-- modify and monitor sessions
create or replace user john password = 'password' default_role = SECURITYADMIN must_change_password = false;
grant role SECURITYADMIN to user john;

-- create systemadmin user
create or replace user jane password = 'password' default_role = SYSADMIN must_change_password = false;
grant role SYSADMIN to user jane;
