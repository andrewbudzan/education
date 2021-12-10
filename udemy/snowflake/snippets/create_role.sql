-- complete hierarchy - any sysadmin can manage objects created by lower roles
create role role_name;
create role role_admin_name;
grant role role_name to role role_admin_name;
grant role role_admin_name to role SYSADMIN;

-- not complete hierarchy - sysadmin cant manage objects created by lower roles
create role role_name;
create role role_admin_name;
grant role role_name to role role_admin_name;
