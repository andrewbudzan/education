create user jsmith
    display_name = 'john smith'
    login_name = jsmith
    password = 'password'
    --     default_role = 'defaultrole'
--     default_warehouse = ''
    must_change_password = false;


create user dnewton
    display_name = 'dave newton'
    login_name = dnewton
    password = 'password'
    default_role = SYSADMIN
    must_change_password = false;

grant role SYSADMIN to user dnewton;


create user jsmith
    display_name = 'john smith'
    login_name = jsmith
    password = 'password'
    --     default_role = 'defaultrole'
--     default_warehouse = ''
    must_change_password = false;


create user dnewton
    display_name = 'dave newton'
    login_name = dnewton
    password = 'password'
    default_role = SYSADMIN
    must_change_password = false;

grant role SYSADMIN to user dnewton;


show users;

use role SECURITYADMIN;

create role marketing_users;
grant role marketing_users to role SYSADMIN;
grant role marketing_users to user jsmith;

use role ACCOUNTADMIN;

alter user jsmith set default_role = marketing_users;


show grants;

grant ownership on database CUSTOMER_ANALYTICS to role marketing_users;
grant ownership on schema CUSTOMER_ANALYTICS.PUBLIC to role marketing_users;
