@echo off
set SNOWSQL_PWD=your_password
@echo on
snowsql -a account-1 -u user -d TEST_SCALE_OUT -s PUBLIC -r ACCOUNTADMIN -w AS_WH -f complex_query.sql
