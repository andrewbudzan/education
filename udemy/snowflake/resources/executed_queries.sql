create or replace stage bulk_copy_example_stage url='s3://snowflake-essentials/ingesting_data/new_customer';

list @bulk_copy_example_stage;

use database ingest_data;

use warehouse COMPUTE_WH;

copy into customer
    from @bulk_copy_example_stage
    pattern='.*.csv'
    file_format = (type = csv field_delimiter = '|' skip_header = 1)

select count(*) from customer;

copy into customer
    from @bulk_copy_example_stage/2019-09-24/additional_data.txt
    file_format = (type = csv field_delimiter = '|' skip_header = 1)
