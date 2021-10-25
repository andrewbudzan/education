create database test_scale_out;

CREATE TABLE customer (
                          Customer_ID string,
                          Customer_Name string ,
                          Customer_Email string ,
                          Customer_City string ,
                          Customer_State string ,
                          Customer_DOB DATE
);


create or replace stage bulk_copy_example_stage url='s3://snowflake-essentials/ingesting_data/new_customer';

-- list the files in the bucket
list @bulk_copy_example_stage;


copy into customer
    from @bulk_copy_example_stage
    pattern='.*.csv'
    file_format = (type = csv field_delimiter = '|' skip_header = 1);


select *
from customer;


CREATE TABLE large_table (
                             Customer_ID string,
                             Customer_Name string ,
                             Customer_Email string ,
                             Customer_City string ,
                             Customer_State string ,
                             Customer_DOB DATE
);


INSERT INTO large_table
SELECT
    RANDOM() AS Customer_ID,
    UUID_STRING() AS Customer_Name,
    UUID_STRING() AS Customer_Email,
    UUID_STRING() AS Customer_City,
    UUID_STRING() AS Customer_State,
    A.Customer_DOB
FROM CUSTOMER A CROSS JOIN CUSTOMER B CROSS JOIN CUSTOMER C CROSS JOIN (SELECT TOP 20 * FROM CUSTOMER) D;

select count(*) from large_table;





create warehouse as_wh
    with warehouse_size = 'xsmall'
    min_cluster_count = 1
    max_cluster_count = 3
    auto_suspend = 300
    auto_resume = true
    comment = 'autoscaled warehouse'
    scaling_policy = 'standard';
