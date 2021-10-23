create table json_lab_raw (
    json_lab_raw variant
);

create or replace stage jsonlab url='s3://snowflake-essentials-json-lab';

list @jsonlab;

copy into json_lab_raw
    from @jsonlab/sample_data.json
    file_format = (type = json);

select * from json_lab_raw;

select
    json_lab_raw:customers,
        json_lab_raw:cdc_date
from json_lab_raw;


select
    value:Customer_City::String,
        value:Customer_ID::String,
        value:Customer_Name::String,
        value:Customer_Phone::String,
        json_lab_raw:cdc_date
from
    json_lab_raw,
    lateral flatten( input => json_lab_raw:customers);


create or replace table customers_ctas as
select
    VALUE:Customer_City::string as city,
        VALUE:Customer_ID::string as customer_id,
        VALUE:Customer_Name::string as name,
        VALUE:Customer_Phone::string as phone,
        json_lab_raw:cdc_date as cdc_date
from
    json_lab_raw,
    lateral flatten( input => json_lab_raw:customers);

select count(*) from customers_ctas;

select count(*) from customers_ctas where city = 'Cornwall';
