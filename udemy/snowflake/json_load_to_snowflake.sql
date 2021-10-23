use database ingest_data;

create table organisations_json_raw (
    json_data_raw variant
);

create or replace stage json_example_stage url='s3://snowflake-essentials/json_data';

list @json_example_stage;

copy into organisations_json_raw
    from @json_example_stage/example_json_file.json
    file_format = (type = json);

select * from organisations_json_raw;

select
    json_data_raw:data_set,
        json_data_raw:extract_date
from organisations_json_raw;


select
    value:name::String,
        value:state::String,
        value:org_code::String,
        json_data_raw:extract_date
from
    organisations_json_raw,
    lateral flatten( input => json_data_raw:organisations);


create or replace table organisations_ctas as
select
    VALUE:name::string as org_name,
        VALUE:state::string as state,
        VALUE:org_code::string as org_code,
        json_data_raw:extract_date as extract_date
from
    organisations_json_raw,
    lateral flatten( input => json_data_raw:organisations);

CREATE TABLE organisations (
                               org_name STRING,
                               state   STRING,
                               org_code STRING,
                               extract_date DATE
);

-- and insert the JSON data into the table
INSERT INTO organisations
SELECT
    VALUE:name::String AS org_name,
        VALUE:state::String AS state,
        VALUE:org_code::String AS org_code,
        json_data_raw:extract_date AS extract_date
FROM
    organisations_json_raw
   , lateral flatten( input => json_data_raw:organisations );

-- validate that the JSON data appears properly
SELECT * FROM organisations;
