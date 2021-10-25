-- create a pipe for data load

create or replace pipe transactions_pipe
    auto_ingest = true
    as copy into TRANSACTIONS from @snowpipe_copy_example_stage
        file_format = ( type = csv field_delimiter = '|' skip_header = 1)

-- run show pipes command to get notification channel - it will be used for s3 notification setup

show pipes;


-- notification channel:
-- arn:aws:sqs:eu-central-1:473381607675:sf-snowpipe-AIDAW4N56DD5Y7IAB25JX-4xh3uqcCuXlWbiPmRhLwqg

select * from TRANSACTIONS;
