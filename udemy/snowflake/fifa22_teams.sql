create or replace database fifa_test_db;

create table teams (
                       teamid number,
                       name string,
                       league string,
                       leagueid number,
                       overall number,
                       attack number,
                       midfield number,
                       defence number,
                       transfer_budget number,
                       domestic_prestige number,
                       int_prestige number,
                       players number,
                       starting_average_age number
                           all_team_average_age number
);

create or replace stage fifateams_stage url='s3://abdzn-test-bucket';

list @fifateams_stage;

use warehouse COMPUTE_WH;

copy into teams
    from @fifateams_stage
    pattern='.*.csv'
    file_format = (type = csv field_delimiter = ',' skip_header = 1)

select * from teams limit 30;

select * from teams where leagueid = 13;

select * from teams where league != 'International' order by overall desc limit 10;

select league, sum(transfer_budget) as transf from teams group by league order by transf desc;
