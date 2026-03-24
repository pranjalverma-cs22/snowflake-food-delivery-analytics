
-- use sysadmin role.
use role sysadmin;

-- create a warehouse if not exist 
create warehouse if not exists adhoc_wh
     comment = 'This is the adhoc-wh'
     warehouse_size = 'x-small' 
     auto_resume = true 
     auto_suspend = 60 
     enable_query_acceleration = false 
     warehouse_type = 'standard' 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     initially_suspended = true;



-- create development database/schema  if does not exist
create database if not exists sandbox;
use database sandbox;
create schema if not exists stage_sch;
create schema if not exists clean_sch;
create schema if not exists consumption_sch;
create schema if not exists common;

use schema stage_sch;



 -- create file format to process the CSV file
  create file format if not exists stage_sch.csv_file_format 
        type = 'csv' 
        compression = 'auto' 
        field_delimiter = ',' 
        record_delimiter = '\n' 
        skip_header = 1 
        field_optionally_enclosed_by = '\042' 
        null_if = ('\\N');

create  stage if not exists stage_sch.csv_stg
    directory = ( enable = true )
    comment = 'this is the snowflake internal stage';


create or replace tag 
    common.pii_policy_tag 
    allowed_values 'PII','PRICE','SENSITIVE','EMAIL'
    comment = 'This is PII policy tag object';

CREATE OR REPLACE MASKING POLICY common.pii_masking_policy
AS (pii_text STRING)
RETURNS STRING ->
CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN', 'DATA_ENGINEER')
        THEN pii_text
    ELSE '** PII MASKED **'
END;

CREATE OR REPLACE MASKING POLICY common.email_masking_policy
AS (email_text STRING)
RETURNS STRING ->
CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN', 'DATA_ENGINEER')
        THEN email_text
    ELSE '** EMAIL MASKED **'
END;



CREATE OR REPLACE MASKING POLICY common.phone_masking_policy
AS (phone STRING)
RETURNS STRING ->
CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN', 'DATA_ENGINEER')
        THEN phone
    ELSE '** PHONE MASKED **'
END;

    List @stage_sch.csv_stg
    