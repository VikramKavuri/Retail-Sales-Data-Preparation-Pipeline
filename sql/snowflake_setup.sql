create database if not exists PRO_DB;
create schema if not exists PRO_DB.PRO_SCHEMA;

use database PRO_DB;
use schema PRO_SCHEMA;

create or replace file format CSV_FORMAT
    type = csv
    skip_header = 0
    field_optionally_enclosed_by = '"';

create or replace storage integration S3_INTEGRATION_PRO
    type = external_stage
    storage_provider = s3
    storage_aws_role_arn = 'arn:aws:iam::490101006133:role/Snowflake_Access_Role'
    enabled = true
    storage_allowed_locations = ('s3://snowflakedatapipeline2022/firehose/');

desc integration S3_INTEGRATION_PRO;

create or replace stage CUSTOMER_RAW_STAGE
    url = 's3://snowflakedatapipeline2022/firehose/customers/'
    storage_integration = S3_INTEGRATION_PRO
    file_format = CSV_FORMAT;

create or replace stage ORDERS_RAW_STAGE
    url = 's3://snowflakedatapipeline2022/firehose/orders/'
    storage_integration = S3_INTEGRATION_PRO
    file_format = CSV_FORMAT;

create or replace table CUSTOMER_RAW (
    C_CUSTKEY number,
    C_NAME varchar,
    C_ADDRESS varchar,
    C_NATIONKEY number,
    C_PHONE varchar,
    C_ACCTBAL number,
    C_MKTSEGMENT varchar,
    C_COMMENT varchar,
    BATCH_ID varchar
);

create or replace table ORDERS_RAW (
    O_ORDERKEY number,
    O_CUSTKEY number,
    O_ORDERSTATUS varchar,
    O_TOTALPRICE number,
    O_ORDERDATE date,
    O_ORDERPRIORITY varchar,
    O_CLERK varchar,
    O_SHIPPRIORITY number,
    O_COMMENT varchar,
    BATCH_ID varchar
);

create or replace table ORDER_CUSTOMER_DATE_PRICE (
    CUSTOMER_NAME varchar(25),
    ORDER_DATE date,
    ORDER_TOTAL_PRICE number(12, 2),
    BATCH_ID varchar
);

-- Optional validation queries
select * from ORDERS_RAW;
select * from CUSTOMER_RAW;
select * from ORDER_CUSTOMER_DATE_PRICE;
