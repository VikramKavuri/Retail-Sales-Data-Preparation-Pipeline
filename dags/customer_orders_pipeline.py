from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_conn")
S3_BUCKET = os.getenv("PIPELINE_S3_BUCKET", "snowflakedatapipeline2022")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "PRO_CURATION")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "PRO_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PRO_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "PRO_DEVELOPER_ROLE")

BATCH_ID = "{{ ts_nodash[:12] }}"


def build_s3_move_command(entity: str, source_zone: str, target_zone: str) -> str:
    return (
        f"aws s3 mv s3://{S3_BUCKET}/firehose/{entity}/{source_zone}/ "
        f"s3://{S3_BUCKET}/firehose/{entity}/{target_zone}/{BATCH_ID}/ --recursive"
    )


copy_orders_sql = f"""
copy into {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.ORDERS_RAW
(O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE,
 O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT, BATCH_ID)
from (
    select
        t.$1,
        t.$2,
        t.$3,
        t.$4,
        t.$5,
        t.$6,
        t.$7,
        t.$8,
        t.$9,
        '{BATCH_ID}'
    from @ORDERS_RAW_STAGE t
);
"""

copy_customers_sql = f"""
copy into {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.CUSTOMER_RAW
(C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE,
 C_ACCTBAL, C_MKTSEGMENT, C_COMMENT, BATCH_ID)
from (
    select
        t.$1,
        t.$2,
        t.$3,
        t.$4,
        t.$5,
        t.$6,
        t.$7,
        t.$8,
        '{BATCH_ID}'
    from @CUSTOMER_RAW_STAGE t
);
"""

transform_orders_sql = """
insert into ORDER_CUSTOMER_DATE_PRICE (
    CUSTOMER_NAME,
    ORDER_DATE,
    ORDER_TOTAL_PRICE,
    BATCH_ID
)
select
    c.C_NAME as CUSTOMER_NAME,
    o.O_ORDERDATE as ORDER_DATE,
    sum(o.O_TOTALPRICE) as ORDER_TOTAL_PRICE,
    c.BATCH_ID
from ORDERS_RAW o
join CUSTOMER_RAW c
    on o.O_CUSTKEY = c.C_CUSTKEY
   and o.BATCH_ID = c.BATCH_ID
where o.O_ORDERSTATUS = 'F'
group by c.C_NAME, o.O_ORDERDATE, c.BATCH_ID
order by o.O_ORDERDATE;
"""


with DAG(
    dag_id="customer_orders_datapipeline_dynamic_batch_id",
    description="Move raw customer and order files through S3 zones and load them into Snowflake.",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "vikram-kavuri",
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["aws", "snowflake", "streaming", "etl"],
) as dag:
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    move_customers_to_processing = BashOperator(
        task_id="move_customers_to_processing",
        bash_command=build_s3_move_command("customers", "landing", "processing"),
    )

    move_orders_to_processing = BashOperator(
        task_id="move_orders_to_processing",
        bash_command=build_s3_move_command("orders", "landing", "processing"),
    )

    load_customers_raw = SnowflakeOperator(
        task_id="load_customers_raw",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=copy_customers_sql,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    load_orders_raw = SnowflakeOperator(
        task_id="load_orders_raw",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=copy_orders_sql,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    move_customers_to_processed = BashOperator(
        task_id="move_customers_to_processed",
        bash_command=build_s3_move_command("customers", "processing", "processed"),
    )

    move_orders_to_processed = BashOperator(
        task_id="move_orders_to_processed",
        bash_command=build_s3_move_command("orders", "processing", "processed"),
    )

    transform_customer_orders = SnowflakeOperator(
        task_id="transform_customer_orders",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=transform_orders_sql,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    start >> [move_customers_to_processing, move_orders_to_processing]
    move_customers_to_processing >> load_customers_raw >> move_customers_to_processed
    move_orders_to_processing >> load_orders_raw >> move_orders_to_processed
    [move_customers_to_processed, move_orders_to_processed] >> transform_customer_orders >> finish
