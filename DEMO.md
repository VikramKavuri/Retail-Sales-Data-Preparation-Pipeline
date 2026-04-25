# Local Demo Guide

This demo provides a local way to walk through the same batch lifecycle defined in the Airflow DAG and Snowflake SQL, without requiring cloud credentials.

## What The Demo Proves

The demo mirrors the same high-level flow as the Airflow DAG:

1. source files appear in a `landing` zone
2. files move to a `processing` zone with a batch identifier
3. raw customer and order records load into warehouse-style tables
4. a joined analytics table is created
5. files move to a `processed` zone

It does this locally with:

- sample CSV inputs in `demo/sample_data/`
- filesystem folders that imitate the S3 zone layout
- SQLite tables that imitate the Snowflake raw and final tables

## Run It

From the repository root:

```bash
python3 demo/local_demo.py
```

## Expected Output

The script prints:

- the generated batch ID
- number of customer rows loaded
- number of order rows loaded
- final rows written to `ORDER_CUSTOMER_DATE_PRICE`

It also creates:

- `demo_artifacts/pipeline_demo.db`
- `demo_artifacts/order_customer_date_price.csv`
- `demo_artifacts/firehose/...` with `landing`, `processing`, and `processed` folders

## How To Inspect It

After the script finishes:

1. open `demo_artifacts/order_customer_date_price.csv` to inspect the final output
2. inspect `demo_artifacts/firehose/` to see the file movement across zones
3. inspect `demo_artifacts/pipeline_demo.db` if you want to review the local raw and final tables

## What This Demo Does Not Cover

This is a local approximation of the pipeline shape and transform logic. It does not replace:

- a real Airflow deployment
- a real S3 bucket
- a real Snowflake environment
- production monitoring or alerting

## Useful Follow-Up Improvements

Reasonable follow-up improvements for the local demo would be:

- sample Airflow DAG run screenshots
- Snowflake query screenshots
- a small test suite that validates the transform output
- optional Docker Compose support for local Airflow
