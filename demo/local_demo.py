from __future__ import annotations

import csv
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SAMPLE_DATA_DIR = ROOT / "demo" / "sample_data"
ARTIFACTS_DIR = ROOT / "demo_artifacts"
SQLITE_DB = ARTIFACTS_DIR / "pipeline_demo.db"
RESULT_CSV = ARTIFACTS_DIR / "order_customer_date_price.csv"


@dataclass(frozen=True)
class DatasetPaths:
    entity: str
    source_file: Path
    landing_dir: Path
    processing_dir: Path
    processed_dir: Path


def build_dataset_paths(entity: str) -> DatasetPaths:
    base = ARTIFACTS_DIR / "firehose" / entity
    return DatasetPaths(
        entity=entity,
        source_file=SAMPLE_DATA_DIR / entity / f"{entity}.csv",
        landing_dir=base / "landing",
        processing_dir=base / "processing",
        processed_dir=base / "processed",
    )


def reset_demo_workspace() -> None:
    if ARTIFACTS_DIR.exists():
        shutil.rmtree(ARTIFACTS_DIR)
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)


def stage_source_files(dataset_paths: DatasetPaths) -> Path:
    dataset_paths.landing_dir.mkdir(parents=True, exist_ok=True)
    landing_file = dataset_paths.landing_dir / dataset_paths.source_file.name
    shutil.copy2(dataset_paths.source_file, landing_file)
    return landing_file


def move_between_zones(source_file: Path, target_dir: Path, batch_id: str) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / batch_id / source_file.name
    target_file.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source_file), target_file)
    return target_file


def create_tables(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        drop table if exists CUSTOMER_RAW;
        drop table if exists ORDERS_RAW;
        drop table if exists ORDER_CUSTOMER_DATE_PRICE;

        create table CUSTOMER_RAW (
            C_CUSTKEY integer,
            C_NAME text,
            C_ADDRESS text,
            C_NATIONKEY integer,
            C_PHONE text,
            C_ACCTBAL real,
            C_MKTSEGMENT text,
            C_COMMENT text,
            BATCH_ID text
        );

        create table ORDERS_RAW (
            O_ORDERKEY integer,
            O_CUSTKEY integer,
            O_ORDERSTATUS text,
            O_TOTALPRICE real,
            O_ORDERDATE text,
            O_ORDERPRIORITY text,
            O_CLERK text,
            O_SHIPPRIORITY integer,
            O_COMMENT text,
            BATCH_ID text
        );

        create table ORDER_CUSTOMER_DATE_PRICE (
            CUSTOMER_NAME text,
            ORDER_DATE text,
            ORDER_TOTAL_PRICE real,
            BATCH_ID text
        );
        """
    )


def load_csv_to_table(
    connection: sqlite3.Connection,
    csv_path: Path,
    table_name: str,
    expected_columns: int,
    batch_id: str,
) -> int:
    rows_loaded = 0
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if len(row) != expected_columns:
                raise ValueError(
                    f"{csv_path} contains {len(row)} columns, expected {expected_columns}"
                )
            connection.execute(
                f"insert into {table_name} values ({','.join(['?'] * (expected_columns + 1))})",
                (*row, batch_id),
            )
            rows_loaded += 1
    return rows_loaded


def transform_orders(connection: sqlite3.Connection, batch_id: str) -> None:
    connection.execute("delete from ORDER_CUSTOMER_DATE_PRICE where BATCH_ID = ?", (batch_id,))
    connection.execute(
        """
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
          and o.BATCH_ID = ?
        group by c.C_NAME, o.O_ORDERDATE, c.BATCH_ID
        order by o.O_ORDERDATE
        """,
        (batch_id,),
    )


def export_results(connection: sqlite3.Connection) -> list[tuple[str, str, float, str]]:
    rows = connection.execute(
        """
        select CUSTOMER_NAME, ORDER_DATE, ORDER_TOTAL_PRICE, BATCH_ID
        from ORDER_CUSTOMER_DATE_PRICE
        order by ORDER_DATE, CUSTOMER_NAME
        """
    ).fetchall()

    with RESULT_CSV.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["CUSTOMER_NAME", "ORDER_DATE", "ORDER_TOTAL_PRICE", "BATCH_ID"])
        writer.writerows(rows)

    return rows


def print_summary(batch_id: str, customer_rows: int, order_rows: int, final_rows: list[tuple[str, str, float, str]]) -> None:
    print(f"Batch ID: {batch_id}")
    print(f"Customers loaded: {customer_rows}")
    print(f"Orders loaded: {order_rows}")
    print(f"Final rows: {len(final_rows)}")
    print("")
    print("ORDER_CUSTOMER_DATE_PRICE")
    for customer_name, order_date, order_total_price, result_batch_id in final_rows:
        print(
            f"  {customer_name} | {order_date} | {order_total_price:.2f} | {result_batch_id}"
        )
    print("")
    print(f"SQLite database: {SQLITE_DB}")
    print(f"Result CSV: {RESULT_CSV}")


def main() -> None:
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M")
    reset_demo_workspace()

    datasets = [build_dataset_paths("customers"), build_dataset_paths("orders")]
    staged_files: dict[str, Path] = {}
    processing_files: dict[str, Path] = {}

    for dataset in datasets:
        staged_files[dataset.entity] = stage_source_files(dataset)
        processing_files[dataset.entity] = move_between_zones(
            staged_files[dataset.entity], dataset.processing_dir, batch_id
        )

    connection = sqlite3.connect(SQLITE_DB)
    try:
        create_tables(connection)
        customer_rows = load_csv_to_table(
            connection, processing_files["customers"], "CUSTOMER_RAW", 8, batch_id
        )
        order_rows = load_csv_to_table(
            connection, processing_files["orders"], "ORDERS_RAW", 9, batch_id
        )
        transform_orders(connection, batch_id)
        connection.commit()
        final_rows = export_results(connection)
    finally:
        connection.close()

    for dataset in datasets:
        move_between_zones(processing_files[dataset.entity], dataset.processed_dir, batch_id)

    print_summary(batch_id, customer_rows, order_rows, final_rows)


if __name__ == "__main__":
    main()
