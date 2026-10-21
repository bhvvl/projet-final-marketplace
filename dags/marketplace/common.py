"""Briques partagées par les DAGs Marketplace : connexions, raw layer MinIO, chargements staging."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from airflow.sdk import Asset

API_CONN_ID = "marketplace_api"
DWH_CONN_ID = "postgres_dwh"
S3_CONN_ID = "minio_s3"
ALERT_CONN_ID = "alert_webhook"
BUCKET = os.environ.get("MINIO_BUCKET", "data-lake")

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"

# Émis quand dwh.fact_orders est à jour pour une date ; déclenche les DAGs aval.
FACT_ORDERS_ASSET = Asset("postgres://postgres-dwh:5432/dwh/dwh/fact_orders")

ORDER_COLUMNS = (
    "order_id", "order_ts", "customer_id", "seller_id", "product_id", "quantity",
    "unit_price", "total_amount", "currency", "status", "payment_method",
)
REFERENTIALS = {
    "sellers": ("seller_id", "name", "seller_type", "country", "city", "joined_date"),
    "products": ("product_id", "name", "category", "seller_id", "base_price", "created_at"),
    "customers": ("customer_id", "email", "city", "country", "signup_date"),
}


def raw_key(entity: str, ds: str) -> str:
    return f"raw/{entity}/dt={ds}/data.json"


def upload_raw(records: list[dict[str, Any]], entity: str, ds: str) -> str:
    """Archive le JSON brut tel que renvoyé par l'API (écrase la partition : rejouable)."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    key = raw_key(entity, ds)
    S3Hook(aws_conn_id=S3_CONN_ID).load_string(
        json.dumps(records, ensure_ascii=False), key=key, bucket_name=BUCKET, replace=True
    )
    return key


def read_raw(entity: str, ds: str) -> list[dict[str, Any]]:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    return json.loads(S3Hook(aws_conn_id=S3_CONN_ID).read_key(raw_key(entity, ds), bucket_name=BUCKET))


def send_alert(kind: str, message: str, details: Any = None) -> None:
    """POST JSON sur le webhook d'alerte (Connection alert_webhook : host = URL, password = token Bearer)."""
    from airflow.providers.http.hooks.http import HttpHook
    from airflow.sdk import BaseHook

    token = BaseHook.get_connection(ALERT_CONN_ID).password
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    HttpHook(method="POST", http_conn_id=ALERT_CONN_ID).run(
        "/webhook", json={"kind": kind, "message": message, "details": details}, headers=headers
    )


def read_sql(name: str) -> str:
    return (SQL_DIR / name).read_text()


def _rows(records: list[dict[str, Any]], columns: tuple[str, ...], prefix: tuple = ()) -> list[tuple]:
    return [prefix + tuple(record.get(col) for col in columns) for record in records]


def load_orders_to_staging(ds: str) -> int:
    """staging.orders pour la date ds, depuis le raw MinIO : DELETE + INSERT dans une transaction."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from psycopg2.extras import execute_values

    rows = _rows(read_raw("orders", ds), ORDER_COLUMNS, prefix=(ds,))
    conn = PostgresHook(postgres_conn_id=DWH_CONN_ID).get_conn()
    try:
        with conn, conn.cursor() as cur:  # commit si tout passe, rollback sinon
            cur.execute("DELETE FROM staging.orders WHERE dt = %s", (ds,))
            if rows:
                execute_values(
                    cur,
                    f"INSERT INTO staging.orders (dt, {', '.join(ORDER_COLUMNS)}) VALUES %s",
                    rows,
                    page_size=1000,
                )
    finally:
        conn.close()
    return len(rows)


def load_dimensions(ds: str) -> dict[str, int]:
    """Instantané des référentiels en staging (TRUNCATE + INSERT) puis UPSERT des dimensions (SCD1)."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from psycopg2.extras import execute_values

    counts = {}
    conn = PostgresHook(postgres_conn_id=DWH_CONN_ID).get_conn()
    try:
        with conn, conn.cursor() as cur:
            for entity, columns in REFERENTIALS.items():
                rows = _rows(read_raw(entity, ds), columns)
                cur.execute(f"TRUNCATE staging.{entity}")
                execute_values(
                    cur, f"INSERT INTO staging.{entity} ({', '.join(columns)}) VALUES %s", rows, page_size=1000
                )
                counts[entity] = len(rows)
            cur.execute(read_sql("upsert_dimensions.sql"))
    finally:
        conn.close()
    return counts
