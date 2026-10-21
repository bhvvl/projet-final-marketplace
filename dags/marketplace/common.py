"""Briques partagées par les DAGs Marketplace : connexions, raw layer MinIO, chargements staging."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
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


STALE_DATES_SQL = """
WITH loaded AS (
    SELECT dt, max(loaded_at) AS loaded_at FROM dwh.fact_orders GROUP BY dt
),
windowed AS (
    SELECT target.dt, max(source.loaded_at) AS source_loaded_at
    FROM loaded target
    JOIN loaded source ON source.dt BETWEEN target.dt - %(lookback_days)s AND target.dt
    GROUP BY target.dt
)
SELECT w.dt::text, w.source_loaded_at::text
FROM windowed w
LEFT JOIN analytics.refresh_log r ON r.dag_id = %(dag_id)s AND r.dt = w.dt
WHERE %(full_refresh)s OR r.source_loaded_at IS NULL OR w.source_loaded_at > r.source_loaded_at
ORDER BY 1
"""


def dates_to_process(context, dag_id: str, lookback_days: int = 0) -> dict[str, Any]:
    """Dates à recalculer par un DAG aval de fact_orders, déduites de l'état des données.

    Une date est à recalculer si ses faits, ou ceux des lookback_days jours précédents dont
    elle dépend, ont été rechargés depuis son dernier calcul (analytics.refresh_log).
    On ne se fie pas seulement aux événements d'Asset : Airflow peut regrouper ou perdre des
    événements émis à quelques millisecondes d'intervalle (constaté pendant un backfill),
    et un run manuel n'en a pas. Avec params.full_refresh, toutes les dates sont recalculées.

    Retourne {"dates": [...], "versions": {dt: source_loaded_at}} pour mark_refreshed().
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    events = [e for asset_events in (context.get("triggering_asset_events") or {}).values() for e in asset_events]
    announced = sorted({(e.extra or {}).get("dt") for e in events} - {None})
    full_refresh = bool((context.get("params") or {}).get("full_refresh"))

    rows = PostgresHook(postgres_conn_id=DWH_CONN_ID).get_records(
        STALE_DATES_SQL,
        parameters={"dag_id": dag_id, "lookback_days": lookback_days, "full_refresh": full_refresh},
    )
    versions = dict(rows)
    print(f"Événements d'Asset : {announced or 'aucun'} ; full_refresh={full_refresh} ; "
          f"{len(versions)} date(s) à recalculer : {sorted(versions)}")
    return {"dates": sorted(versions), "versions": versions}


def mark_refreshed(dag_id: str, versions: dict[str, str]) -> None:
    """Enregistre la version des faits utilisée pour chaque date recalculée."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    if not versions:
        return
    now = datetime.now(timezone.utc)
    PostgresHook(postgres_conn_id=DWH_CONN_ID).insert_rows(
        "analytics.refresh_log",
        [(dag_id, dt, loaded_at, now) for dt, loaded_at in versions.items()],
        target_fields=["dag_id", "dt", "source_loaded_at", "refreshed_at"],
        replace=True,
        replace_index=["dag_id", "dt"],
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
