"""Agrégats analytics (lus par Metabase), déclenchés par la mise à jour de dwh.fact_orders.

    resolve_dates ──► build_daily_summary
                  ├─► build_seller_daily
                  └─► build_category_daily

Planifié sur l'Asset fact_orders : chaque run d'ingestion réussi déclenche ce DAG.
Un run déclenché par Asset n'a pas de logical_date en Airflow 3 : les dates à
recalculer sont lues dans l'extra {"dt": ...} des événements (plusieurs événements
peuvent être regroupés dans un même run, ex. pendant un backfill).
Lancé à la main, ou si un événement n'a pas de dt, le DAG recalcule toutes les dates de fact_orders.

Chaque table est reconstruite par DELETE + INSERT par date : idempotent.
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task

from marketplace.common import DWH_CONN_ID, FACT_ORDERS_ASSET, dates_to_process, read_sql

AGGREGATES = {
    "build_daily_summary": "analytics_daily_summary.sql",
    "build_seller_daily": "analytics_seller_daily.sql",
    "build_category_daily": "analytics_category_daily.sql",
}


@dag(
    dag_id="marketplace_analytics_aggregate_daily",
    description="Agrégats analytics.* recalculés à chaque mise à jour de dwh.fact_orders",
    schedule=[FACT_ORDERS_ASSET],
    start_date=datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-team", "retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["marketplace", "analytics"],
)
def marketplace_analytics_aggregate_daily():
    @task
    def resolve_dates(**context) -> list[str]:
        return dates_to_process(context)

    @task
    def build_aggregate(sql_file: str, dates: list[str]) -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        sql = read_sql(sql_file)
        for dt in dates:
            hook.run(sql, parameters={"dt": dt}, autocommit=False)  # 1 transaction par date
        return len(dates)

    dates = resolve_dates()
    for task_id, sql_file in AGGREGATES.items():
        build_aggregate.override(task_id=task_id)(sql_file, dates)


marketplace_analytics_aggregate_daily()
