"""Agrégats analytics (lus par Metabase), déclenchés par la mise à jour de dwh.fact_orders.

    resolve_dates ──► build_daily_summary  ──┐
                  ├─► build_seller_daily   ──┼──► mark_refreshed
                  └─► build_category_daily ──┘

Planifié sur l'Asset fact_orders : chaque run d'ingestion réussi déclenche ce DAG.
Les dates à recalculer sont déduites de l'état des données (voir dates_to_process) :
celles dont les faits ont été rechargés depuis leur dernier calcul. Un événement d'Asset
perdu ou regroupé est donc rattrapé au run suivant. Lancé à la main, le DAG rattrape les
dates en retard ; avec {"full_refresh": true}, il recalcule tout.

Chaque table est reconstruite par DELETE + INSERT par date : idempotent.
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task

from marketplace.common import (
    DWH_CONN_ID,
    FACT_ORDERS_ASSET,
    dates_to_process,
    mark_refreshed,
    notify_dag_failure,
    read_sql,
)

DAG_ID = "marketplace_analytics_aggregate_daily"
AGGREGATES = {
    "build_daily_summary": "analytics_daily_summary.sql",
    "build_seller_daily": "analytics_seller_daily.sql",
    "build_category_daily": "analytics_category_daily.sql",
}


@dag(
    dag_id=DAG_ID,
    description="Agrégats analytics.* recalculés à chaque mise à jour de dwh.fact_orders",
    schedule=[FACT_ORDERS_ASSET],
    start_date=datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    # Actif dès sa création : il ne fait que réagir aux ingestions (aucune exécution sans événement).
    is_paused_upon_creation=False,
    on_failure_callback=notify_dag_failure,  # US-07 : alerte webhook si le run échoue
    default_args={"owner": "data-team", "retries": 2, "retry_delay": timedelta(minutes=1)},
    params={"full_refresh": False},
    tags=["marketplace", "analytics"],
)
def marketplace_analytics_aggregate_daily():
    @task
    def resolve_dates(**context) -> dict:
        return dates_to_process(context, dag_id=DAG_ID)

    @task
    def build_aggregate(sql_file: str, dates: list[str]) -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        sql = read_sql(sql_file)
        for dt in dates:
            hook.run(sql, parameters={"dt": dt}, autocommit=False)  # 1 transaction par date
        return len(dates)

    @task(task_id="mark_refreshed")
    def mark_dates_refreshed(versions: dict) -> None:
        mark_refreshed(DAG_ID, versions)

    resolved = resolve_dates()
    builds = [
        build_aggregate.override(task_id=task_id)(sql_file, resolved["dates"])
        for task_id, sql_file in AGGREGATES.items()
    ]
    builds >> mark_dates_refreshed(resolved["versions"])


marketplace_analytics_aggregate_daily()
