"""Pipeline ELT principal : API Marketplace -> MinIO (raw) -> staging -> dwh.fact_orders.

    extract_orders_to_raw ──► load_orders_staging ──► dq_staging_orders ──► check_quality ──┬──► alert_dq_failed
                                                                                           │
    extract_referentials_to_raw ──► load_dims ─────────────────────────────────────────────┴──► transform_fact_orders
                                                                                                    │
                                                    (Asset fact_orders, extra {"dt": ds}) ◄── publish_fact_orders

Contrôle qualité de staging.orders sur la partition {{ ds }} : si une règle est violée,
le DWH n'est pas touché, une alerte part sur le webhook et le run échoue.

Chaque tâche est rejouable pour une même date {{ ds }} :
  - raw : la partition dt= est écrasée ;
  - staging.orders et dwh.fact_orders : DELETE + INSERT de la partition dans une transaction ;
  - dimensions : UPSERT (SCD1).
Les chargements relisent le raw MinIO : un bug de transformation se corrige
et se rejoue sans rappeler l'API.

Rejouer une date : airflow dags test marketplace_orders_ingest_daily 2026-04-07
"""

from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

from marketplace.common import (
    API_CONN_ID,
    DWH_CONN_ID,
    FACT_ORDERS_ASSET,
    load_dimensions,
    load_orders_to_staging,
    send_alert,
    upload_raw,
)
from operators.data_quality_operator import DataQualityOperator

STAGING_ORDERS_CHECKS = [
    {"rule": "not_empty"},
    # customer_id peut être nul (rattaché au membre UNKNOWN) : il n'est pas contrôlé ici.
    {"rule": "not_null", "columns": ["order_id", "order_ts", "seller_id", "product_id",
                                     "quantity", "total_amount", "status"]},
    {"rule": "no_future_date", "column": "order_ts"},
]


@dag(
    dag_id="marketplace_orders_ingest_daily",
    description="Commandes du jour : API -> MinIO raw -> staging -> dwh.fact_orders (idempotent)",
    # Airflow 3 : "@daily" seul donnerait ds = jour du run (journée incomplète).
    # Avec un data interval, le run de J traite la journée complète J-1 ({{ ds }}).
    schedule=CronDataIntervalTimetable("@daily", timezone="UTC"),
    start_date=datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-team", "retries": 2, "retry_delay": timedelta(minutes=2)},
    dagrun_timeout=timedelta(hours=1),
    params={"commission_rate": 0.12},
    tags=["marketplace", "elt", "ingest"],
)
def marketplace_orders_ingest_daily():
    @task
    def extract_orders_to_raw(**context) -> dict:
        """Appelle /orders?date={{ ds }} et archive le JSON brut dans MinIO."""
        from hooks.marketplace_api_hook import MarketplaceAPIHook

        ds = context["ds"]
        orders = MarketplaceAPIHook(marketplace_api_conn_id=API_CONN_ID).get_orders(ds)
        return {"key": upload_raw(orders, "orders", ds), "count": len(orders)}

    @task
    def extract_referentials_to_raw(**context) -> dict:
        """Instantané des vendeurs, produits et clients, archivé dans MinIO."""
        from hooks.marketplace_api_hook import MarketplaceAPIHook

        ds = context["ds"]
        hook = MarketplaceAPIHook(marketplace_api_conn_id=API_CONN_ID)
        snapshots = {"sellers": hook.get_sellers(), "products": hook.get_products(), "customers": hook.get_customers()}
        return {entity: upload_raw(records, entity, ds) for entity, records in snapshots.items()}

    @task
    def load_orders_staging(extracted: dict, **context) -> int:
        count = load_orders_to_staging(context["ds"])
        if count != extracted["count"]:
            raise ValueError(f"staging.orders : {count} lignes chargées, {extracted['count']} extraites")
        return count

    @task
    def load_dims(_extracted: dict, **context) -> dict:
        return load_dimensions(context["ds"])

    dq_staging_orders = DataQualityOperator(
        task_id="dq_staging_orders",
        conn_id=DWH_CONN_ID,
        table="staging.orders",
        where="dt = '{{ ds }}'",
        checks=STAGING_ORDERS_CHECKS,
    )

    @task.branch
    def check_quality(report: dict) -> str:
        return "transform_fact_orders" if report["passed"] else "alert_dq_failed"

    @task(retries=0)
    def alert_dq_failed(report: dict, **context) -> None:
        from airflow.exceptions import AirflowFailException  # échec sans retry

        failed = [check for check in report["checks"] if not check["passed"]]
        message = f"Qualité KO sur staging.orders pour {context['ds']} : DWH non chargé"
        send_alert("data_quality", message, {"dt": context["ds"], "failed_checks": failed})
        raise AirflowFailException(f"{message} — {failed}")

    transform_fact_orders = SQLExecuteQueryOperator(
        task_id="transform_fact_orders",
        conn_id=DWH_CONN_ID,
        sql="sql/transform_fact_orders.sql",
        autocommit=False,  # DELETE + INSERT validés ensemble, en un seul COMMIT
    )

    @task(outlets=[FACT_ORDERS_ASSET])
    def publish_fact_orders(**context) -> None:
        """Émet l'événement d'Asset en y joignant la date chargée.

        Les DAGs aval n'ont pas de logical_date (Airflow 3) : c'est l'extra qui leur dit quoi recalculer.
        """
        context["outlet_events"][FACT_ORDERS_ASSET].extra = {"dt": context["ds"]}

    staged = load_orders_staging(extract_orders_to_raw())
    dims = load_dims(extract_referentials_to_raw())
    staged >> dq_staging_orders
    branch = check_quality(dq_staging_orders.output)
    branch >> alert_dq_failed(dq_staging_orders.output)
    [branch, dims] >> transform_fact_orders >> publish_fact_orders()


marketplace_orders_ingest_daily()
