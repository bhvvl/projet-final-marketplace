"""Détection d'anomalies commerciales, déclenchée par la mise à jour de dwh.fact_orders.

    resolve_dates ──► detect_anomalies ──► check_anomalies ──┬──► notify_anomalies   (webhook)
                                                             └──► no_anomaly

Règles (seuils dans params) :
  - global  : CA du jour < 70 % de la moyenne des 7 jours précédents (US-05) ;
  - seller  : même règle par vendeur, pour les vendeurs significatifs ;
  - price   : prix unitaire > 3× ou < 0,2× le prix catalogue.
Résultats dans analytics.anomalies (DELETE + INSERT par date, idempotent).

Planifié sur l'Asset fact_orders plutôt qu'à heure fixe : la détection part dès que
les données du jour sont chargées. Seule la date la plus récente traitée est notifiée
(pas de rafale d'alertes pendant un backfill).
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task

from marketplace.common import DWH_CONN_ID, FACT_ORDERS_ASSET, dates_to_process, read_sql, send_alert


@dag(
    dag_id="marketplace_anomaly_detect_daily",
    description="Anomalies de CA (global, vendeurs) et prix suspects -> analytics.anomalies + alerte",
    schedule=[FACT_ORDERS_ASSET],
    start_date=datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-team", "retries": 2, "retry_delay": timedelta(minutes=1)},
    params={
        "threshold": 0.7,
        "min_history_days": 4,
        "seller_min_avg": 1500,
        "price_high_ratio": 3,
        "price_low_ratio": 0.2,
    },
    tags=["marketplace", "anomalies"],
)
def marketplace_anomaly_detect_daily():
    @task
    def resolve_dates(**context) -> list[str]:
        return dates_to_process(context)

    @task
    def detect_anomalies(dates: list[str], **context) -> dict:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        sql = read_sql("anomaly_detect.sql")
        for dt in dates:
            hook.run(sql, parameters={"dt": dt, **context["params"]}, autocommit=False)

        latest = max(dates) if dates else None
        rows = hook.get_records(
            "SELECT scope, entity_id, value, reference, ratio FROM analytics.anomalies "
            "WHERE dt = %s ORDER BY scope, ratio",
            parameters=(latest,),
        ) if latest else []
        anomalies = [
            {"scope": s, "entity_id": e, "value": float(v), "reference": float(r), "ratio": float(q)}
            for s, e, v, r, q in rows
        ]
        print(f"{len(dates)} date(s) analysée(s) ; {len(anomalies)} anomalie(s) le {latest}")
        return {"dates": len(dates), "latest": latest, "anomalies": anomalies}

    @task.branch
    def check_anomalies(result: dict) -> str:
        return "notify_anomalies" if result["anomalies"] else "no_anomaly"

    @task
    def notify_anomalies(result: dict) -> None:
        by_scope = {}
        for anomaly in result["anomalies"]:
            by_scope.setdefault(anomaly["scope"], []).append(anomaly)
        counts = {scope: len(items) for scope, items in by_scope.items()}
        message = f"{len(result['anomalies'])} anomalie(s) commerciale(s) le {result['latest']} : {counts}"
        # Les 10 écarts les plus forts par type suffisent à l'alerte ; le détail est dans analytics.anomalies.
        send_alert("anomalies", message, {"dt": result["latest"], "counts": counts,
                                          "top": {scope: items[:10] for scope, items in by_scope.items()}})

    result = detect_anomalies(resolve_dates())
    check_anomalies(result) >> [notify_anomalies(result), EmptyOperator(task_id="no_anomaly")]


marketplace_anomaly_detect_daily()
