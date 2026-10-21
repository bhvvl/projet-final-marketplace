#!/usr/bin/env bash
# Charge les données de démonstration puis configure Metabase. Ré-exécutable.
#
#   ./scripts/load_demo_data.sh [DATE_DEBUT] [DATE_FIN]     (défaut : 2026-09-01 2026-10-20)
#
# 1. backfill Airflow du DAG d'ingestion : les dates déjà chargées avec succès sont ignorées,
#    les runs en échec sont rejoués (relancer le script répare un backfill interrompu) ;
#    chaque run déclenche les agrégats analytics et la détection d'anomalies (Asset) ;
# 2. remet le DAG d'ingestion en pause (pour que "airflow dags test" reste utilisable) ;
# 3. déclenche une dernière fois les DAGs aval : ils rattrapent les dates dont les faits ont
#    changé depuis leur dernier calcul (filet de sécurité si Airflow a regroupé ou perdu
#    des événements d'Asset pendant le backfill) ;
# 4. crée ou met à jour les dashboards Metabase.
set -euo pipefail
cd "$(dirname "$0")/.."

FROM=${1:-2026-09-01}
TO=${2:-2026-10-20}
INGEST=marketplace_orders_ingest_daily
DOWNSTREAM="marketplace_analytics_aggregate_daily marketplace_anomaly_detect_daily"

airflow() { docker compose exec -T airflow-worker airflow "$@" 2>/dev/null; }
active_runs() {
    docker compose exec -T postgres-airflow sh -c \
        'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT count(*) FROM dag_run WHERE state IN ('"'queued'"', '"'running'"')"'
}
wait_for_runs() {
    sleep 10
    while [ "$(active_runs)" != "0" ]; do
        echo "  … $(active_runs) run(s) en cours"
        sleep 15
    done
}

echo "▶ Backfill de $INGEST du $FROM au $TO"
airflow dags unpause "$INGEST" >/dev/null
airflow backfill create --dag-id "$INGEST" --from-date "$FROM" --to-date "$TO" \
    --reprocess-behavior failed --max-active-runs 5 >/dev/null
wait_for_runs
airflow dags pause "$INGEST" >/dev/null

echo "▶ Rattrapage des agrégats et des anomalies"
for dag_id in $DOWNSTREAM; do airflow dags trigger "$dag_id" >/dev/null; done
wait_for_runs

echo "▶ Bilan"
docker compose exec -T postgres-airflow sh -c \
    'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT dag_id, run_type, state, count(*) FROM dag_run GROUP BY 1, 2, 3 ORDER BY 1, 2, 3"'
docker compose exec -T postgres-dwh sh -c \
    'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT count(*) AS commandes, count(DISTINCT dt) AS jours, sum(total_amount) FILTER (WHERE status = '"'completed'"') AS ca FROM dwh.fact_orders"'

echo "▶ Metabase"
python3 metabase/setup_dashboards.py
