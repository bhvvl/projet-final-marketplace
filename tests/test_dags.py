import re
from pathlib import Path

import pytest

INGEST_DAG_ID = "marketplace_orders_ingest_daily"
CHECKSUM_SQL = """
    SELECT count(*), md5(string_agg(
        order_id || '|' || seller_id || '|' || product_id || '|' || customer_id || '|'
        || total_amount || '|' || commission_amount || '|' || status, ',' ORDER BY order_id))
    FROM dwh.fact_orders WHERE dt = %s
"""


@pytest.fixture(scope="module")
def ingest_dag(dagbag):
    dag = dagbag.get_dag(INGEST_DAG_ID)
    assert dag is not None, f"DAG {INGEST_DAG_ID} introuvable"
    return dag


def transform_sql(dag) -> str:
    sql = dag.get_task("transform_fact_orders").sql
    # Le DagBag remplace en général le chemin .sql par le contenu du fichier.
    return (Path(dag.fileloc).parent / sql).read_text() if sql.endswith(".sql") else sql


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}
    assert INGEST_DAG_ID in dagbag.dag_ids


def test_all_dags_catchup_false(dagbag):
    assert dagbag.dags
    for dag_id, dag in dagbag.dags.items():
        assert dag.catchup is False, f"{dag_id} : catchup doit être False"


def test_marketplace_ingest_has_idempotent_transform(ingest_dag):
    task = ingest_dag.get_task("transform_fact_orders")
    sql = re.sub(r"--[^\n]*", "", transform_sql(ingest_dag))  # ignore les commentaires

    delete = re.search(r"DELETE\s+FROM\s+dwh\.fact_orders\s+WHERE\s+dt\s*=\s*'\{\{ ds \}\}'", sql)
    insert = re.search(r"INSERT\s+INTO\s+dwh\.fact_orders", sql)
    assert delete, "la partition {{ ds }} doit être supprimée avant rechargement"
    assert insert and delete.start() < insert.start(), "DELETE doit précéder INSERT"
    assert re.search(r"WHERE\s+o\.dt\s*=\s*'\{\{ ds \}\}'", sql), "l'INSERT doit lire la seule partition {{ ds }}"
    assert not re.search(r"\d{4}-\d{2}-\d{2}", sql), "aucune date en dur dans le SQL"
    assert task.autocommit is False, "DELETE + INSERT doivent être validés dans une seule transaction"


def test_marketplace_ingest_structure(ingest_dag):
    expected_upstream = {
        "extract_orders_to_raw": set(),
        "extract_referentials_to_raw": set(),
        "load_orders_staging": {"extract_orders_to_raw"},
        "load_dims": {"extract_referentials_to_raw"},
        "dq_staging_orders": {"load_orders_staging"},
        "check_quality": {"dq_staging_orders"},
        "alert_dq_failed": {"check_quality", "dq_staging_orders"},
        "transform_fact_orders": {"check_quality", "load_dims"},
        "publish_fact_orders": {"transform_fact_orders"},
    }
    assert set(ingest_dag.task_ids) == set(expected_upstream)
    for task_id, upstream in expected_upstream.items():
        assert ingest_dag.get_task(task_id).upstream_task_ids == upstream, task_id

    assert ingest_dag.max_active_runs == 1
    assert all(task.retries >= 1 for task in ingest_dag.tasks if task.task_id != "alert_dq_failed")
    outlets = ingest_dag.get_task("publish_fact_orders").outlets
    assert [asset.uri for asset in outlets] == ["postgres://postgres-dwh:5432/dwh/dwh/fact_orders"]


def test_transform_is_idempotent_on_dwh(ingest_dag):
    """Joue 2 fois le SQL de transformation sur le DWH du compose : contenu identique.

    Ignoré si le DWH est injoignable ou si staging.orders est vide.
    """
    import jinja2
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="postgres_dwh")
    try:
        row = hook.get_first("SELECT max(dt)::text FROM staging.orders")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"DWH injoignable : {exc}")
    if not row or row[0] is None:
        pytest.skip("staging.orders vide : lancer d'abord le DAG d'ingestion")
    ds = row[0]

    sql = jinja2.Template(transform_sql(ingest_dag)).render(
        ds=ds, params={"commission_rate": ingest_dag.params["commission_rate"]}
    )
    results = []
    for _ in range(2):
        hook.run(sql, autocommit=False)
        results.append(hook.get_first(CHECKSUM_SQL, parameters=(ds,)))

    staged = hook.get_first("SELECT count(*) FROM staging.orders WHERE dt = %s", parameters=(ds,))[0]
    assert results[0] == results[1]
    assert results[0][0] == staged


# --------------------------------------------------------------------- analytics

ANALYTICS_DAG_ID = "marketplace_analytics_aggregate_daily"
ANALYTICS_TABLES = {
    "analytics_daily_summary.sql": "analytics.daily_summary",
    "analytics_seller_daily.sql": "analytics.seller_daily",
    "analytics_category_daily.sql": "analytics.category_daily",
}


@pytest.fixture(scope="module")
def analytics_dag(dagbag):
    dag = dagbag.get_dag(ANALYTICS_DAG_ID)
    assert dag is not None, f"DAG {ANALYTICS_DAG_ID} introuvable"
    return dag


def test_analytics_is_triggered_by_fact_orders_asset(analytics_dag):
    from airflow.timetables.simple import AssetTriggeredTimetable

    assert isinstance(analytics_dag.timetable, AssetTriggeredTimetable)
    uris = [asset.uri for _, asset in analytics_dag.timetable.asset_condition.iter_assets()]
    assert uris == ["postgres://postgres-dwh:5432/dwh/dwh/fact_orders"]
    assert set(analytics_dag.task_ids) == {
        "resolve_dates", "build_daily_summary", "build_seller_daily", "build_category_daily"
    }


@pytest.mark.parametrize("sql_file, table", ANALYTICS_TABLES.items())
def test_analytics_sql_is_idempotent(sql_file, table):
    sql = re.sub(r"--[^\n]*", "", (Path(__file__).resolve().parents[1] / "dags" / "sql" / sql_file).read_text())

    delete = re.search(rf"DELETE\s+FROM\s+{re.escape(table)}\s+WHERE\s+dt\s*=\s*%\(dt\)s", sql)
    insert = re.search(rf"INSERT\s+INTO\s+{re.escape(table)}", sql)
    assert delete and insert and delete.start() < insert.start()


def test_resolve_dates_reads_dt_from_asset_events(analytics_dag):
    from types import SimpleNamespace

    resolve = analytics_dag.get_task("resolve_dates").python_callable
    asset = analytics_dag.timetable.asset_condition
    events = {asset: [SimpleNamespace(extra={"dt": "2026-10-19"}), SimpleNamespace(extra={"dt": "2026-10-18"})]}

    assert resolve(triggering_asset_events=events) == ["2026-10-18", "2026-10-19"]


def test_analytics_reconciles_with_fact_orders():
    """Le CA est identique dans fact_orders et dans les 3 tables analytics (DWH du compose)."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="postgres_dwh")
    try:
        fact = hook.get_first("SELECT sum(total_amount) FROM dwh.fact_orders WHERE status = 'completed'")[0]
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"DWH injoignable : {exc}")
    if fact is None:
        pytest.skip("dwh.fact_orders vide")

    aggregates = hook.get_first(
        "SELECT (SELECT sum(total_revenue) FROM analytics.daily_summary),"
        "       (SELECT sum(revenue) FROM analytics.seller_daily),"
        "       (SELECT sum(revenue) FROM analytics.category_daily)"
    )
    assert list(aggregates) == [fact, fact, fact]


@pytest.mark.parametrize("passed, expected", [(True, "transform_fact_orders"), (False, "alert_dq_failed")])
def test_quality_branch_routes_to_dwh_or_alert(ingest_dag, passed, expected):
    branch = ingest_dag.get_task("check_quality").python_callable

    assert branch({"passed": passed, "checks": []}) == expected
