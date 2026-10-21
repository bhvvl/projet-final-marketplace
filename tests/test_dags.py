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
        "transform_fact_orders": {"load_orders_staging", "load_dims"},
    }
    assert set(ingest_dag.task_ids) == set(expected_upstream)
    for task_id, upstream in expected_upstream.items():
        assert ingest_dag.get_task(task_id).upstream_task_ids == upstream, task_id

    assert ingest_dag.max_active_runs == 1
    assert all(task.retries >= 1 for task in ingest_dag.tasks)
    outlets = ingest_dag.get_task("transform_fact_orders").outlets
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
