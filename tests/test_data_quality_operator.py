"""Tests du DataQualityOperator, base de données mockée."""

from unittest import mock

import pytest
from airflow.sdk.exceptions import AirflowException

from operators.data_quality_operator import DataQualityOperator

CHECKS = [
    {"rule": "not_empty"},
    {"rule": "not_null", "columns": ["order_id", "seller_id"]},
    {"rule": "no_future_date", "column": "order_ts"},
]


def make_operator(**kwargs):
    params = {"task_id": "dq", "conn_id": "postgres_dwh", "table": "staging.orders",
              "checks": CHECKS, "where": "dt = '2026-04-07'"}
    params.update(kwargs)
    return DataQualityOperator(**params)


def run_with_counts(operator, counts):
    """Exécute l'opérateur avec get_first renvoyant successivement les nombres de violations."""
    with mock.patch("airflow.providers.postgres.hooks.postgres.PostgresHook.get_first",
                    side_effect=[(c,) for c in counts]) as get_first:
        report = operator.execute(context={})
    return report, [call.args[0] for call in get_first.call_args_list]


def test_all_rules_pass():
    report, queries = run_with_counts(make_operator(), [0, 0, 0])

    assert report["passed"] is True
    assert [c["rule"] for c in report["checks"]] == ["not_empty", "not_null", "no_future_date"]
    assert all("dt = '2026-04-07'" in q and "staging.orders" in q for q in queries)


def test_generated_sql_per_rule():
    operator = make_operator()

    assert "count(*) = 0" in operator.build_sql(CHECKS[0])
    assert "order_id IS NULL OR seller_id IS NULL" in operator.build_sql(CHECKS[1])
    assert "order_ts > now()" in operator.build_sql(CHECKS[2])


def test_failed_rule_returns_negative_report_without_raising():
    report, _ = run_with_counts(make_operator(), [0, 3, 0])

    assert report["passed"] is False
    failed = [c for c in report["checks"] if not c["passed"]]
    assert failed == [{"rule": "not_null", "target": "order_id, seller_id", "violations": 3, "passed": False}]


def test_empty_partition_fails_not_empty():
    report, _ = run_with_counts(make_operator(checks=[{"rule": "not_empty"}]), [1])

    assert report["passed"] is False


def test_fail_on_error_raises():
    with pytest.raises(AirflowException, match="Contrôle qualité KO"):
        run_with_counts(make_operator(fail_on_error=True), [0, 0, 2])


@pytest.mark.parametrize("checks, message", [
    ([], "Au moins une règle"),
    ([{"rule": "unique"}], "Règle inconnue"),
    ([{"rule": "not_null"}], "exige le paramètre 'columns'"),
    ([{"rule": "no_future_date"}], "exige le paramètre 'column'"),
    ([{"rule": "not_null", "columns": ["id; DROP TABLE x"]}], "Nom de colonne invalide"),
])
def test_invalid_configuration_is_rejected_at_parse_time(checks, message):
    with pytest.raises(ValueError, match=message):
        make_operator(checks=checks)


def test_where_is_templated():
    assert "where" in DataQualityOperator.template_fields
