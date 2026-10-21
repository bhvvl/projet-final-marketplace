"""Opérateur de contrôle qualité SQL à règles configurables."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from typing import Any

from airflow.sdk import BaseOperator
from airflow.sdk.exceptions import AirflowException

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def _not_empty(table: str, where: str, check: dict) -> str:
    # 1 violation si aucune ligne
    return f"SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END FROM {table} WHERE {where}"


def _not_null(table: str, where: str, check: dict) -> str:
    nulls = " OR ".join(f"{column} IS NULL" for column in check["columns"])
    return f"SELECT count(*) FROM {table} WHERE ({where}) AND ({nulls})"


def _no_future_date(table: str, where: str, check: dict) -> str:
    return f"SELECT count(*) FROM {table} WHERE ({where}) AND {check['column']} > now()"


# règle -> (générateur SQL qui compte les violations, clés de configuration obligatoires)
RULES: dict[str, tuple[Callable[[str, str, dict], str], tuple[str, ...]]] = {
    "not_empty": (_not_empty, ()),
    "not_null": (_not_null, ("columns",)),
    "no_future_date": (_no_future_date, ("column",)),
}


class DataQualityOperator(BaseOperator):
    """Exécute des règles de qualité sur une table (ou une partition) PostgreSQL.

    Chaque règle est une requête qui compte les lignes en violation ; la règle passe si ce
    nombre vaut 0. Règles disponibles :

    - ``{"rule": "not_empty"}`` : au moins une ligne ;
    - ``{"rule": "not_null", "columns": ["order_id", ...]}`` : aucune valeur nulle ;
    - ``{"rule": "no_future_date", "column": "order_ts"}`` : aucune date postérieure à maintenant.

    Le rapport ``{"passed": bool, "checks": [...]}`` est renvoyé en XCom pour un branching.
    Avec ``fail_on_error=True``, la tâche échoue si une règle est violée.

    :param conn_id: Connection PostgreSQL.
    :param table: table contrôlée (``schema.table``).
    :param checks: liste des règles et de leur configuration.
    :param where: filtre SQL de la partition contrôlée (templaté, ex. ``"dt = '{{ ds }}'"``).
    :param fail_on_error: échouer plutôt que renvoyer un rapport négatif.
    """

    template_fields: Sequence[str] = ("where",)
    ui_color = "#fbe9e7"

    def __init__(
        self,
        *,
        conn_id: str,
        table: str,
        checks: list[dict[str, Any]],
        where: str = "TRUE",
        fail_on_error: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.table = table
        self.checks = checks
        self.where = where
        self.fail_on_error = fail_on_error
        self._validate()

    def _validate(self) -> None:
        """Erreurs de configuration détectées au parsing du DAG, pas à l'exécution."""
        if not IDENTIFIER.match(self.table):
            raise ValueError(f"Nom de table invalide : {self.table!r}")
        if not self.checks:
            raise ValueError("Au moins une règle de qualité est requise")
        for check in self.checks:
            rule = check.get("rule")
            if rule not in RULES:
                raise ValueError(f"Règle inconnue {rule!r} ; disponibles : {sorted(RULES)}")
            for key in RULES[rule][1]:
                if not check.get(key):
                    raise ValueError(f"La règle {rule!r} exige le paramètre {key!r}")
            columns = check.get("columns", []) + ([check["column"]] if "column" in check else [])
            for column in columns:
                if not IDENTIFIER.match(column):
                    raise ValueError(f"Nom de colonne invalide : {column!r}")

    def build_sql(self, check: dict[str, Any]) -> str:
        return RULES[check["rule"]][0](self.table, self.where, check)

    def execute(self, context) -> dict[str, Any]:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=self.conn_id)
        results = []
        for check in self.checks:
            violations = int(hook.get_first(self.build_sql(check))[0])
            target = check.get("column") or ", ".join(check.get("columns", [])) or self.table
            results.append({"rule": check["rule"], "target": target, "violations": violations, "passed": violations == 0})
            self.log.info("%s [%s] sur %s : %s (%d violation(s))", check["rule"], target, self.table,
                          "OK" if violations == 0 else "KO", violations)

        report = {"table": self.table, "where": self.where, "passed": all(r["passed"] for r in results),
                  "checks": results}
        if not report["passed"] and self.fail_on_error:
            raise AirflowException(f"Contrôle qualité KO sur {self.table} : {report}")
        return report
