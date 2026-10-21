#!/usr/bin/env python3
"""Configure Metabase de façon reproductible via son API REST (bibliothèque standard uniquement).

    python3 metabase/setup_dashboards.py

1. Premier lancement : crée le compte admin (MB_ADMIN_EMAIL / MB_ADMIN_PASSWORD du .env).
2. Déclare la source PostgreSQL DWH (host postgres-dwh, port interne 5432).
3. Crée ou met à jour, par leur nom, les questions SQL et les 2 dashboards MVP
   dans la collection "Maelys Marketplace".

Ré-exécutable : utile après un "docker compose down -v" qui efface la base H2 de Metabase.
Les KPIs sont calculés par rapport au dernier jour chargé (max(dt)), pas à la date système :
les dashboards restent lisibles quelle que soit la date de démonstration.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COLLECTION = "Maelys Marketplace"
DATABASE = "Maelys DWH"

REF_DAY = "(SELECT max(dt) FROM analytics.daily_summary)"

CARDS = {
    # ------------------------------------------------------------ Executive Summary
    "CA du dernier jour": {
        "display": "smartscalar",
        "sql": f"""
SELECT dt AS "Jour", total_revenue AS "CA"
FROM analytics.daily_summary
WHERE dt > {REF_DAY} - 30
ORDER BY dt""",
        "viz": {"column_settings": {'["name","CA"]': {"number_style": "currency", "currency": "EUR"}}},
    },
    "Commandes du dernier jour": {
        "display": "scalar",
        "sql": f"""
SELECT total_orders AS "Commandes"
FROM analytics.daily_summary WHERE dt = {REF_DAY}""",
        "viz": {},
    },
    "Panier moyen du dernier jour": {
        "display": "scalar",
        "sql": f"""
SELECT avg_basket AS "Panier moyen"
FROM analytics.daily_summary WHERE dt = {REF_DAY}""",
        "viz": {"column_settings": {'["name","Panier moyen"]': {"number_style": "currency", "currency": "EUR"}}},
    },
    "Commission Maelys du dernier jour": {
        "display": "scalar",
        "sql": f"""
SELECT total_commission AS "Commission"
FROM analytics.daily_summary WHERE dt = {REF_DAY}""",
        "viz": {"column_settings": {'["name","Commission"]': {"number_style": "currency", "currency": "EUR"}}},
    },
    "CA des 30 derniers jours": {
        "display": "line",
        "sql": f"""
SELECT dt AS "Jour", total_revenue AS "CA"
FROM analytics.daily_summary
WHERE dt > {REF_DAY} - 30
ORDER BY dt""",
        "viz": {
            "graph.dimensions": ["Jour"],
            "graph.metrics": ["CA"],
            "graph.x_axis.title_text": "Jour",
            "graph.y_axis.title_text": "CA (€)",
        },
    },
    "Top 5 vendeurs du dernier jour": {
        "display": "row",
        "sql": f"""
SELECT s.name AS "Vendeur", sd.revenue AS "CA"
FROM analytics.seller_daily sd
JOIN dwh.dim_seller s ON s.seller_id = sd.seller_id
WHERE sd.dt = {REF_DAY}
ORDER BY sd.revenue DESC
LIMIT 5""",
        "viz": {"graph.dimensions": ["Vendeur"], "graph.metrics": ["CA"], "graph.show_values": True},
    },
    # ------------------------------------------------------------------ Top Sellers
    "Top 10 vendeurs du mois": {
        "display": "row",
        "sql": f"""
SELECT s.name AS "Vendeur", sum(sd.revenue) AS "CA"
FROM analytics.seller_daily sd
JOIN dwh.dim_seller s ON s.seller_id = sd.seller_id
WHERE sd.dt BETWEEN date_trunc('month', {REF_DAY})::date AND {REF_DAY}
GROUP BY s.name
ORDER BY 2 DESC
LIMIT 10""",
        "viz": {"graph.dimensions": ["Vendeur"], "graph.metrics": ["CA"], "graph.show_values": True},
    },
    "CA des 3 meilleurs vendeurs sur 30 jours": {
        "display": "line",
        "sql": f"""
WITH ref AS (SELECT {REF_DAY} AS d),
top3 AS (
    SELECT sd.seller_id
    FROM analytics.seller_daily sd, ref
    WHERE sd.dt > ref.d - 30
    GROUP BY sd.seller_id
    ORDER BY sum(sd.revenue) DESC
    LIMIT 3
),
days AS (
    SELECT generate_series(ref.d - 29, ref.d, interval '1 day')::date AS dt FROM ref
)
SELECT days.dt AS "Jour", s.name AS "Vendeur", COALESCE(sd.revenue, 0) AS "CA"
FROM days
CROSS JOIN top3
JOIN dwh.dim_seller s ON s.seller_id = top3.seller_id
LEFT JOIN analytics.seller_daily sd ON sd.dt = days.dt AND sd.seller_id = top3.seller_id
ORDER BY 1, 2""",
        "viz": {"graph.dimensions": ["Jour", "Vendeur"], "graph.metrics": ["CA"], "graph.y_axis.title_text": "CA (€)"},
    },
    "Vendeurs inactifs depuis plus de 7 jours": {
        "display": "table",
        "sql": f"""
WITH last_sale AS (
    SELECT seller_id, max(dt) AS last_dt
    FROM dwh.fact_orders
    WHERE status = 'completed'
    GROUP BY seller_id
)
SELECT s.name AS "Vendeur", s.seller_id AS "ID", s.seller_type AS "Type", s.country AS "Pays",
       l.last_dt AS "Dernière vente",
       {REF_DAY} - l.last_dt AS "Jours sans vente"
FROM dwh.dim_seller s
LEFT JOIN last_sale l ON l.seller_id = s.seller_id
WHERE s.seller_id <> 'UNKNOWN'
  AND (l.last_dt IS NULL OR l.last_dt < {REF_DAY} - 7)
ORDER BY l.last_dt NULLS FIRST, s.name""",
        "viz": {},
    },
    # ------------------------------------------------------------------ Commissions
    "Commission du mois": {
        "display": "scalar",
        "sql": f"""
SELECT sum(total_commission) AS "Commission"
FROM analytics.daily_summary
WHERE dt BETWEEN date_trunc('month', {REF_DAY})::date AND {REF_DAY}""",
        "viz": {"column_settings": {'["name","Commission"]': {"number_style": "currency", "currency": "EUR"}}},
    },
    "CA commissionné du mois": {
        "display": "scalar",
        "sql": f"""
SELECT sum(total_revenue) AS "CA"
FROM analytics.daily_summary
WHERE dt BETWEEN date_trunc('month', {REF_DAY})::date AND {REF_DAY}""",
        "viz": {"column_settings": {'["name","CA"]': {"number_style": "currency", "currency": "EUR"}}},
    },
    "CA annulé ou remboursé du mois (non commissionné)": {
        "display": "scalar",
        "sql": f"""
SELECT COALESCE(sum(total_amount), 0) AS "CA non commissionné"
FROM dwh.fact_orders
WHERE status IN ('cancelled', 'refunded')
  AND dt BETWEEN date_trunc('month', {REF_DAY})::date AND {REF_DAY}""",
        "viz": {"column_settings": {'["name","CA non commissionné"]': {"number_style": "currency", "currency": "EUR"}}},
    },
    "Commissions par jour (30 jours)": {
        "display": "bar",
        "sql": f"""
SELECT dt AS "Jour", total_commission AS "Commission"
FROM analytics.daily_summary
WHERE dt > {REF_DAY} - 30
ORDER BY dt""",
        "viz": {"graph.dimensions": ["Jour"], "graph.metrics": ["Commission"], "graph.y_axis.title_text": "Commission (€)"},
    },
    "Récapitulatif des commissions par jour (mois)": {
        "display": "table",
        "sql": f"""
SELECT dt AS "Jour",
       completed_orders AS "Commandes facturées",
       cancelled_orders AS "Annulées",
       refunded_orders AS "Remboursées",
       total_revenue AS "CA",
       total_commission AS "Commission"
FROM analytics.daily_summary
WHERE dt BETWEEN date_trunc('month', {REF_DAY})::date AND {REF_DAY}
ORDER BY dt DESC""",
        "viz": {},
    },
    "Commissions à facturer par vendeur (mois)": {
        "display": "table",
        "sql": f"""
SELECT s.name AS "Vendeur", s.seller_id AS "ID",
       sum(sd.orders) AS "Commandes", sum(sd.revenue) AS "CA", sum(sd.commission) AS "Commission"
FROM analytics.seller_daily sd
JOIN dwh.dim_seller s ON s.seller_id = sd.seller_id
WHERE sd.dt BETWEEN date_trunc('month', {REF_DAY})::date AND {REF_DAY}
GROUP BY s.name, s.seller_id
ORDER BY "Commission" DESC""",
        "viz": {},
    },
    # ------------------------------------------------------------------- Catégories
    "Répartition du CA par catégorie (30 jours)": {
        "display": "pie",
        "sql": f"""
SELECT category AS "Catégorie", sum(revenue) AS "CA"
FROM analytics.category_daily
WHERE dt > {REF_DAY} - 30
GROUP BY category
ORDER BY 2 DESC""",
        "viz": {"pie.dimension": "Catégorie", "pie.metric": "CA", "pie.show_legend": True},
    },
    "CA hebdomadaire par catégorie": {
        "display": "bar",
        "sql": f"""
SELECT date_trunc('week', dt)::date AS "Semaine", category AS "Catégorie", sum(revenue) AS "CA"
FROM analytics.category_daily
WHERE dt > {REF_DAY} - 28
GROUP BY 1, 2
ORDER BY 1, 2""",
        "viz": {
            "graph.dimensions": ["Semaine", "Catégorie"],
            "graph.metrics": ["CA"],
            "stackable.stack_type": "stacked",
            "graph.y_axis.title_text": "CA (€)",
        },
    },
    "Indicateurs par catégorie (30 jours)": {
        "display": "table",
        "sql": f"""
SELECT category AS "Catégorie",
       sum(orders) AS "Commandes",
       sum(revenue) AS "CA",
       round(sum(revenue) / NULLIF(sum(orders), 0), 2) AS "Panier moyen",
       sum(revenue) / sum(sum(revenue)) OVER () AS "Part du CA"
FROM analytics.category_daily
WHERE dt > {REF_DAY} - 30
GROUP BY category
ORDER BY "CA" DESC""",
        "viz": {"column_settings": {'["name","Part du CA"]': {"number_style": "percent", "decimals": 1}}},
    },
    # -------------------------------------------------------------------- Anomalies
    "Anomalies détectées par jour (30 jours)": {
        "display": "bar",
        "sql": f"""
SELECT dt AS "Jour",
       CASE scope WHEN 'global' THEN 'Chute du CA global'
                  WHEN 'seller' THEN 'Chute de CA vendeur'
                  ELSE 'Prix suspect' END AS "Type",
       count(*) AS "Anomalies"
FROM analytics.anomalies
WHERE dt > {REF_DAY} - 30
GROUP BY 1, 2
ORDER BY 1""",
        "viz": {"graph.dimensions": ["Jour", "Type"], "graph.metrics": ["Anomalies"], "stackable.stack_type": "stacked"},
    },
    "Chutes de CA (< 70 % de la moyenne 7 jours)": {
        "display": "table",
        "sql": """
SELECT a.dt AS "Jour",
       CASE a.scope WHEN 'global' THEN 'Marketplace' ELSE s.name END AS "Périmètre",
       a.value AS "CA du jour", a.reference AS "Moyenne 7 jours", a.ratio AS "Ratio"
FROM analytics.anomalies a
LEFT JOIN dwh.dim_seller s ON s.seller_id = a.entity_id
WHERE a.scope IN ('global', 'seller')
ORDER BY a.dt DESC, a.scope, a.ratio""",
        "viz": {"column_settings": {'["name","Ratio"]': {"number_style": "percent", "decimals": 0}}},
    },
    "Prix suspects (hors 0,2× – 3× le prix catalogue)": {
        "display": "table",
        "sql": """
SELECT a.dt AS "Jour", a.entity_id AS "Commande", p.name AS "Produit", s.name AS "Vendeur",
       a.value AS "Prix payé", a.reference AS "Prix catalogue", a.ratio AS "Ratio"
FROM analytics.anomalies a
JOIN dwh.fact_orders f ON f.order_id = a.entity_id
JOIN dwh.dim_product p ON p.product_id = f.product_id
JOIN dwh.dim_seller s ON s.seller_id = f.seller_id
WHERE a.scope = 'price'
ORDER BY a.dt DESC, a.ratio DESC""",
        "viz": {},
    },
}

# Grille Metabase : 24 colonnes.
DASHBOARDS = {
    "Executive Summary": {
        "description": "KPIs du dernier jour chargé et tendance du CA sur 30 jours (commandes completed).",
        "cards": [
            ("CA du dernier jour", 0, 0, 6, 4),
            ("Commandes du dernier jour", 0, 6, 6, 4),
            ("Panier moyen du dernier jour", 0, 12, 6, 4),
            ("Commission Maelys du dernier jour", 0, 18, 6, 4),
            ("CA des 30 derniers jours", 4, 0, 16, 8),
            ("Top 5 vendeurs du dernier jour", 4, 16, 8, 8),
        ],
    },
    "Top Sellers": {
        "description": "Classement des vendeurs, évolution du top 3 et vendeurs inactifs (> 7 jours sans vente).",
        "cards": [
            ("Top 10 vendeurs du mois", 0, 0, 12, 9),
            ("CA des 3 meilleurs vendeurs sur 30 jours", 0, 12, 12, 9),
            ("Vendeurs inactifs depuis plus de 7 jours", 9, 0, 24, 8),
        ],
    },
    "Commissions": {
        "description": "Finance : commissions Maelys (12 % des commandes completed) pour la clôture et la facturation.",
        "cards": [
            ("Commission du mois", 0, 0, 8, 4),
            ("CA commissionné du mois", 0, 8, 8, 4),
            ("CA annulé ou remboursé du mois (non commissionné)", 0, 16, 8, 4),
            ("Commissions par jour (30 jours)", 4, 0, 24, 7),
            ("Récapitulatif des commissions par jour (mois)", 11, 0, 12, 9),
            ("Commissions à facturer par vendeur (mois)", 11, 12, 12, 9),
        ],
    },
    "Catégories": {
        "description": "Catalogue : répartition et évolution du CA par catégorie (commandes completed).",
        "cards": [
            ("Répartition du CA par catégorie (30 jours)", 0, 0, 10, 9),
            ("CA hebdomadaire par catégorie", 0, 10, 14, 9),
            ("Indicateurs par catégorie (30 jours)", 9, 0, 24, 7),
        ],
    },
    "Anomalies": {
        "description": "Sorties du DAG marketplace_anomaly_detect_daily : chutes de CA (global, vendeurs) et prix suspects.",
        "cards": [
            ("Anomalies détectées par jour (30 jours)", 0, 0, 24, 6),
            ("Chutes de CA (< 70 % de la moyenne 7 jours)", 6, 0, 12, 9),
            ("Prix suspects (hors 0,2× – 3× le prix catalogue)", 6, 12, 12, 9),
        ],
    },
}


def load_env() -> dict[str, str]:
    env = {}
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.strip() and not line.lstrip().startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                env[key.strip()] = value.strip()
    env.update({k: v for k, v in os.environ.items() if k.startswith(("MB_", "DWH_"))})
    return env


class Metabase:
    def __init__(self, url: str):
        self.url = url.rstrip("/")
        self.session: str | None = None

    def call(self, method: str, path: str, payload: dict | None = None):
        data = json.dumps(payload).encode() if payload is not None else None
        request = urllib.request.Request(f"{self.url}/api/{path}", data=data, method=method)
        request.add_header("Content-Type", "application/json")
        if self.session:
            request.add_header("X-Metabase-Session", self.session)
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                body = response.read()
        except urllib.error.HTTPError as exc:
            raise SystemExit(f"{method} /api/{path} -> {exc.code} : {exc.read().decode()[:500]}") from None
        return json.loads(body) if body else None


def wait_until_ready(mb: Metabase, timeout: int = 300) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if mb.call("GET", "health").get("status") == "ok":
                return
        except (urllib.error.URLError, ConnectionError, SystemExit):
            pass
        time.sleep(5)
    raise SystemExit("Metabase ne répond pas sur /api/health")


def ensure_admin(mb: Metabase, email: str, password: str) -> None:
    props = mb.call("GET", "session/properties")
    if not props.get("has-user-setup"):
        print(f"Création du compte admin {email}")
        mb.call("POST", "setup", {
            "token": props["setup-token"],
            "user": {"first_name": "Admin", "last_name": "Maelys", "email": email,
                     "password": password, "password_confirm": password},
            "prefs": {"site_name": "Maelys Marketplace", "site_locale": "fr", "allow_tracking": False},
        })
    mb.session = mb.call("POST", "session", {"username": email, "password": password})["id"]


def ensure_settings(mb: Metabase) -> None:
    """Formats français : 1 234,56 €, dates JJ/MM/AAAA."""
    mb.call("PUT", "setting/site-locale", {"value": "fr"})
    mb.call("PUT", "setting/custom-formatting", {"value": {
        "type/Number": {"number_separators": ", "},
        "type/Currency": {"currency": "EUR", "currency_style": "symbol"},
        "type/Temporal": {"date_style": "D/M/YYYY", "date_separator": "/"},
    }})


def ensure_database(mb: Metabase, env: dict[str, str]) -> int:
    details = {
        "host": "postgres-dwh", "port": 5432, "dbname": env.get("DWH_DB", "dwh"),
        "user": env["DWH_USER"], "password": env["DWH_PASSWORD"], "ssl": False,
    }
    databases = mb.call("GET", "database")
    databases = databases.get("data", databases) if isinstance(databases, dict) else databases
    for db in databases:
        if db["name"] == DATABASE:
            mb.call("PUT", f"database/{db['id']}", {"details": details})
            return db["id"]
    print(f"Ajout de la source {DATABASE} (postgres-dwh:5432)")
    return mb.call("POST", "database", {"engine": "postgres", "name": DATABASE, "details": details})["id"]


def ensure_collection(mb: Metabase) -> int:
    for collection in mb.call("GET", "collection"):
        if collection.get("name") == COLLECTION and not collection.get("archived"):
            return collection["id"]
    return mb.call("POST", "collection", {"name": COLLECTION, "parent_id": None})["id"]


def collection_items(mb: Metabase, collection_id: int, model: str) -> dict[str, int]:
    items = mb.call("GET", f"collection/{collection_id}/items?models={model}")
    items = items.get("data", items) if isinstance(items, dict) else items
    return {item["name"]: item["id"] for item in items}


def upsert_cards(mb: Metabase, database_id: int, collection_id: int) -> dict[str, int]:
    existing = collection_items(mb, collection_id, "card")
    ids = {}
    for name, spec in CARDS.items():
        payload = {
            "name": name,
            "display": spec["display"],
            "collection_id": collection_id,
            "visualization_settings": spec["viz"],
            "dataset_query": {
                "type": "native",
                "database": database_id,
                "native": {"query": spec["sql"].strip(), "template-tags": {}},
            },
        }
        if name in existing:
            ids[name] = mb.call("PUT", f"card/{existing[name]}", payload)["id"]
        else:
            ids[name] = mb.call("POST", "card", payload)["id"]
        print(f"  question : {name} (#{ids[name]})")
    return ids


def upsert_dashboards(mb: Metabase, collection_id: int, card_ids: dict[str, int]) -> None:
    existing = collection_items(mb, collection_id, "dashboard")
    for name, spec in DASHBOARDS.items():
        dashboard_id = existing.get(name) or mb.call(
            "POST", "dashboard", {"name": name, "collection_id": collection_id}
        )["id"]
        dashcards = [
            {"id": -(i + 1), "card_id": card_ids[card], "row": row, "col": col, "size_x": w, "size_y": h,
             "parameter_mappings": [], "visualization_settings": {}}
            for i, (card, row, col, w, h) in enumerate(spec["cards"])
        ]
        mb.call("PUT", f"dashboard/{dashboard_id}", {"description": spec["description"], "dashcards": dashcards})
        print(f"Dashboard « {name} » : {mb.url}/dashboard/{dashboard_id}")


def main() -> None:
    env = load_env()
    mb = Metabase(env.get("MB_URL", "http://localhost:3000"))
    wait_until_ready(mb)
    ensure_admin(mb, env["MB_ADMIN_EMAIL"], env["MB_ADMIN_PASSWORD"])
    ensure_settings(mb)
    database_id = ensure_database(mb, env)
    collection_id = ensure_collection(mb)
    card_ids = upsert_cards(mb, database_id, collection_id)
    upsert_dashboards(mb, collection_id, card_ids)


if __name__ == "__main__":
    sys.exit(main())
