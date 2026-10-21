# Plan — Projet A : MarketPlace Analytics (Maelys Marketplace)

> Sources : *Cahier des charges v3.0 (avril 2026)* et *Pitch*. En cas de divergence, **le cahier des charges fait foi** (plus récent). Voir §9.

## 0. Objectif en une phrase

En 2 jours, livrer un MVP data **reproductible (Docker Compose)** : API backoffice → Airflow 3.1.8 (Custom Hook) → MinIO (raw JSON `dt=`) → PostgreSQL (`staging` / `dwh` / `analytics`) → Metabase (≥ 2 dashboards), avec **idempotence prouvée** et **≥ 3 tests pytest**.

**Règle d'or : MVP à 100 % avant tout bonus.**

---

## 1. Périmètre priorisé

| Priorité | Contenu | Points |
|---|---|---|
| 🟠 **Must (MVP)** | `MarketplaceAPIHook` (`get_orders`, `get_sellers`, `get_products`) · DAG ingest extract → MinIO → `staging.orders` → `dwh.fact_orders` (DELETE + INSERT par `dt`) · ≥ 3 dims + 1 fait (PK/FK) + ≥ 1 table `analytics` · 2 dashboards Metabase (Executive Summary, Top Sellers) · ≥ 3 tests pytest (dont idempotence) · README avec Mermaid | socle |
| 🟡 Should | `DataQualityOperator` (3 règles : not null, not empty, pas de date future) · branching DQ KO → alerte · 2 dashboards en plus (Commissions, Catégories) | +5 |
| 🔵 Could | DAG anomaly detection (moyenne mobile 7j, CA < 70 %) · Prometheus + Grafana + exporters · backfill 7 jours documenté · tests avec mock DB | +10 / +5 |
| 🔴 Won't | SCD2, CI/CD, cloud, dbt, Spark | — |

---

## 2. Arborescence cible du repo

```
projet-final-marketplace/
├── docker-compose.yaml          # Airflow (Celery) + pg-airflow + pg-dwh + redis + minio(+init) + api + metabase [+ obs]
├── .env.example                 # creds DWH, MinIO, AIRFLOW_CONN_* (jamais de secret en dur)
├── api/                         # API Flask simulée (si non fournie par le kit)
├── init-db/
│   └── schema.sql               # CREATE SCHEMA staging/dwh/analytics + tables + dim_date pré-remplie
├── dags/
│   ├── marketplace_dims_refresh_daily.py
│   ├── marketplace_orders_ingest_daily.py
│   ├── marketplace_analytics_aggregate_daily.py
│   ├── marketplace_anomaly_detect_daily.py      # bonus
│   └── sql/                     # requêtes .sql templatisées ({{ ds }})
├── plugins/
│   ├── hooks/marketplace_api_hook.py
│   └── operators/data_quality_operator.py       # bonus
├── tests/
│   ├── conftest.py              # fixture dagbag
│   ├── test_dags.py             # import, catchup=False, structure + DELETE/INSERT
│   ├── test_marketplace_hook.py # mock requests (auth, retry 5xx/timeout)
│   └── test_data_quality.py     # bonus, mock PostgresHook
├── observability/               # bonus : prometheus.yml, statsd_mapping.yml
├── docs/                        # captures dashboards, schéma ER
└── README.md
```

---

## 3. Modèle de données (Kimball, étoile)

**Grain de `fact_orders`** : à confirmer avec `curl /orders` (1 ligne = 1 commande ou 1 ligne de commande). Si une commande contient plusieurs produits → grain = ligne de commande, PK `(order_id, line_id)`.

```
staging.orders        (order_id, dt, seller_id, product_id, customer_id, quantity, unit_price, total, status, raw_loaded_at)
staging.sellers / staging.products / staging.customers   (copie typée de l'API)

dwh.dim_date          (dt PK, year, month, day, day_of_week, week, is_weekend)       -- generate_series 2025→2027
dwh.dim_seller        (seller_id PK, name, country, joined_date)                      -- SCD1 (UPSERT)
dwh.dim_product       (product_id PK, name, category, base_price, seller_id)          -- SCD1
dwh.dim_customer      (customer_id PK, email, city, signup_date)                      -- si /customers exploitable
dwh.fact_orders       (order_id PK, dt FK, seller_id FK, product_id FK, customer_id FK,
                       quantity, total_amount, commission_amount = total × 0.12, status)
                       INDEX (dt), (seller_id)

analytics.daily_summary   (dt PK, total_orders, total_revenue, total_commission, avg_basket, top_seller_id)
analytics.seller_daily    (dt, seller_id, orders, revenue, commission)      PK (dt, seller_id)
analytics.category_daily  (dt, category, orders, revenue)                   PK (dt, category)
analytics.anomalies       (dt, scope, entity_id, value, avg_7d, ratio, detected_at)   -- bonus
```

**Points d'attention**
- **FK & ordre de chargement** : une commande peut référencer un vendeur/produit absent des dims → refresh des dims *avant* le load des faits (tâche en amont dans le DAG ingest, ou membre « inconnu » `-1`). À décider après inspection de l'API.
- Commission : 12 % du `total_amount` hors annulations (filtrer `status` si présent) — à valider.
- Montants en `NUMERIC(12,2)`, jamais `FLOAT`.

---

## 4. Composants techniques

### 4.1 `MarketplaceAPIHook` (`plugins/hooks/`)
- Hérite de `BaseHook` (Airflow 3 : `airflow.sdk.bases.hook`), `conn_name_attr = "marketplace_api_conn_id"`, défaut `marketplace_api`.
- Lit `host` (URL) et `password` (token Bearer) depuis la Connection → déclarée via `AIRFLOW_CONN_MARKETPLACE_API` dans `.env`.
- `requests.Session` + `HTTPAdapter(Retry(total=3, backoff_factor=1, status_forcelist=[500,502,503,504]))`, `timeout=(5, 30)`, `raise_for_status()`.
- Méthodes : `get_orders(date)`, `get_sellers(limit=None)`, `get_products()`, `get_customers(limit=None)`, `health()`.
- Testable : session injectable / `requests` mocké.

### 4.2 DAG `marketplace_orders_ingest_daily` (`@daily`, `catchup=False`, `max_active_runs=1`)
```
extract_orders (Hook) ─► upload_raw_minio  s3://data-lake/raw/orders/dt={{ ds }}/data.json   (S3Hook, replace=True)
                    ─► load_staging       DELETE staging.orders WHERE dt={{ ds }} ; INSERT (lecture depuis MinIO)
                    ─► [bonus] data_quality ─► branch ─┬─► transform_dwh   BEGIN; DELETE fact WHERE dt; INSERT … SELECT; COMMIT
                                                      └─► alert_dq_failed
                    ─► transform_dwh  (outlets=[Asset dwh.fact_orders])
```
- Le load staging lit **depuis MinIO** (pas depuis XCom) → rejouable sans rappeler l'API, et pas de gros payload en XCom.
- Toutes les dates via `{{ ds }}` / `logical_date`, jamais en dur.
- Connections : `marketplace_api`, `postgres_dwh`, `minio_s3` (type `aws`, `endpoint_url` en extra).

### 4.3 DAG `marketplace_dims_refresh_daily` (`@daily`)
extract sellers/products(/customers) → raw MinIO `raw/<entity>/dt=` → staging → `INSERT … ON CONFLICT (pk) DO UPDATE` dans les dims. *Sacrifiable si retard* (dims chargées alors dans le DAG ingest).

### 4.4 DAG `marketplace_analytics_aggregate_daily` (schedule = Asset `dwh.fact_orders`)
DELETE + INSERT par `dt` dans `daily_summary`, `seller_daily`, `category_daily`.

### 4.5 Bonus
- `DataQualityOperator(conn_id, table, checks=[...], partition="{{ ds }}")` → règles `not_null(col)`, `not_empty`, `no_future_date(col)` ; renvoie le résultat pour un `@task.branch`.
- `marketplace_anomaly_detect_daily` : CA du jour vs moyenne 7j (global + par vendeur) ; si < 70 % → `analytics.anomalies` + branche alerte (log / webhook simulé).
- Observabilité : `statsd-exporter`, `postgres-exporter`, `prometheus`, `grafana:3001` ; dashboards 11010 et 9628.

---

## 5. Metabase

- Service `metabase/metabase:v0.59.6.1`, volume nommé `metabase-data` (H2), port 3000.
- Source : host `postgres-dwh`, port **5432** (interne), base `dwh`.
- **Dashboard 1 — Executive Summary** : Big Number CA du jour · Line CA 30 j · Bar top 5 vendeurs du jour.
- **Dashboard 2 — Top Sellers** : Bar top 10 du mois · Line CA top 3 sur 30 j · Table vendeurs inactifs > 7 j.
- Bonus : **Commissions** (commission/jour, cumul mensuel) · **Catégories** (CA par catégorie, part %).
- Requêtes SQL des questions sauvegardées dans `docs/metabase/` + captures d'écran (filet de sécurité si le volume est perdu).
- ⚠️ Prévoir un **backfill ~30 jours** pour que les graphes 30 j aient des données.

---

## 6. Tests (`docker compose exec airflow-worker pytest tests/ -v`)

| Test | Vérifie |
|---|---|
| `test_no_import_errors` | `DagBag.import_errors == {}` |
| `test_all_dags_catchup_false` | tous les DAGs `catchup=False` |
| `test_marketplace_ingest_has_idempotent_transform` | la tâche transform contient `DELETE … WHERE dt` avant `INSERT`, et tâches attendues présentes |
| `test_hook_sends_bearer_token` | header `Authorization` construit depuis la Connection (mock) |
| `test_hook_retries_on_5xx` | retry déclenché sur 503 |
| (bonus) `test_dq_operator_*` | règles OK/KO avec `PostgresHook` mocké |

**Test manuel d'idempotence** (celui du formateur) : `airflow dags test marketplace_orders_ingest_daily 2026-04-07` ×3 → `SELECT COUNT(*) … WHERE dt='2026-04-07'` identique. Résultats collés dans le README.

---

## 7. Planning

### Jour 1 — Pipeline ELT bout en bout
| Créneau | Tâche | Done quand… |
|---|---|---|
| 1h | Récupérer le kit, `git init`, `.env`, `docker compose up -d`, tous services *healthy* | UI Airflow/MinIO accessibles, bucket `data-lake` créé |
| 0h30 | `curl` sur tous les endpoints, noter le format JSON (grain, champs, statuts) | format documenté dans `docs/api.md` |
| 1h30 | Modèle sur papier → `init-db/schema.sql`, passé à la main via `psql` | schémas + tables créés sans erreur |
| 1h30 | `MarketplaceAPIHook` + script de test isolé + test pytest du Hook | appel OK depuis le worker |
| 2h | DAG ingest **tâche par tâche** : extract → MinIO → staging | JSON visible dans MinIO, lignes dans `staging.orders` |
| 1h | Dims (dans le DAG ingest ou DAG dédié) + transform idempotent → `fact_orders` | 3 runs = même COUNT |
| 0h30 | 3 tests pytest de base verts, **premier commit** | ✅ checkpoint J1 |

### Jour 2 — Analytics, dashboards, qualité du livrable
| Créneau | Tâche | Done quand… |
|---|---|---|
| 1h | Backfill ~30 jours (`airflow dags backfill` ou boucle `dags test`) | données sur 30 dates |
| 1h30 | DAG analytics (Asset) + tables `analytics.*` | agrégats remplis et cohérents avec `fact_orders` |
| 2h | Metabase : service, connexion, 2 dashboards MVP | ✅ MVP complet |
| 1h | README (Mermaid archi + ER, lancement, choix, idempotence, limites) + captures | DoD cochée |
| reste | Bonus dans l'ordre : DQ Operator + branching → 2 dashboards bonus → anomaly DAG → Prometheus/Grafana | — |

**Si retard** : sacrifier d'abord le DAG `dims_refresh` séparé (dims chargées dans le DAG ingest), puis tout bonus. Minimum absolu acceptable : 1 dashboard avec 3 visualisations.

---

## 8. Definition of Done (checklist finale)

- [ ] `marketplace_orders_ingest_daily` en success
- [ ] `dwh.fact_orders` contient des données (≥ 1 date, idéalement 30)
- [ ] Rejouer 2–3 fois la même date → même `COUNT(*)`
- [ ] Metabase : ≥ 2 dashboards avec données réelles, volume persistant
- [ ] ≥ 3 tests pytest verts
- [ ] README : Mermaid, lancement, choix techniques
- [ ] Aucun token/mot de passe en dur (Connections + `.env`, `.env` dans `.gitignore`)
- [ ] `docker compose down -v && up -d` → tout se reconstruit (hors dashboards Metabase H2, documenté)

---

## 9. Divergences Pitch ↔ Cahier & questions ouvertes

| Sujet | Pitch | Cahier v3.0 (retenu) |
|---|---|---|
| PostgreSQL | 16 | **18** |
| Metabase | `latest`, 4 dashboards | **`v0.59.6.1`**, 2 MVP + 2 bonus |
| DQ Operator | 5 règles, J1 obligatoire | **3 règles, bonus Should** |
| Build DWH | DAG séparé `marketplace_dwh_build_daily` (Asset) | **dans le DAG ingest** ; DAG `dims_refresh` séparé |
| Streamlit | option 2 | Metabase uniquement (Superset toléré) |

**À clarifier avant de coder**
1. Le **kit** (docker-compose, API Flask simulée, `Documentation commune.html`) est-il disponible ? Le répertoire est vide pour l'instant — sinon il faut écrire l'API Flask déterministe (`seed = md5(date)`) nous-mêmes.
2. Travail **seul ou en groupe** ? (répartition possible : Hook/DAG · modèle/SQL/analytics · Metabase/tests/README)
3. Format exact de `/orders` (multi-produits par commande ? `status` annulé/remboursé ?) → impacte grain et calcul commission.
