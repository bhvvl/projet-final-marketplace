# Maelys Marketplace — MarketPlace Analytics

MVP de plateforme data : API backoffice → Airflow 3.1.8 → MinIO (raw) → PostgreSQL 18 (`staging` / `dwh` / `analytics`) → Metabase.

Plan de travail : [PLAN.md](PLAN.md). Démarrage rapide : voir [Lancement](#lancement) (4 commandes depuis un clone neuf).

## Architecture

```mermaid
flowchart LR
    api[("API Marketplace<br/>Flask simulée")] --> hook["MarketplaceAPIHook"]
    hook --> airflow[["Airflow 3.1.8"]]
    airflow -- "raw JSON dt=" --> minio[("MinIO<br/>data-lake")]
    airflow -- "load + transform" --> pg[("PostgreSQL 18<br/>staging / dwh / analytics")]
    pg --> mb["Metabase"]
    airflow -. "StatsD" .-> prom[("Prometheus")]
    pg -. "postgres-exporter" .-> prom
    prom --> graf["Grafana"]
```

## Modèle de données

Fichier : [init-db/schema.sql](init-db/schema.sql) (ré-exécutable). Trois schémas :

- `staging` : copie typée de l'API (`orders` par `dt`, référentiels `sellers` / `products` / `customers`).
- `dwh` : étoile Kimball. Grain de `fact_orders` = 1 commande (l'API vend 1 produit par commande).
- `analytics` : agrégats pour Metabase (`daily_summary`, `seller_daily`, `category_daily`, `anomalies`).

```mermaid
erDiagram
    DIM_DATE ||--o{ FACT_ORDERS : "dt"
    DIM_SELLER ||--o{ FACT_ORDERS : "seller_id"
    DIM_PRODUCT ||--o{ FACT_ORDERS : "product_id"
    DIM_CUSTOMER ||--o{ FACT_ORDERS : "customer_id"
    DIM_SELLER ||--o{ DIM_PRODUCT : "seller_id"
    DIM_DATE { date dt PK
        smallint year
        smallint month
        smallint day_of_week
        boolean is_weekend }
    DIM_SELLER { text seller_id PK
        text name
        text seller_type
        text country
        date joined_date }
    DIM_PRODUCT { text product_id PK
        text name
        text category
        text seller_id FK
        numeric base_price }
    DIM_CUSTOMER { text customer_id PK
        text email
        text city
        date signup_date }
    FACT_ORDERS { text order_id PK
        date dt FK
        text seller_id FK
        text product_id FK
        text customer_id FK
        int quantity
        numeric total_amount
        numeric commission_amount
        text status }
```

Choix :

- **Dimensions SCD1** (UPSERT) : l'historique des attributs n'est pas demandé (SCD2 hors périmètre).
- **Membre `UNKNOWN`** dans chaque dimension : un `customer_id` nul ou une clé inconnue est rattaché à `UNKNOWN`, les FOREIGN KEY de `fact_orders` restent toujours valides.
- **Commission** = 12 % de `total_amount` pour les commandes `completed`, 0 sinon. Le taux est stocké (`commission_rate`) pour tracer un éventuel changement.
- **CA** (`revenue`) = commandes `completed` uniquement.
- `dim_date` pré-remplie de 2020 à 2030 par `generate_series`.

## MarketplaceAPIHook

[plugins/hooks/marketplace_api_hook.py](plugins/hooks/marketplace_api_hook.py) : hérite de `BaseHook` et lit la Connection `marketplace_api` (`host` = URL, `password` = token Bearer).

- `get_orders(day)` (`str`, `date` ou `datetime`), `get_sellers(limit)`, `get_products()`, `get_customers(limit)`, `health()`, `test_connection()`.
- Retry avec backoff exponentiel sur timeout, connexion refusée et 429/5xx (3 retries par défaut). 400/401 échouent immédiatement.
- Tests : [tests/test_marketplace_api_hook.py](tests/test_marketplace_api_hook.py) — serveur HTTP local scripté pour tester le retry réellement, plus un test de déterminisme contre l'API du compose.

## DAG `marketplace_orders_ingest_daily`

[dags/marketplace_orders_ingest_daily.py](dags/marketplace_orders_ingest_daily.py)

```mermaid
flowchart LR
    eo["extract_orders_to_raw<br/>API → MinIO raw/orders/dt="] --> lo["load_orders_staging<br/>MinIO → staging.orders"]
    er["extract_referentials_to_raw<br/>API → MinIO raw/sellers|products|customers/dt="] --> ld["load_dims<br/>staging + UPSERT dims"]
    lo --> dq["dq_staging_orders<br/>DataQualityOperator"]
    dq --> br{"check_quality"}
    br -- "KO" --> alert["alert_dq_failed<br/>webhook + run en échec"]
    br -- "OK" --> tf["transform_fact_orders<br/>DELETE + INSERT dt = ds"]
    ld --> tf
    tf --> pub["publish_fact_orders<br/>Asset fact_orders, extra {dt}"]
    pub -. "déclenche" .-> aval["analytics + anomalies"]
```

- **Planification** : `CronDataIntervalTimetable("@daily")`. En Airflow 3, `schedule="@daily"` seul donne `ds` = jour du run (journée incomplète). Avec un data interval, le run de J traite la journée complète J-1.
- **Raw d'abord** : les chargements relisent le JSON archivé dans MinIO, pas l'API ni les XCom. Une transformation corrigée se rejoue sans rappeler la source, et aucun gros volume ne passe par les XCom.
- **Idempotence** :
  - raw : la partition `dt=` est écrasée ;
  - `staging.orders` et `dwh.fact_orders` : `DELETE` + `INSERT` de la partition dans une seule transaction ;
  - dimensions : UPSERT.
- **Contrôle** : `load_orders_staging` échoue si le nombre de lignes chargées diffère du nombre extrait.
- **Qualité (bonus)** : voir [Qualité de données et branching](#qualité-de-données-et-branching-bonus).
- `retries=2`, `max_active_runs=1`, `catchup=False`.
- SQL : [dags/sql/](dags/sql/). Code partagé : [dags/marketplace/common.py](dags/marketplace/common.py) (exclus du parsing par `.airflowignore`).

### Preuve d'idempotence (2026-04-07)

Trois `airflow dags test marketplace_orders_ingest_daily 2026-04-07` successifs, en succès, donnent un contenu strictement identique ([docs/idempotence-2026-04-07.txt](docs/idempotence-2026-04-07.txt)) :

| Run | `fact_orders` | `staging.orders` | `dim_product` | Somme `total_amount` | MD5 du contenu |
|---|---|---|---|---|---|
| 1 | 1 011 | 1 011 | 3 001 | 143 426,40 | `04b5d5f2ac91f435294c9c376ed2a97c` |
| 2 | 1 011 | 1 011 | 3 001 | 143 426,40 | `04b5d5f2ac91f435294c9c376ed2a97c` |
| 3 | 1 011 | 1 011 | 3 001 | 143 426,40 | `04b5d5f2ac91f435294c9c376ed2a97c` |

> ⚠️ **`airflow dags test` exige le DAG en pause.** Sur un DAG actif, le scheduler prend aussi en charge le run créé par `dags test` et les deux se disputent les tâches (erreur `ServerResponseError` au démarrage des tâches). Pour rejouer une date sur un DAG actif, utiliser plutôt `airflow dags trigger marketplace_orders_ingest_daily --logical-date 2026-04-07T00:00:00+00:00`.

## Qualité de données et branching (bonus)

[plugins/operators/data_quality_operator.py](plugins/operators/data_quality_operator.py) : `DataQualityOperator`, des règles SQL configurables qui comptent chacune les lignes en violation.

| Règle | Configuration | Échoue si |
|---|---|---|
| `not_empty` | — | la partition est vide |
| `not_null` | `columns: [...]` | une des colonnes est nulle |
| `no_future_date` | `column` | une date est postérieure à maintenant |

- Une configuration invalide (règle inconnue, paramètre manquant, nom de colonne suspect) fait échouer l'import du DAG, pas l'exécution.
- Le rapport `{"passed": …, "checks": [...]}` est renvoyé en XCom ; `fail_on_error=True` fait échouer la tâche à la place.
- Dans le DAG d'ingestion, il contrôle `staging.orders` sur `dt = '{{ ds }}'` :
  - **OK** : `check_quality` oriente vers `transform_fact_orders` ;
  - **KO** : `alert_dq_failed` envoie une alerte au webhook et fait échouer le run, sans retry. `fact_orders` n'est pas touché et aucun événement d'Asset n'est émis.
- **Vérifié** : le 20/10 passe par le chemin OK. Une date future (25/10, liste vide côté API) passe par le chemin KO : `not_empty` échoue, l'alerte arrive au webhook et le run échoue.
- `customer_id` n'est pas contrôlé : il est nul pour environ 0,2 % des commandes, rattachées au membre `UNKNOWN`.

**Webhook d'alerte** : Connection `alert_webhook` (`host` = URL, `password` = token Bearer). En local, c'est l'API simulée qui reçoit les alertes (`POST /webhook`) ; on les relit avec :

```bash
curl -H "Authorization: Bearer formation-token-2026" http://localhost:5000/webhook/alerts
```

Passer à Slack ou Teams ne demande que de changer la Connection.

## Alerte en cas d'échec de DAG (US-07)

Les 3 DAGs déclarent `on_failure_callback=notify_dag_failure` ([dags/marketplace/common.py](dags/marketplace/common.py)) : chaque run en échec envoie une alerte `dag_failure` au même webhook.

```json
{"kind": "dag_failure",
 "message": "Échec du DAG marketplace_orders_ingest_daily (run manual__2026-10-21T13:20:05…)",
 "details": {"dag_id": "marketplace_orders_ingest_daily", "run_id": "manual__…",
             "logical_date": "2026-08-29T00:00:00+00:00", "reason": "task_failure",
             "url": "http://localhost:8080/dags/marketplace_orders_ingest_daily/runs/manual__…"}}
```

- **Où il s'exécute** : en Airflow 3, le callback d'un DAG tourne dans le dag-processor, hors des tâches. Il ne lève donc jamais d'exception : une panne du webhook est seulement journalisée, sinon l'alerte serait perdue sans bruit.
- **Pas de nom de tâche** : le contexte transmis par Airflow 3.1 est celui de la *dernière* tâche du run, pas forcément celle qui a échoué. L'alerte donne à la place le lien vers le run dans l'UI.
- **Vérifié en conditions réelles** :
  - l'API est mise en panne avec `API_FAILURE_RATE=1 docker compose up -d api-marketplace` ;
  - un run pour le 29/08 est déclenché ;
  - l'extraction échoue après ses 3 tentatives (retry du Hook sur 503, puis `retries=2` d'Airflow) ;
  - l'alerte arrive à la seconde où le run échoue.
- **Doublon côté qualité** : un contrôle qualité KO produit deux alertes, `data_quality` (le détail des règles) puis `dag_failure` (l'échec du run).
- Les pannes simulées de l'API (`API_FAILURE_RATE`) épargnent `/webhook`, qui représente un service d'alerte distinct.

## DAG `marketplace_analytics_aggregate_daily`

[dags/marketplace_analytics_aggregate_daily.py](dags/marketplace_analytics_aggregate_daily.py) : `resolve_dates`, puis `build_daily_summary`, `build_seller_daily` et `build_category_daily` en parallèle, puis `mark_refreshed`.

- **Déclenché par l'Asset** `fact_orders` (`schedule=[FACT_ORDERS_ASSET]`) : chaque ingestion réussie relance les agrégats. Le DAG est actif dès sa création.
- **Quelle date recalculer ?** Elle est déduite de **l'état des données**, pas seulement des événements :
  - **Pourquoi** : en Airflow 3, un run déclenché par Asset n'a pas de `logical_date`. Et pendant le test de reproductibilité, Airflow a **perdu** deux événements émis à quelques millisecondes d'intervalle (08/09 et 10/09, absents des agrégats).
  - **Comment** : `analytics.refresh_log` enregistre, pour chaque DAG aval et chaque date, la version des faits utilisée (`max(fact_orders.loaded_at)`). Une date est recalculée dès que ses faits ont été rechargés depuis ([dags/marketplace/common.py](dags/marketplace/common.py), `dates_to_process`).
  - **Conséquence** : un événement perdu est rattrapé au run suivant, et un déclenchement manuel rattrape tout ce qui est en retard.
  - **Recalcul forcé** : `airflow dags trigger marketplace_analytics_aggregate_daily --conf '{"full_refresh": true}'`.
  - L'ingestion publie toujours la date traitée dans l'`extra` de l'événement (`{"dt": …}`), mais seulement pour information dans les logs.
- **Vérifié** : un premier déclenchement traite 51 dates, un second n'en traite aucune, et un rechargement du 08/09 sans événement est rattrapé seul.
- `DELETE` + `INSERT` par date dans une transaction ([dags/sql/analytics_*.sql](dags/sql/)) : idempotent.
- **Réconciliation** : le CA de `fact_orders` (commandes `completed`) est égal au centime près à celui de `daily_summary`, `seller_daily` et `category_daily`, ce que vérifie un test.

## DAG `marketplace_anomaly_detect_daily` (bonus)

[dags/marketplace_anomaly_detect_daily.py](dags/marketplace_anomaly_detect_daily.py) : `resolve_dates`, puis `detect_anomalies`, puis `check_anomalies`, qui oriente vers `notify_anomalies` (webhook) ou `no_anomaly`.

| Type (`scope`) | Règle (seuils dans `params`) |
|---|---|
| `global` | CA du jour < 70 % de la moyenne des 7 jours précédents (US-05) |
| `seller` | même règle par vendeur, pour les vendeurs dont la moyenne 7 jours dépasse 1 500 € (sinon trop de bruit) |
| `price` | prix unitaire > 3× ou < 0,2× le prix catalogue |

- Résultats dans `analytics.anomalies`, en `DELETE` + `INSERT` par date ([dags/sql/anomaly_detect.sql](dags/sql/anomaly_detect.sql)).
- Calculé sur `fact_orders`, pas sur `analytics.*` : aucune dépendance à l'ordre des DAGs déclenchés par le même Asset.
- Il faut au moins 4 jours chargés parmi les 7 précédents pour conclure.
- **Planification** : par l'Asset `fact_orders`, et non `@daily` comme suggéré dans le cahier des charges. La détection part dès que les données du jour sont chargées, sans risquer de tourner avant elles.
- **Notification** : seule la date la plus récente traitée est notifiée, pour éviter une rafale d'alertes pendant un backfill.
- **Résultats sur les 51 jours chargés** :
  - les 2 journées de chute simulées par l'API sont détectées (05/09 à 62 % de la moyenne, 14/09 à 47 %) ;
  - 206 prix suspects, environ 0,4 % des commandes, soit le taux de défauts injecté dans l'API ;
  - 133 chutes de CA vendeur.
- **Dates à recalculer** : même mécanisme que pour les agrégats, avec une fenêtre de 7 jours. Une date est recalculée si ses faits **ou ceux des 7 jours précédents** (sa référence) ont changé. Après un backfill, les dates voisines sont donc recalculées automatiquement. Exemple : recharger le 08/09 relance la détection du 08/09 au 15/09.

## Backfill (rejouer des dates passées)

Le plus simple : [scripts/load_demo_data.sh](scripts/load_demo_data.sh) (`./scripts/load_demo_data.sh [DEBUT] [FIN]`, par défaut du 01/09 au 20/10/2026). Il est ré-exécutable :
1. il active le DAG d'ingestion et lance le backfill (5 runs en parallèle). Les dates déjà chargées sont ignorées ; les runs **en échec sont rejoués**, donc relancer le script répare un backfill interrompu ;
2. il remet le DAG d'ingestion en pause ;
3. il déclenche une dernière fois les DAGs aval pour rattraper les dates en retard ;
4. il configure Metabase.

À la main, c'est un backfill Airflow 3 exécuté par le scheduler (le DAG doit être actif) :

```bash
docker compose exec airflow-worker airflow backfill create --dag-id marketplace_orders_ingest_daily \
  --from-date 2026-09-01 --to-date 2026-10-20 --reprocess-behavior failed --dry-run   # puis sans --dry-run
```

Résultat : 50 runs en succès. `fact_orders` contient 52 936 commandes sur 51 dates (avec le 07/04), pour 6 979 609,21 € de CA. Les chiffres sont identiques avant et après une remise à zéro complète, puisque l'API est déterministe.

> ⚠️ Docker Desktop est suspendu quand le Mac se met en veille (capot fermé). Les tâches en cours échouent alors au réveil (`Invalid auth token: Signature has expired`, `dagrun_timeout`). Il suffit de relancer `./scripts/load_demo_data.sh`.

## Metabase

http://localhost:3000. Identifiants admin : `MB_ADMIN_EMAIL` / `MB_ADMIN_PASSWORD` du `.env`.

La configuration est entièrement scriptée et ré-exécutable ([metabase/setup_dashboards.py](metabase/setup_dashboards.py), bibliothèque standard uniquement) :

```bash
python3 metabase/setup_dashboards.py
```

Le script :
- crée le compte admin au premier lancement ;
- déclare la source `postgres-dwh:5432` (nom Docker et port interne) ;
- applique les formats français ;
- crée ou met à jour, par leur nom, 21 questions SQL et 5 dashboards dans la collection « Maelys Marketplace ».

À relancer après un `docker compose down -v`, qui efface la base H2 de Metabase.

Les KPIs sont calculés par rapport au **dernier jour chargé** (`max(dt)`), pas à la date système : les dashboards restent pleins quelle que soit la date de démonstration.

| Dashboard | Contenu |
|---|---|
| **Executive Summary** | CA du dernier jour avec variation par rapport à la veille, commandes, panier moyen, commission Maelys · CA des 30 derniers jours · Top 5 vendeurs du dernier jour |
| **Top Sellers** | Top 10 vendeurs du mois · CA des 3 meilleurs vendeurs sur 30 jours · Vendeurs sans vente depuis plus de 7 jours (ou jamais) |
| **Commissions** (bonus, Finance) | Commission et CA du mois · CA annulé ou remboursé (non commissionné) · Commissions par jour · Récapitulatif journalier du mois · Commissions à facturer par vendeur |
| **Catégories** (bonus, Catalogue) | Répartition du CA sur 30 jours · CA hebdomadaire empilé par catégorie (la semaine en cours est partielle) · Commandes, CA, panier moyen et part du CA par catégorie |
| **Anomalies** (bonus) | Anomalies par jour et par type · Chutes de CA (global et vendeurs) · Prix suspects |

![Executive Summary](docs/metabase/executive-summary.jpg)
![Top Sellers](docs/metabase/top-sellers.jpg)
![Commissions](docs/metabase/commissions.jpg)
![Catégories](docs/metabase/categories.jpg)
![Anomalies](docs/metabase/anomalies.jpg)

## Observabilité : Prometheus + Grafana (bonus)

```mermaid
flowchart LR
    af[["Airflow<br/>(StatsD)"]] -- "UDP 9125" --> sx["statsd-exporter<br/>:9102"]
    pg[("postgres-dwh")] --> px["postgres-exporter<br/>:9187"]
    sx --> prom[("Prometheus<br/>:9090")]
    px --> prom
    prom --> graf["Grafana<br/>:3001"]
```

| Service | URL | Rôle |
|---|---|---|
| Grafana | http://localhost:3001 (`admin` / `admin`, à changer hors formation) | Dashboards, chargés automatiquement au démarrage |
| Prometheus | http://localhost:9090 | Collecte des métriques (cibles : `airflow`, `postgres-dwh`, `prometheus`) |
| statsd-exporter | http://localhost:9102/metrics | Métriques StatsD d'Airflow converties au format Prometheus |
| postgres-exporter | http://localhost:9187/metrics | Métriques du Data Warehouse |

Les métriques Airflow sont activées dans `x-airflow-common` (`AIRFLOW__METRICS__STATSD_*`).

Tout est versionné et chargé au démarrage, sans import manuel à refaire :
- [observability/prometheus/prometheus.yml](observability/prometheus/prometheus.yml) : cibles de collecte ;
- [observability/grafana/provisioning/](observability/grafana/provisioning/) : source de données Prometheus et dossier de dashboards ;
- [observability/grafana/dashboards/](observability/grafana/dashboards/) : les JSON des dashboards ;
- [observability/statsd/mapping.yml](observability/statsd/mapping.yml) : une seule règle, voir plus bas.

**Dashboards importés** (dossier « Maelys - Supervision ») :

| Dashboard | ID grafana.com | Adaptation |
|---|---|---|
| **Airflow 3** | 24832 | Source de données fixée sur Prometheus. Unité des durées passée de ms à s, car statsd-exporter convertit les timers en secondes. `airflow.triggerer_heartbeat` est renommé `airflow_triggerer_heartbeat_total` (suffixe de compteur attendu). Les 16 panneaux ont des données. |
| **PostgreSQL Database** | 9628 | Aucune modification du JSON. Conçu pour Kubernetes, il filtre sur les labels `release` et `kubernetes_namespace` : Prometheus les ajoute de façon statique à la cible postgres-exporter. 31 panneaux sur 32 ont des données. |

> ⚠️ **L'ID 11010 indiqué dans le cahier des charges n'est pas un dashboard Airflow** : sur grafana.com, c'est « Kube Carbon » (empreinte carbone Kubernetes). Le « Airflow cluster dashboard » de databand (20788) attend des métriques renommées par leur propre configuration statsd, pensée pour Airflow 2. J'ai retenu **24832 « Airflow 3 »**, conçu pour Airflow 3.1 et compatible avec un statsd-exporter standard.

**Limites connues** :
- **Checkpoint Stats vide** : depuis PostgreSQL 17, les statistiques de checkpoint ont quitté `pg_stat_bgwriter` pour `pg_stat_checkpointer`, alors que le dashboard 9628 date d'avant.
- **Start Time** affiche N/A.

![Airflow 3](docs/grafana/airflow-3.jpg)
![PostgreSQL Database](docs/grafana/postgresql-database.jpg)

## Tests

```bash
docker compose exec airflow-worker pytest tests/ -v
```

44 tests. Ceux qui interrogent l'API ou le DWH du compose sont ignorés (`skip`) s'ils ne sont pas joignables ou vides.

| Fichier | Tests |
|---|---|
| [tests/test_dags.py](tests/test_dags.py) | Les 3 tests demandés : `test_no_import_errors`, `test_all_dags_catchup_false`, `test_marketplace_ingest_has_idempotent_transform` (DELETE avant INSERT sur `{{ ds }}`, aucune date en dur, une seule transaction). En plus : structure du DAG d'ingestion et branchement qualité ; `test_transform_is_idempotent_on_dwh`, qui joue 2 fois la transformation sur le DWH et compare les empreintes ; DAGs analytics et anomalies (planification par Asset, SQL idempotents, branchement de notification) ; choix des dates à recalculer, avec la base mockée puis sur le DWH (une date rechargée après son dernier calcul est détectée) ; alerte d'échec (callback présent sur les 3 DAGs, contenu de l'alerte, aucune exception si le webhook est en panne) ; réconciliation des agrégats avec `fact_orders` |
| [tests/test_data_quality_operator.py](tests/test_data_quality_operator.py) | 11 tests du `DataQualityOperator`, base de données mockée : règles OK et KO, SQL généré, `fail_on_error`, configurations invalides rejetées |
| [tests/test_marketplace_api_hook.py](tests/test_marketplace_api_hook.py) | 12 tests du Hook |

## Lancement

Depuis un clone neuf (testé après `docker compose down -v` et `git clean -xfd`) :

```bash
./scripts/init_env.sh          # crée .env depuis .env.example, avec des secrets Airflow générés
docker compose up -d --build   # environ 1 minute (images en cache) ; le schéma DWH est créé au premier démarrage
docker compose ps              # attendre que tout soit healthy
./scripts/load_demo_data.sh    # 50 jours de données, agrégats, anomalies et dashboards Metabase
```

Grafana se configure tout seul au démarrage. Le DAG d'ingestion est **en pause** après installation, pour que le test du formateur (`airflow dags test …`) fonctionne. Les DAGs analytics et anomalies sont actifs. Pour une exploitation quotidienne, activer l'ingestion dans l'UI ou avec `airflow dags unpause marketplace_orders_ingest_daily`.

| Service | URL / accès | Identifiants |
|---|---|---|
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| MinIO console | http://localhost:9001 | `minio_admin` / `minio_password_2026` |
| API Marketplace | http://localhost:5000 | Bearer `formation-token-2026` |
| Metabase | http://localhost:3000 | `admin@maelys.local` / `Admin2026!` (après `python3 metabase/setup_dashboards.py`) |
| PostgreSQL DWH | `localhost:5433` (hôte) · `postgres-dwh:5432` (conteneurs) | `dwh_user` / `dwh_password_2026`, base `dwh` |

Connections Airflow (déclarées par variables d'environnement `AIRFLOW_CONN_*` dans `docker-compose.yaml`, donc absentes de la liste de l'UI) :

| conn_id | Type | Usage |
|---|---|---|
| `marketplace_api` | http | `host` = URL de l'API, `password` = token Bearer |
| `postgres_dwh` | postgres | Data Warehouse |
| `minio_s3` | aws | MinIO via `S3Hook` (`endpoint_url` en extra) |

## API Marketplace simulée (`api/`)

| Méthode | Endpoint | Paramètres | Auth |
|---|---|---|---|
| GET | `/health` | — | non |
| GET | `/orders` | `date=YYYY-MM-DD` (obligatoire) | Bearer |
| GET | `/sellers` | `limit` | Bearer |
| GET | `/products` | — | Bearer |
| GET | `/customers` | `limit` | Bearer |

- **Déterministe** : catalogue généré avec une graine fixe, commandes avec `seed = md5(date)` → une même date renvoie toujours les mêmes commandes.
- Volume : ~`API_ORDERS_PER_DAY` commandes/jour (1 000 par défaut, +25 % le week-end), 150 vendeurs, 3 000 produits, 8 000 clients.
- Une commande = un produit. `status` ∈ `completed` / `cancelled` / `refunded`.
- Défauts volontaires : ~0,2 % de `customer_id` nuls, ~0,4 % de prix suspects (×10, ×25 ou 0,01 €), ~7 % de journées en chute de ventes, ~6 % de vendeurs qui arrêtent de vendre.
- Date future → liste vide · date absente ou invalide → 400 · sans token → 401.
- `API_FAILURE_RATE` > 0 renvoie des 503 aléatoires pour tester le retry du Hook (sauf sur `/webhook`). Exemple : `API_FAILURE_RATE=1 docker compose up -d api-marketplace` pour une panne totale, puis `docker compose up -d api-marketplace` pour revenir à la normale.

```bash
curl http://localhost:5000/health
curl -H "Authorization: Bearer formation-token-2026" "http://localhost:5000/orders?date=2026-04-07"
```

## Commandes utiles

```bash
docker compose logs -f airflow-scheduler --tail=50
docker compose exec airflow-worker airflow dags test marketplace_orders_ingest_daily 2026-04-07
docker compose exec postgres-dwh psql -U dwh_user -d dwh
docker compose exec airflow-worker pytest tests/ -v
docker compose down        # volumes conservés
docker compose down -v     # reset complet
```

> `init-db/*.sql` n'est exécuté par PostgreSQL qu'à la **création** du volume `pg-dwh-data`. Après modification du schéma : `docker compose down -v` ou rejouer le fichier avec `psql`.
