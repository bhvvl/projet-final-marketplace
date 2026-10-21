# Maelys Marketplace — MarketPlace Analytics

MVP de plateforme data : API backoffice → Airflow 3.1.8 → MinIO (raw) → PostgreSQL 18 (`staging` / `dwh` / `analytics`) → Metabase.

Plan de travail : [PLAN.md](PLAN.md).

## Architecture

```mermaid
flowchart LR
    api[("API Marketplace<br/>Flask simulée")] --> hook["MarketplaceAPIHook"]
    hook --> airflow[["Airflow 3.1.8"]]
    airflow -- "raw JSON dt=" --> minio[("MinIO<br/>data-lake")]
    airflow -- "load + transform" --> pg[("PostgreSQL 18<br/>staging / dwh / analytics")]
    pg --> mb["Metabase"]
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

## Lancement

```bash
cp .env.example .env   # puis générer les 3 secrets Airflow (voir commentaires)
docker compose up -d --build
docker compose ps      # attendre ~1-2 min que tout soit healthy
```

| Service | URL / accès | Identifiants |
|---|---|---|
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| MinIO console | http://localhost:9001 | `minio_admin` / `minio_password_2026` |
| API Marketplace | http://localhost:5000 | Bearer `formation-token-2026` |
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
- `API_FAILURE_RATE` > 0 renvoie des 503 aléatoires pour tester le retry du Hook.

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
