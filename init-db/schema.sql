-- =============================================================================
-- Maelys Marketplace — Data Warehouse (PostgreSQL 18)
--
--   staging   : copie typée des données brutes de l'API (1 chargement = 1 dt)
--   dwh       : modèle dimensionnel en étoile (Kimball)
--   analytics : agrégations pré-calculées pour Metabase
--
-- Grain de dwh.fact_orders : 1 ligne = 1 commande (l'API vend 1 produit par commande).
-- Le script est ré-exécutable (IF NOT EXISTS / ON CONFLICT) :
--   docker compose exec postgres-dwh psql -U dwh_user -d dwh -f /docker-entrypoint-initdb.d/schema.sql
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS analytics;

-- -----------------------------------------------------------------------------
-- STAGING
-- -----------------------------------------------------------------------------

-- Commandes d'une journée, rechargées par DELETE + INSERT sur dt.
CREATE TABLE IF NOT EXISTS staging.orders (
    order_id        TEXT          NOT NULL,
    dt              DATE          NOT NULL,
    order_ts        TIMESTAMP,
    customer_id     TEXT,
    seller_id       TEXT,
    product_id      TEXT,
    quantity        INTEGER,
    unit_price      NUMERIC(12, 2),
    total_amount    NUMERIC(12, 2),
    currency        TEXT,
    status          TEXT,
    payment_method  TEXT,
    loaded_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (dt, order_id)
);

-- Référentiels : instantané complet rechargé à chaque refresh (TRUNCATE + INSERT).
CREATE TABLE IF NOT EXISTS staging.sellers (
    seller_id    TEXT PRIMARY KEY,
    name         TEXT,
    seller_type  TEXT,
    country      TEXT,
    city         TEXT,
    joined_date  DATE,
    loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.products (
    product_id  TEXT PRIMARY KEY,
    name        TEXT,
    category    TEXT,
    seller_id   TEXT,
    base_price  NUMERIC(12, 2),
    created_at  DATE,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id  TEXT PRIMARY KEY,
    email        TEXT,
    city         TEXT,
    country      TEXT,
    signup_date  DATE,
    loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- DWH — dimensions (SCD type 1 : écrasement par UPSERT)
-- Chaque dimension contient un membre "UNKNOWN" pour les clés absentes ou nulles,
-- ce qui garantit que les FOREIGN KEY de la table de faits sont toujours satisfaites.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    dt           DATE PRIMARY KEY,
    year         SMALLINT NOT NULL,
    quarter      SMALLINT NOT NULL,
    month        SMALLINT NOT NULL,
    month_name   TEXT     NOT NULL,
    iso_week     SMALLINT NOT NULL,
    day          SMALLINT NOT NULL,
    day_of_week  SMALLINT NOT NULL,  -- ISO : 1 = lundi … 7 = dimanche
    day_name     TEXT     NOT NULL,
    is_weekend   BOOLEAN  NOT NULL
);

INSERT INTO dwh.dim_date
SELECT d::date,
       EXTRACT(YEAR FROM d),
       EXTRACT(QUARTER FROM d),
       EXTRACT(MONTH FROM d),
       (ARRAY['janvier','février','mars','avril','mai','juin','juillet',
              'août','septembre','octobre','novembre','décembre'])[EXTRACT(MONTH FROM d)],
       EXTRACT(WEEK FROM d),
       EXTRACT(DAY FROM d),
       EXTRACT(ISODOW FROM d),
       (ARRAY['lundi','mardi','mercredi','jeudi','vendredi','samedi','dimanche'])[EXTRACT(ISODOW FROM d)],
       EXTRACT(ISODOW FROM d) >= 6
FROM generate_series(DATE '2020-01-01', DATE '2030-12-31', INTERVAL '1 day') AS d
ON CONFLICT (dt) DO NOTHING;

CREATE TABLE IF NOT EXISTS dwh.dim_seller (
    seller_id    TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    seller_type  TEXT,
    country      TEXT,
    city         TEXT,
    joined_date  DATE,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_id  TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    category    TEXT NOT NULL,
    seller_id   TEXT NOT NULL REFERENCES dwh.dim_seller (seller_id),
    base_price  NUMERIC(12, 2),
    created_at  DATE,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_id  TEXT PRIMARY KEY,
    email        TEXT,
    city         TEXT,
    country      TEXT,
    signup_date  DATE,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO dwh.dim_seller (seller_id, name) VALUES ('UNKNOWN', 'Vendeur inconnu')
ON CONFLICT (seller_id) DO NOTHING;
INSERT INTO dwh.dim_product (product_id, name, category, seller_id) VALUES ('UNKNOWN', 'Produit inconnu', 'Inconnue', 'UNKNOWN')
ON CONFLICT (product_id) DO NOTHING;
INSERT INTO dwh.dim_customer (customer_id) VALUES ('UNKNOWN')
ON CONFLICT (customer_id) DO NOTHING;

-- -----------------------------------------------------------------------------
-- DWH — table de faits
-- Rechargée par partition : DELETE WHERE dt = '{{ ds }}' puis INSERT (idempotent).
-- Montants en NUMERIC, commission = 12 % des commandes "completed" uniquement.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dwh.fact_orders (
    order_id           TEXT           PRIMARY KEY,
    dt                 DATE           NOT NULL REFERENCES dwh.dim_date (dt),
    seller_id          TEXT           NOT NULL REFERENCES dwh.dim_seller (seller_id),
    product_id         TEXT           NOT NULL REFERENCES dwh.dim_product (product_id),
    customer_id        TEXT           NOT NULL REFERENCES dwh.dim_customer (customer_id),
    order_ts           TIMESTAMP      NOT NULL,
    quantity           INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price         NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    total_amount       NUMERIC(12, 2) NOT NULL CHECK (total_amount >= 0),
    commission_rate    NUMERIC(5, 4)  NOT NULL DEFAULT 0.12,
    commission_amount  NUMERIC(12, 2) NOT NULL,
    status             TEXT           NOT NULL CHECK (status IN ('completed', 'cancelled', 'refunded')),
    payment_method     TEXT,
    currency           CHAR(3)        NOT NULL DEFAULT 'EUR',
    loaded_at          TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_fact_orders_dt ON dwh.fact_orders (dt);
CREATE INDEX IF NOT EXISTS ix_fact_orders_seller_dt ON dwh.fact_orders (seller_id, dt);
CREATE INDEX IF NOT EXISTS ix_fact_orders_product ON dwh.fact_orders (product_id);

-- -----------------------------------------------------------------------------
-- ANALYTICS — agrégats lus par Metabase (DELETE + INSERT par dt)
-- Le CA ("revenue") ne compte que les commandes "completed".
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.daily_summary (
    dt                DATE PRIMARY KEY,
    total_orders      INTEGER        NOT NULL,
    completed_orders  INTEGER        NOT NULL,
    cancelled_orders  INTEGER        NOT NULL,
    refunded_orders   INTEGER        NOT NULL,
    total_revenue     NUMERIC(14, 2) NOT NULL,
    total_commission  NUMERIC(14, 2) NOT NULL,
    avg_basket        NUMERIC(12, 2),
    active_sellers    INTEGER        NOT NULL,
    top_seller_id     TEXT,
    refreshed_at      TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analytics.seller_daily (
    dt             DATE           NOT NULL,
    seller_id      TEXT           NOT NULL,
    orders         INTEGER        NOT NULL,
    revenue        NUMERIC(14, 2) NOT NULL,
    commission     NUMERIC(14, 2) NOT NULL,
    refreshed_at   TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (dt, seller_id)
);

CREATE TABLE IF NOT EXISTS analytics.category_daily (
    dt             DATE           NOT NULL,
    category       TEXT           NOT NULL,
    orders         INTEGER        NOT NULL,
    revenue        NUMERIC(14, 2) NOT NULL,
    commission     NUMERIC(14, 2) NOT NULL,
    refreshed_at   TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (dt, category)
);

-- Journal des recalculs des DAGs aval. source_loaded_at = version des faits utilisée
-- (max(fact_orders.loaded_at) sur la fenêtre de la date) : une date est à recalculer
-- dès que ses faits ont été rechargés depuis, même si un événement d'Asset a été perdu.
CREATE TABLE IF NOT EXISTS analytics.refresh_log (
    dag_id            TEXT        NOT NULL,
    dt                DATE        NOT NULL,
    source_loaded_at  TIMESTAMPTZ NOT NULL,
    refreshed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dag_id, dt)
);

-- Bonus : anomalies détectées (CA du jour < 70 % de la moyenne mobile 7 jours).
CREATE TABLE IF NOT EXISTS analytics.anomalies (
    dt           DATE           NOT NULL,
    scope        TEXT           NOT NULL CHECK (scope IN ('global', 'seller', 'price')),
    entity_id    TEXT           NOT NULL,  -- 'ALL', seller_id ou order_id
    metric       TEXT           NOT NULL,
    value        NUMERIC(14, 2),
    reference    NUMERIC(14, 2),           -- moyenne 7 j ou prix de base
    ratio        NUMERIC(8, 4),
    detected_at  TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (dt, scope, entity_id, metric)
);
