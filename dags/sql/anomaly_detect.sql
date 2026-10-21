-- Anomalies commerciales d'une date (paramètre %(dt)s) : DELETE + INSERT, idempotent.
-- Calculées sur dwh.fact_orders (commandes completed) et non sur analytics.* :
-- pas de dépendance à l'ordre d'exécution des DAGs déclenchés par le même Asset.
-- La référence "7 jours" est la moyenne des jours chargés parmi les 7 précédents ;
-- il en faut au moins %(min_history_days)s pour conclure.

DELETE FROM analytics.anomalies WHERE dt = %(dt)s;

-- 1. Global (US-05) : CA du jour < seuil × moyenne des 7 jours précédents.
INSERT INTO analytics.anomalies (dt, scope, entity_id, metric, value, reference, ratio)
SELECT %(dt)s::date, 'global', 'ALL', 'revenue', today.revenue, hist.avg_7d, round(today.revenue / hist.avg_7d, 4)
FROM (
    SELECT COALESCE(sum(total_amount), 0) AS revenue
    FROM dwh.fact_orders
    WHERE dt = %(dt)s AND status = 'completed'
) today,
(
    SELECT avg(revenue) AS avg_7d, count(*) AS days
    FROM (
        SELECT dt, sum(total_amount) AS revenue
        FROM dwh.fact_orders
        WHERE status = 'completed' AND dt >= %(dt)s::date - 7 AND dt < %(dt)s::date
        GROUP BY dt
    ) d
) hist
WHERE hist.days >= %(min_history_days)s
  AND today.revenue < %(threshold)s * hist.avg_7d;

-- 2. Vendeurs : CA du jour < seuil × moyenne 7 jours (jours sans vente comptés à 0),
--    limité aux vendeurs dont la moyenne dépasse %(seller_min_avg)s € (sinon trop de bruit).
WITH history_days AS (
    SELECT count(DISTINCT dt) AS days
    FROM dwh.fact_orders
    WHERE dt >= %(dt)s::date - 7 AND dt < %(dt)s::date
),
history AS (
    SELECT seller_id, sum(total_amount) / (SELECT days FROM history_days) AS avg_7d
    FROM dwh.fact_orders
    WHERE status = 'completed' AND dt >= %(dt)s::date - 7 AND dt < %(dt)s::date
    GROUP BY seller_id
),
today AS (
    SELECT seller_id, sum(total_amount) AS revenue
    FROM dwh.fact_orders
    WHERE status = 'completed' AND dt = %(dt)s
    GROUP BY seller_id
)
INSERT INTO analytics.anomalies (dt, scope, entity_id, metric, value, reference, ratio)
SELECT %(dt)s::date, 'seller', h.seller_id, 'revenue',
       COALESCE(t.revenue, 0), round(h.avg_7d, 2), round(COALESCE(t.revenue, 0) / h.avg_7d, 4)
FROM history h
LEFT JOIN today t ON t.seller_id = h.seller_id
WHERE (SELECT days FROM history_days) >= %(min_history_days)s
  AND h.seller_id <> 'UNKNOWN'
  AND h.avg_7d >= %(seller_min_avg)s
  AND COALESCE(t.revenue, 0) < %(threshold)s * h.avg_7d;

-- 3. Prix suspects : prix unitaire hors de [bas × prix catalogue ; haut × prix catalogue].
INSERT INTO analytics.anomalies (dt, scope, entity_id, metric, value, reference, ratio)
SELECT f.dt, 'price', f.order_id, 'unit_price', f.unit_price, p.base_price, round(f.unit_price / p.base_price, 4)
FROM dwh.fact_orders f
JOIN dwh.dim_product p ON p.product_id = f.product_id
WHERE f.dt = %(dt)s
  AND p.base_price > 0
  AND (f.unit_price > %(price_high_ratio)s * p.base_price OR f.unit_price < %(price_low_ratio)s * p.base_price);
