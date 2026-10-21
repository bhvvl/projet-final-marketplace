-- analytics.category_daily pour une date (paramètre %(dt)s) : DELETE + INSERT, idempotent.
-- Commandes "completed" uniquement.

DELETE FROM analytics.category_daily WHERE dt = %(dt)s;

INSERT INTO analytics.category_daily (dt, category, orders, revenue, commission)
SELECT f.dt, p.category, count(*), sum(f.total_amount), sum(f.commission_amount)
FROM dwh.fact_orders f
JOIN dwh.dim_product p ON p.product_id = f.product_id
WHERE f.dt = %(dt)s AND f.status = 'completed'
GROUP BY f.dt, p.category;
