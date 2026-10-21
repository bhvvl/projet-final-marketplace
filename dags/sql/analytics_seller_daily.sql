-- analytics.seller_daily pour une date (paramètre %(dt)s) : DELETE + INSERT, idempotent.
-- Commandes "completed" uniquement.

DELETE FROM analytics.seller_daily WHERE dt = %(dt)s;

INSERT INTO analytics.seller_daily (dt, seller_id, orders, revenue, commission)
SELECT dt, seller_id, count(*), sum(total_amount), sum(commission_amount)
FROM dwh.fact_orders
WHERE dt = %(dt)s AND status = 'completed'
GROUP BY dt, seller_id;
