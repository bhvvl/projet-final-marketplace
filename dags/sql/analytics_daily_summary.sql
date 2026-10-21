-- analytics.daily_summary pour une date (paramètre %(dt)s) : DELETE + INSERT, idempotent.
-- CA, panier moyen et commission ne comptent que les commandes "completed".

DELETE FROM analytics.daily_summary WHERE dt = %(dt)s;

INSERT INTO analytics.daily_summary (
    dt, total_orders, completed_orders, cancelled_orders, refunded_orders,
    total_revenue, total_commission, avg_basket, active_sellers, top_seller_id
)
SELECT
    f.dt,
    count(*),
    count(*) FILTER (WHERE f.status = 'completed'),
    count(*) FILTER (WHERE f.status = 'cancelled'),
    count(*) FILTER (WHERE f.status = 'refunded'),
    COALESCE(sum(f.total_amount) FILTER (WHERE f.status = 'completed'), 0),
    COALESCE(sum(f.commission_amount), 0),
    round(avg(f.total_amount) FILTER (WHERE f.status = 'completed'), 2),
    count(DISTINCT f.seller_id) FILTER (WHERE f.status = 'completed'),
    (SELECT t.seller_id
       FROM dwh.fact_orders t
      WHERE t.dt = %(dt)s AND t.status = 'completed'
      GROUP BY t.seller_id
      ORDER BY sum(t.total_amount) DESC, t.seller_id
      LIMIT 1)
FROM dwh.fact_orders f
WHERE f.dt = %(dt)s
GROUP BY f.dt;
