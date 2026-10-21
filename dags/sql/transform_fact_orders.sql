-- staging.orders -> dwh.fact_orders pour la partition {{ ds }}.
-- Idempotent : DELETE + INSERT de la partition, exécutés dans une seule transaction
-- (rejouer la même date donne exactement le même contenu).
-- Les clés absentes des dimensions (ou customer_id nul) sont rattachées au membre UNKNOWN.

DELETE FROM dwh.fact_orders WHERE dt = '{{ ds }}';

INSERT INTO dwh.fact_orders (
    order_id, dt, seller_id, product_id, customer_id, order_ts,
    quantity, unit_price, total_amount, commission_rate, commission_amount,
    status, payment_method, currency
)
SELECT
    o.order_id,
    o.dt,
    COALESCE(s.seller_id, 'UNKNOWN'),
    COALESCE(p.product_id, 'UNKNOWN'),
    COALESCE(c.customer_id, 'UNKNOWN'),
    o.order_ts,
    o.quantity,
    o.unit_price,
    o.total_amount,
    {{ params.commission_rate }},
    CASE WHEN o.status = 'completed' THEN ROUND(o.total_amount * {{ params.commission_rate }}, 2) ELSE 0 END,
    o.status,
    o.payment_method,
    COALESCE(o.currency, 'EUR')
FROM staging.orders o
LEFT JOIN dwh.dim_seller   s ON s.seller_id   = o.seller_id
LEFT JOIN dwh.dim_product  p ON p.product_id  = o.product_id
LEFT JOIN dwh.dim_customer c ON c.customer_id = o.customer_id
WHERE o.dt = '{{ ds }}';
