-- Dimensions SCD1 : insertion des nouveaux membres, écrasement des attributs existants.
-- Ordre imposé par la FK dim_product.seller_id -> dim_seller.

INSERT INTO dwh.dim_seller (seller_id, name, seller_type, country, city, joined_date, updated_at)
SELECT seller_id, name, seller_type, country, city, joined_date, now()
FROM staging.sellers
ON CONFLICT (seller_id) DO UPDATE SET
    name        = EXCLUDED.name,
    seller_type = EXCLUDED.seller_type,
    country     = EXCLUDED.country,
    city        = EXCLUDED.city,
    joined_date = EXCLUDED.joined_date,
    updated_at  = now();

INSERT INTO dwh.dim_product (product_id, name, category, seller_id, base_price, created_at, updated_at)
SELECT p.product_id, p.name, p.category, COALESCE(s.seller_id, 'UNKNOWN'), p.base_price, p.created_at, now()
FROM staging.products p
LEFT JOIN dwh.dim_seller s ON s.seller_id = p.seller_id
ON CONFLICT (product_id) DO UPDATE SET
    name       = EXCLUDED.name,
    category   = EXCLUDED.category,
    seller_id  = EXCLUDED.seller_id,
    base_price = EXCLUDED.base_price,
    created_at = EXCLUDED.created_at,
    updated_at = now();

INSERT INTO dwh.dim_customer (customer_id, email, city, country, signup_date, updated_at)
SELECT customer_id, email, city, country, signup_date, now()
FROM staging.customers
ON CONFLICT (customer_id) DO UPDATE SET
    email       = EXCLUDED.email,
    city        = EXCLUDED.city,
    country     = EXCLUDED.country,
    signup_date = EXCLUDED.signup_date,
    updated_at  = now();
