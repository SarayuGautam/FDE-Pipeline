


CREATE SCHEMA IF NOT EXISTS STAGING;

CREATE OR REPLACE VIEW Staging.STG_PRODUCTS AS
SELECT
    'API' as source_system,
    'PRD' || LPAD((product_json->>'id'), 3, '0') as product_id,
    product_json->>'title' as product_name,
    product_json->>'category' as category,
    product_json->>'brand' as brand,
    (product_json->>'price')::DECIMAL(10,2) as price,
    (product_json->>'stock')::INTEGER as stock_quantity,
    product_json->>'sku' as sku,
    loaded_at as source_loaded_at
FROM Landing.V_LND_PRODUCTS_API
WHERE product_json IS NOT NULL

UNION ALL

SELECT
    'JSON' AS source_system,
    product_json->>'id' AS product_id,
    product_json->>'title' AS product_name,
    product_json->>'category' AS category,
    product_json->>'brand' AS brand,
    (product_json->>'price')::DECIMAL(10,2) AS price,
    (product_json->>'stock')::INTEGER AS stock_quantity,
    NULL AS sku,
    loaded_at AS source_loaded_at
FROM Landing.V_LND_PRODUCTS_JSON
WHERE product_json IS NOT NULL;

UNION ALL

SELECT
    'CSV' as source_system,
    product_key as product_id,
    product_name,
    category,
    brand,
    unit_price_usd as price,
    NULL::INTEGER as stock_quantity,
    NULL as sku,
    loaded_at as source_loaded_at
FROM Landing.LND_PRODUCTS_CSV;


CREATE OR REPLACE VIEW Staging.STG_USERS AS
SELECT
    'API' as source_system,
    'CUS' || LPAD((user_json->>'id'), 3, '0') as user_id,
    user_json->>'firstName' as first_name,
    user_json->>'lastName' as last_name,
    user_json->>'email' as email,
    user_json->>'phone' as phone,
    (user_json->>'age')::INTEGER as age,
    user_json->>'gender' as gender,
    user_json->'address'->>'city' as city,
    user_json->'address'->>'state' as state,
    user_json->'address'->>'postalCode' as postal_code,
    user_json->'address'->>'country' as country,
    loaded_at as source_loaded_at
FROM Landing.V_LND_USERS_API
WHERE user_json IS NOT NULL

UNION ALL

SELECT
    'CSV' as source_system,
    customer_key as user_id,
    SPLIT_PART(name, ' ', 1) as first_name,
    CASE
        WHEN array_length(string_to_array(name, ' '), 1) > 1
		THEN regexp_replace(name, '^[^ ]+ ', '')
        ELSE NULL
    END as last_name,
    NULL as email,
    NULL as phone,
    NULL::INTEGER as age,
    gender,
    city,
    state,
    zip_code as postal_code,
    country,
    loaded_at as source_loaded_at
FROM Landing.LND_CUSTOMERS_CSV;


CREATE OR REPLACE VIEW Staging.STG_SALES AS
SELECT
    'JSON' as source_system,
    (sales_json->>'Sale_ID') as sale_id,
    (sales_json->>'Date')::DATE as sale_date,
    (sales_json->>'Store_ID')::INTEGER as store_id,
    sales_json->>'Product_ID' as product_id,
    sales_json->>'Customer'->>'id' as customer_id,
    (sales_json->>'Units')::INTEGER as quantity,
    NULL::DECIMAL(10,2) as unit_price,
    NULL::DECIMAL(12,2) as total_amount,
    loaded_at as source_loaded_at
FROM Landing.V_LND_SALES_JSON
WHERE sales_json IS NOT NULL

UNION ALL

SELECT
    'CSV' as source_system,
    ROW_NUMBER() OVER (ORDER BY order_number, line_item) as sale_id,
    order_date as sale_date,
    store_key::INTEGER as store_id,
    product_key as product_id,
    customer_key as customer_id,
    quantity,
    NULL::DECIMAL(10,2) as unit_price,
    NULL::DECIMAL(12,2) as total_amount,
    loaded_at as source_loaded_at
FROM Landing.LND_SALES_CSV;
