CREATE OR REPLACE VIEW Landing.V_LND_PRODUCTS_API AS
SELECT
     l.loaded_at,
    jsonb_array_elements(l.raw_data->'products') AS product_json
FROM Landing.LND_PRODUCTS_API l
WHERE l.raw_data IS NOT NULL;


CREATE OR REPLACE VIEW Landing.V_LND_PRODUCTS_JSON AS
SELECT
     l.loaded_at,
    jsonb_array_elements(l.raw_data->'products') AS product_json
FROM Landing.LND_PRODUCTS_JSON l
WHERE l.raw_data IS NOT NULL;

CREATE OR REPLACE VIEW Landing.V_LND_USERS_API AS
SELECT
     l.loaded_at,
    jsonb_array_elements(l.raw_data->'users') AS user_json
FROM Landing.LND_USERS_API l
WHERE l.raw_data IS NOT NULL;


CREATE OR REPLACE VIEW Landing.V_LND_SALES_JSON AS
SELECT
     l.loaded_at,
    jsonb_array_elements(l.raw_data->'sales') AS sales_json
FROM Landing.LND_SALES_JSON l
WHERE l.raw_data IS NOT NULL;
