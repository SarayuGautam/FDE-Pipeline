
CREATE OR REPLACE VIEW staging.STG_PRODUCT_CATEGORIES AS
SELECT
    category_id,
    category_name,
    category_description,
    parent_category_id,
    category_level,
    sort_order,
    is_active,
    created_date,
    last_updated,
    category_path,
    category_depth,
    'PARQUET' AS source_system,
    loaded_at AS source_loaded_at
FROM landing.LND_PRODUCT_CATEGORIES_PARQUET;

