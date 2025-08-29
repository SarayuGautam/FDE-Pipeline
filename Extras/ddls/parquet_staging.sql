
CREATE OR REPLACE VIEW staging.STG_PRODUCT_CATEGORIES AS
SELECT
    category_id,
    category_description,
    parent_category_id,
    category_level,
    created_date,
    last_updated,
    category_path,
    loaded_at AS source_loaded_at
FROM landing.LND_PRODUCT_CATEGORIES_PARQUET;

