-- Landing table for product categories parquet data

DROP TABLE landing.LND_PRODUCT_CATEGORIES_PARQUET CASCADE;

CREATE TABLE IF NOT EXISTS landing.LND_PRODUCT_CATEGORIES_PARQUET (
    id BIGSERIAL PRIMARY KEY,
    category_id VARCHAR(20),
    category_description TEXT,
    parent_category_id VARCHAR(20),
    category_level INTEGER,
    created_date TIMESTAMP,
    last_updated TIMESTAMP,
    category_path VARCHAR(500),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
