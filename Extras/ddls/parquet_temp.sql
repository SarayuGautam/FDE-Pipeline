DROP TABLE transform.TMP_PRODUCT_CATEGORIES CASCADE;

CREATE TABLE IF NOT EXISTS transform.TMP_PRODUCT_CATEGORIES (
    category_id VARCHAR(20),
    category_description TEXT,
    parent_category_id VARCHAR(20),
    category_level INTEGER,
    created_date TIMESTAMP,
    last_updated TIMESTAMP,
    category_path VARCHAR(500),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
