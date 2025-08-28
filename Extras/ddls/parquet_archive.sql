CREATE TABLE IF NOT EXISTS archive.ARCHIVE_LND_PRODUCT_CATEGORIES_PARQUET (
    id BIGSERIAL PRIMARY KEY,
    category_id VARCHAR(20),
    category_name VARCHAR(255),
    category_description TEXT,
    parent_category_id VARCHAR(20),
    category_level INTEGER,
    sort_order INTEGER,
    is_active BOOLEAN,
    created_date TIMESTAMP,
    last_updated TIMESTAMP,
    category_path VARCHAR(500),
    category_depth INTEGER,
    loaded_at TIMESTAMP,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


