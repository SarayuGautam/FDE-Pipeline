
CREATE TABLE IF NOT EXISTS temp.TMP_PRODUCT_CATEGORIES (
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
    source_system VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
