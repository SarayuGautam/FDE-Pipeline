-- Dimension table for product categories

DROP TABLE target.DIM_PRODUCT_CATEGORIES CASCADE;

CREATE TABLE IF NOT EXISTS target.DIM_PRODUCT_CATEGORIES (
    category_id VARCHAR(20) PRIMARY KEY,
    category_description TEXT,
    parent_category_id VARCHAR(20),
    category_level INTEGER NOT NULL,
    created_date TIMESTAMP,
    last_updated TIMESTAMP,
    category_path VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (parent_category_id) REFERENCES target.DIM_PRODUCT_CATEGORIES (category_id)
);
