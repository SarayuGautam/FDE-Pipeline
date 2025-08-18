-- Dimension table for product categories
CREATE TABLE IF NOT EXISTS target.DIM_PRODUCT_CATEGORIES (
    category_id VARCHAR(20) PRIMARY KEY,
    category_name VARCHAR(255) NOT NULL,
    category_description TEXT,
    parent_category_id VARCHAR(20),
    category_level INTEGER NOT NULL,
    sort_order INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP,
    last_updated TIMESTAMP,
    category_path VARCHAR(500),
    category_depth INTEGER,
    source_system VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Self-referencing foreign key for hierarchy
    FOREIGN KEY (parent_category_id) REFERENCES target.DIM_PRODUCT_CATEGORIES(category_id)
);

-- Update existing DIM_PRODUCTS table to include category_id
-- Students need to add this column to their existing DIM_PRODUCTS table
ALTER TABLE target.DIM_PRODUCTS ADD COLUMN IF NOT EXISTS category_id VARCHAR(20);
ALTER TABLE target.DIM_PRODUCTS ADD CONSTRAINT IF NOT EXISTS fk_products_category
    FOREIGN KEY (category_id) REFERENCES target.DIM_PRODUCT_CATEGORIES(category_id);
