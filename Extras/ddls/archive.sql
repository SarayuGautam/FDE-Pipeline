CREATE SCHEMA IF NOT EXISTS ARCHIVE;

-- Archive for API Products
CREATE TABLE Archive.ARCHIVE_LND_PRODUCTS_API (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    loaded_at TIMESTAMP,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Archive for API Users
CREATE TABLE Archive.ARCHIVE_LND_USERS_API (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    loaded_at TIMESTAMP,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Archive for CSV Customers
CREATE TABLE Archive.ARCHIVE_LND_CUSTOMERS_CSV (
    id SERIAL PRIMARY KEY,
    customer_key VARCHAR(50),
    gender VARCHAR(20),
    name VARCHAR(255),
    city VARCHAR(100),
    state_code VARCHAR(50),
    state VARCHAR(100),
    zip_code VARCHAR(20),
    country VARCHAR(100),
    continent VARCHAR(50),
    birthday DATE,
    loaded_at TIMESTAMP,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Archive for CSV Products
CREATE TABLE Archive.ARCHIVE_LND_PRODUCTS_CSV (
    id SERIAL PRIMARY KEY,
    product_key VARCHAR(50),
    product_name VARCHAR(255),
    brand VARCHAR(100),
    color VARCHAR(50),
    unit_cost_usd DECIMAL(10,2),
    unit_price_usd DECIMAL(10,2),
    subcategory_key VARCHAR(50),
    subcategory VARCHAR(100),
    category_key VARCHAR(50),
    category VARCHAR(100),
    loaded_at TIMESTAMP,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Archive for CSV Sales
CREATE TABLE Archive.ARCHIVE_LND_SALES_CSV (
    id SERIAL PRIMARY KEY,
    order_number VARCHAR(100),
    line_item INTEGER,
    order_date DATE,
    delivery_date DATE,
    customer_key VARCHAR(50),
    store_key VARCHAR(50),
    product_key VARCHAR(50),
    quantity INTEGER,
    currency_code VARCHAR(10),
    loaded_at TIMESTAMP,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Archive for JSON Products
CREATE TABLE Archive.ARCHIVE_LND_PRODUCTS_JSON (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    loaded_at TIMESTAMP,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Archive for JSON Sales
CREATE TABLE Archive.ARCHIVE_LND_SALES_JSON (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    loaded_at TIMESTAMP,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
