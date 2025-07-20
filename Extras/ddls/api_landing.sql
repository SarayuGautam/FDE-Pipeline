-- Database: FDE
-- Schema: Landing

CREATE SCHEMA IF NOT EXISTS Landing;

CREATE TABLE Landing.LND_PRODUCTS_API (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    api_endpoint VARCHAR(500),
    request_timestamp TIMESTAMP,
    response_status INTEGER,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Landing.LND_USERS_API (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    api_endpoint VARCHAR(500),
    request_timestamp TIMESTAMP,
    response_status INTEGER,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
