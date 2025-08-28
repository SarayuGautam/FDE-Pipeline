# FDE Project - Module Usage Documentation

This document tracks where each Python module is imported and used in the FDE (Fake Data Extractor) project.

## Database Connectivity

### psycopg2-binary>=2.9.0
- **Extractor/database_connector.py** - Database connection management
- **Extractor/json_extractor.py** - JSON data insertion using psycopg2.extras.Json
- **Loader/utils.py** - Database connection for loader operations
- **Loader/sales.py** - SQL operations for sales data loading
- **Loader/products.py** - SQL operations for products data loading
- **Loader/users.py** - SQL operations for users data loading

## Database ORM

### sqlalchemy>=1.4.0
- **Extractor/database_connector.py** - Engine creation for database connections
- **Extractor/csv_extractor.py** - SQL text operations for CSV data loading
- **Extractor/archive.py** - Database operations for archiving data
- **Extractor/main_extractor.py** - SQL text operations for table management

## Data Processing

### pandas>=1.5.0
- **Extractor/csv_extractor.py** - CSV data processing and DataFrame operations
- **Extractor/s3_extractor.py** - CSV data processing from S3 files

## HTTP Requests

### requests>=2.28.0
- **Extractor/api_extractor.py** - API endpoint data extraction
- **Extractor/s3_extractor.py** - S3 file downloads via HTTP
- **Extras/Utils/generate_data.py** - Fetching sample data from dummyjson.com API

## Configuration

### pyyaml>=6.0
- **Extractor/main_extractor.py** - Loading configuration from config.yaml
- **Extractor/archive.py** - Loading configuration for archiving operations
- **Loader/utils.py** - Loading configuration for loader operations

## Environment Variables

### python-dotenv>=0.19.0
- **Extractor/main_extractor.py** - Loading environment variables from .env file
- **Extractor/archive.py** - Loading environment variables for archiving
- **Loader/utils.py** - Loading environment variables for loader configuration

## Workflow Orchestration

### prefect>=3.4.7
- **Orchestrator/pipeline.py** - Workflow orchestration and task management

## Standard Library Modules Used

### Built-in modules (no installation required):
- **logging** - Used throughout for logging operations
- **os** - Environment and path operations
- **sys** - System path management
- **string.Template** - Template string substitution
- **datetime** - Date and time operations
- **re** - Regular expressions
- **io** - StringIO for data processing
- **json** - JSON data processing
- **csv** - CSV file operations
- **collections.OrderedDict** - Ordered dictionary operations
- **random** - Random data generation
- **pathlib.Path** - Path operations
