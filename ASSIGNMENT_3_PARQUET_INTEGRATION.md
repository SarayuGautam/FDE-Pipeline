# Assignment 3: Parquet File Integration with Product Categories

## Overview
In this assignment, you will extend the existing FDE pipeline to support parquet files containing product categories data. You'll integrate this with the existing pipeline architecture.

## Learning Objectives
- Implement parquet file extraction from S3
- Design appropriate DDLs for new data entity
- Integrate new data into the staging and target tables
- Practice dimensional modeling with fact or dimension categorization

## Current Pipeline Architecture
The existing pipeline processes data from:
- **S3 Sources**: JSON and CSV files
- **API Sources**: REST API endpoints
- **Target Areas**: Products, Users, and Sales

## Assignment Tasks

### Task 1: Data Preparation
- Upload the provided `product_categories.parquet` file to your S3 bucket
- Ensure it's accessible via the existing S3 extractor

### Task 2: Landing Layer
- Create `parquet_landing.sql` with landing table for product categories
- Follow existing naming conventions (e.g., `lnd_product_categories_parquet`)

### Task 3: Extractor Integration
- Create `parquet_extractor.py` for parquet file processing
- Update `main_extractor.py` to include parquet extraction
- Modify existing `config.yaml` to include parquet file configurations
- Ensure it follows existing patterns and error handling

### Task 4: Staging and Target Layers
- Create `parquet_staging.sql` with staging view
- Create `parquet_target.sql` with temp and target tables
- Design proper dimensional model for categories

### Task 5: Loading and Orchestration
- Create load script for the new data
- Integrate with existing orchestrator workflow
- Test end-to-end pipeline

## File Structure to Create
```
Extractor/
├── parquet_extractor.py              # Parquet extraction logic
└── config.yaml                       # Modified existing config

DDLs/
├── parquet_landing.sql               # Landing table DDL
├── parquet_staging.sql               # Staging view DDL
└── parquet_target.sql                # Target tables DDL

Loader/
└── categories.py                     # Load script for categories
```

## Technical Requirements

### Parquet File Processing
- Use `pyarrow` or `pandas` for parquet file reading
- Implement proper error handling for corrupted files

### Pipeline Integration
- Integrate with existing Prefect workflow
- Maintain data lineage and audit trails

## Evaluation Criteria
1. **Code Quality** (25%): Clean, well-documented code following Python best practices
2. **Data Modeling** (25%): Proper dimensional modeling with appropriate fact/dimension tables
3. **Pipeline Integration** (25%): Seamless integration with existing pipeline
5. **Documentation** (25%): Clear documentation of design decisions and implementation

## Submission Requirements
1. All source code files
2. DDL scripts for database schema and tables


## Submission Requirements

What is the revenue breakdown by product category and subcategory?
