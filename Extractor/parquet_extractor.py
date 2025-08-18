#!/usr/bin/env python3
"""
Parquet File Extractor for FDE Pipeline
Follows the same architecture and complexity as other extractors.
"""

import logging
import pandas as pd
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ParquetExtractor:
    """
    Extracts data from parquet files and loads it into the landing layer.

    This class follows the same pattern as the existing extractors in the pipeline.
    """

    def __init__(self, db_connector):
        """
        Initialize the ParquetExtractor.

        Args:
            db_connector: Database connector instance for database operations
        """
        self.db_connector = db_connector

    def load_to_landing(self, table_name, parquet_data):
        """
        Load parquet data to the landing layer.

        Args:
            table_name: Name of the landing table
            parquet_data: DataFrame containing the parquet data
        """
        table_name = table_name.lower()
        engine = self.db_connector.get_engine()

        try:
            # Get table columns to ensure proper mapping
            table_columns = self.get_table_columns(table_name, schema="landing")

            # Filter DataFrame to only include existing columns
            existing_columns = [col for col in parquet_data.columns if col in table_columns]
            missing_columns = [col for col in parquet_data.columns if col not in table_columns]

            if missing_columns:
                logger.warning(
                    f"The following columns will be skipped (not in table {table_name}): {missing_columns}"
                )

            # Filter data to only include existing columns
            df_filtered = parquet_data[existing_columns]

            # Load data to landing table
            df_filtered.to_sql(
                table_name, engine, schema="landing", if_exists="append", index=False
            )

            logger.info(f"Successfully loaded {len(df_filtered)} rows into {table_name}")

        except Exception as e:
            logger.error(f"Error loading data into {table_name}: {e}")
            raise

    def get_table_columns(self, table_name, schema="landing"):
        """Get the actual column names from the database table"""
        table_name = table_name.lower()
        engine = self.db_connector.get_engine()

        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text(
                        f"""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = '{schema}'
                        AND table_name = '{table_name}'
                        ORDER BY ordinal_position
                        """
                    )
                )
                columns = [row[0] for row in result]
                logger.info(f"Table {schema}.{table_name} has columns: {columns}")
                return columns
        except Exception as e:
            logger.error(f"Error getting table columns for {table_name}: {str(e)}")
            raise
