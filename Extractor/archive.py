import logging
import os
from datetime import datetime
from string import Template

import yaml
from dotenv import load_dotenv
from sqlalchemy import text

from Extractor.database_connector import DatabaseConnector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONFIG_PATH = "Extractor/config.yaml"

load_dotenv()


def load_config(config_path):
    with open(config_path, "r") as file:
        config_content = file.read()
    template = Template(config_content)
    config_content = template.safe_substitute(os.environ)
    return yaml.safe_load(config_content)


def get_table_columns(engine, table_name, schema):
    table_name = table_name.lower()
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
        return [row[0] for row in result]


def archive_table(
    engine,
    source_table,
    archive_table,
    source_schema="landing",
    archive_schema="archive",
):
    source_table = source_table.lower()
    archive_table = archive_table.lower()
    source_columns = get_table_columns(engine, source_table, source_schema)
    archive_columns = get_table_columns(engine, archive_table, archive_schema)

    common_columns = [col for col in source_columns if col in archive_columns]
    if not common_columns:
        logger.warning(
            f"No common columns to archive from {source_schema}.{source_table} to {archive_schema}.{archive_table}"
        )
        return
    insert_columns = ", ".join(common_columns + ["archived_at"])
    select_columns = ", ".join(common_columns) + ", :archived_at"
    params = {"archived_at": datetime.now()}

    sql = text(
        f"""
        INSERT INTO {archive_schema}.{archive_table} ({insert_columns})
        SELECT {select_columns} FROM {source_schema}.{source_table}
        """
    )

    with engine.connect() as conn:
        result = conn.execute(sql, params)
        conn.commit()
        logger.info(
            f"Archived {getattr(result, 'rowcount', 'unknown')} rows from {source_schema}.{source_table} to {archive_schema}.{archive_table}"
        )


def main():
    config = load_config(CONFIG_PATH)
    db_connector = DatabaseConnector(config)
    engine = db_connector.get_engine()

    landing_tables = set(config.get("s3", {}).get("files", {}).values())
    landing_tables.update(config.get("api", {}).get("endpoints", {}).values())

    for table in landing_tables:
        archive_table_name = f"archive_{table.lower()}"
        try:
            archive_table(
                engine,
                table,
                archive_table_name,
                source_schema="landing",
                archive_schema="archive",
            )
        except Exception as e:
            logger.error(f"Failed to archive {table}: {str(e)}")

    engine.dispose()


if __name__ == "__main__":
    main()
