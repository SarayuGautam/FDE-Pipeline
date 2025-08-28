import logging

from psycopg2 import sql
from utils import execute_query, get_db_connection, get_entities, get_schemas

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_products():

  schemas = get_schemas()
  entities = get_entities()
  conn = get_db_connection()

  ENTITY = "products"

  try:
      # Truncate the Temp Table
      truncate_query = f"TRUNCATE TABLE {schemas['transform_schema']}.{entities[ENTITY]['temp_table']}"
      execute_query(conn, truncate_query)

      # Load the Temp Table
      load_temp_query = sql.SQL(
          """
        INSERT INTO {temp_table}
        (
            product_id,
          product_name,
          category,
          brand,
          price,
          stock_quantity,
          sku,
          source_system,
          source_loaded_at
        )
        SELECT
          product_id,
          product_name,
          category,
          brand,
          price,
          stock_quantity,
          sku,
          source_system,
          source_loaded_at
          FROM {staging_view}
        """
      ).format(
          temp_table=sql.Identifier(schemas["transform_schema"], entities[ENTITY]["temp_table"]),
          staging_view=sql.Identifier(schemas["staging_schema"], entities[ENTITY]["staging_view"]),
      )
      execute_query(conn, load_temp_query)

      # Load to Target
      load_target_query = sql.SQL(
          """
        INSERT INTO {target_table}
        (
            product_id,
          product_name,
          category,
          brand,
          price,
          stock_quantity,
          sku,
          source_system
        )
        SELECT
          product_id,
          product_name,
          category,
          brand,
          price,
          stock_quantity,
          sku,
          source_system
          FROM {temp_table}
          ON CONFLICT (product_id) DO UPDATE
          SET
          product_name = EXCLUDED.product_name,
          category=EXCLUDED.category,
          brand=EXCLUDED.brand,
          price=EXCLUDED.price,
          stock_quantity=EXCLUDED.stock_quantity,
          sku=EXCLUDED.sku,
          source_system=EXCLUDED.source_system,
          updated_at=CURRENT_TIMESTAMP
        """
      ).format(
          target_table=sql.Identifier(schemas["target_schema"], entities[ENTITY]["target_table"]),
          temp_table=sql.Identifier(schemas["transform_schema"], entities[ENTITY]["temp_table"]),
      )
      execute_query(conn, load_target_query)
  except Exception as e:
      logger.error(f"Error while loading products {str(e)}")
      raise
  finally:
      logger.info("Products Loaded Successfully!")


if __name__ == "__main__":
    load_products()
