from psycopg2 import sql
from Loader.utils import execute_query, get_db_connection, get_entities, get_schemas

ENTITY = "categories"


def load_categories():
    """Load categories data from staging to target"""
    schemas = get_schemas()
    entities = get_entities()

    conn = get_db_connection()
    try:
        # Step 1: Clear temp table
        execute_query(
            conn,
            f"TRUNCATE TABLE {schemas['transform_schema']}.{entities[ENTITY]['temp_table']}",
        )

        # Step 2: Load staging data to temp table
        load_query = sql.SQL(
            """
            INSERT INTO {transform_table} (
                category_id, category_description, parent_category_id,
                category_level, created_date, last_updated,
                category_path
            )
            SELECT
                category_id, category_description, parent_category_id,
                category_level, created_date, last_updated,
                category_path
            FROM {staging_view}
        """
        ).format(
            transform_table=sql.Identifier(
                schemas["transform_schema"], entities[ENTITY]["temp_table"]
            ),
            staging_view=sql.Identifier(
                schemas["staging_schema"], entities[ENTITY]["staging_view"]
            ),
        )
        execute_query(conn, load_query)

        # Step 3: Merge into target table
        merge_query = sql.SQL(
            """
            INSERT INTO {target_table} (
                category_id, category_description, parent_category_id,
                category_level, created_date, last_updated,
                category_path
            )
            SELECT DISTINCT ON (category_id)
                category_id, category_description, parent_category_id,
                category_level, created_date, last_updated,
                category_path
            FROM {transform_table}
            ORDER BY category_id, last_updated DESC NULLS LAST, created_date DESC NULLS LAST
            ON CONFLICT (category_id) DO UPDATE SET
                category_description = EXCLUDED.category_description,
                parent_category_id = EXCLUDED.parent_category_id,
                category_level = EXCLUDED.category_level,
                created_date = EXCLUDED.created_date,
                last_updated = EXCLUDED.last_updated,
                category_path = EXCLUDED.category_path,
                updated_at = CURRENT_TIMESTAMP
        """
        ).format(
            target_table=sql.Identifier(
                schemas["target_schema"], entities[ENTITY]["target_table"]
            ),
            transform_table=sql.Identifier(
                schemas["transform_schema"], entities[ENTITY]["temp_table"]
            ),
        )
        execute_query(conn, merge_query)

        print("Categories loaded successfully")
    except Exception as e:
        print(f"Error loading Categories: {str(e)}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    load_categories()
