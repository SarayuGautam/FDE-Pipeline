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
                category_id, category_name, category_description, parent_category_id,
                category_level, sort_order, is_active, created_date, last_updated,
                category_path, category_depth, source_system
            )
            SELECT
                category_id, category_name, category_description, parent_category_id,
                category_level, sort_order, is_active, created_date, last_updated,
                category_path, category_depth, source_system
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
                category_id, category_name, category_description, parent_category_id,
                category_level, sort_order, is_active, created_date, last_updated,
                category_path, category_depth, source_system
            )
            SELECT
                category_id, category_name, category_description, parent_category_id,
                category_level, sort_order, is_active, created_date, last_updated,
                category_path, category_depth, source_system
            FROM {transform_table}
            ON CONFLICT (category_id) DO UPDATE SET
                category_name = EXCLUDED.category_name,
                category_description = EXCLUDED.category_description,
                parent_category_id = EXCLUDED.parent_category_id,
                category_level = EXCLUDED.category_level,
                sort_order = EXCLUDED.sort_order,
                is_active = EXCLUDED.is_active,
                created_date = EXCLUDED.created_date,
                last_updated = EXCLUDED.last_updated,
                category_path = EXCLUDED.category_path,
                category_depth = EXCLUDED.category_depth,
                source_system = EXCLUDED.source_system,
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
