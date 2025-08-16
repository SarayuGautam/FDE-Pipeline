from psycopg2 import sql
from utils import execute_query, get_db_connection, get_entities, get_schemas

ENTITY = "users"


def load_users():
    """Load users data from Staging to Target"""

    schemas = get_schemas()
    entities = get_entities()
    conn = get_db_connection()

    try:
        # Truncate temp table before loading
        truncate_query = f"TRUNCATE TABLE {schemas['transform_schema']}.{entities[ENTITY]['temp_table']}"
        execute_query(conn, truncate_query)

        # Load to Temp
        load_to_temp_query = sql.SQL(
            """
          INSERT INTO {temp_table}
          (
            user_id, first_name, last_name, email, phone, age, gender, city, state, postal_code, country, source_system, source_loaded_at
          )
          SELECT
            user_id, first_name, last_name, email, phone, age, gender, city, state, postal_code, country, source_system, source_loaded_at
            FROM
            {staging_view}
          """
        ).format(
            temp_table=sql.Identifier(
                schemas["transform_schema"], entities[ENTITY]["temp_table"]
            ),
            staging_view=sql.Identifier(
                schemas["staging_schema"], entities[ENTITY]["staging_view"]
            ),
        )
        execute_query(conn, load_to_temp_query)

        # Load into Target (Upsert -> Merge -> Insert/Update)
        load_to_target_query = sql.SQL(
            """
          INSERT INTO {target_table}
          (
             user_id, first_name, last_name, email, phone, age, gender, city, state, postal_code, country, source_system
          )
          SELECT
            user_id, first_name, last_name, email, phone, age, gender, city, state, postal_code, country, source_system
          FROM
          {temp_table}
          ON CONFLICT(user_id) DO
          UPDATE SET
          first_name= EXCLUDED.first_name,
          last_name= EXCLUDED.last_name,
          email= EXCLUDED.email,
          phone= EXCLUDED.phone,
          age= EXCLUDED.age,
          gender= EXCLUDED.gender,
          city= EXCLUDED.city,
          state= EXCLUDED.state,
          postal_code= EXCLUDED.postal_code,
          country= EXCLUDED.country,
          source_system= EXCLUDED.source_system,
          updated_at= CURRENT_TIMESTAMP
          """
        ).format(
            temp_table=sql.Identifier(
                schemas["transform_schema"], entities[ENTITY]["temp_table"]
            ),
            target_table=sql.Identifier(
                schemas["target_schema"], entities[ENTITY]["target_table"]
            ),
        )
        execute_query(conn, load_to_target_query)

        print("Users Loaded Successfully")
    except Exception as e:
        print(f"Failed to load users data {str(e)}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    load_users()
