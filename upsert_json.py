import snowflake.connector
import json

# Snowflake connection details
def snowflake_connect():
    return snowflake.connector.connect(
        user='your_username',
        password='your_password',
        account='your_account',
        warehouse='your_warehouse',
        database='your_database',
        schema='your_schema'
    )

# Insert/Update function
def upsert_json_data(connection, table_name, json_data):
    """
    Inserts or updates JSON data into a Snowflake table.
    """
    cursor = connection.cursor()

    # Iterate through each record in the JSON data
    for record in json_data:
        # Convert preferences to a JSON string for VARIANT
        preferences = json.dumps(record["preferences"])

        # Upsert query
        upsert_query = f"""
        MERGE INTO {table_name} t
        USING (
            SELECT {record['id']} AS id,
                   '{record['name']}' AS name,
                   {record['age']} AS age,
                   PARSE_JSON('{preferences}') AS preferences
        ) s
        ON t.id = s.id
        WHEN MATCHED THEN
            UPDATE SET
                name = s.name,
                age = s.age,
                preferences = s.preferences
        WHEN NOT MATCHED THEN
            INSERT (id, name, age, preferences)
            VALUES (s.id, s.name, s.age, s.preferences);
        """
        # Execute the upsert query
        cursor.execute(upsert_query)

    print("Upsert completed successfully!")
    cursor.close()

# Main function
if __name__ == "__main__":
    # Sample JSON data to upsert
    sample_data = [
        {"id": 1, "name": "Alice", "age": 26, "preferences": {"likes": "books", "dislikes": "noise"}},
        {"id": 2, "name": "Bob", "age": 31, "preferences": {"likes": "traveling", "dislikes": "waiting"}},
        {"id": 6, "name": "Frank", "age": 29, "preferences": {"likes": "gaming", "dislikes": "interruptions"}}
    ]

    # Connect to Snowflake
    conn = snowflake_connect()

    try:
        # Upsert the sample data into the 'users_json' table
        upsert_json_data(conn, "users_json", sample_data)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the connection
        conn.close()
