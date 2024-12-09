import snowflake.connector
import os
import json

# Step 1: Create sample JSON data
json_file_path = "sample_data.json"
sample_data = [
    {"id": 1, "name": "Alice", "age": 25, "preferences": {"likes": "reading", "dislikes": "noise"}},
    {"id": 2, "name": "Bob", "age": 30, "preferences": {"likes": "traveling", "dislikes": "waiting"}},
    {"id": 3, "name": "Charlie", "age": 35, "preferences": {"likes": "music", "dislikes": "silence"}},
    {"id": 4, "name": "Diana", "age": 28, "preferences": {"likes": "dancing", "dislikes": "rain"}},
    {"id": 5, "name": "Eve", "age": 32, "preferences": {"likes": "coding", "dislikes": "bugs"}}
]

# Save the data to a JSON file
with open(json_file_path, 'w') as json_file:
    json.dump(sample_data, json_file, indent=4)

print(f"Sample JSON created at: {json_file_path}")

# Step 2: Connect to Snowflake
conn = snowflake.connector.connect(
    user='your_username',
    password='your_password',
    account='your_account',
    warehouse='your_warehouse',
    database='your_database',
    schema='your_schema'
)
cur = conn.cursor()

# Step 3: Create a target table
create_table_query = """
CREATE OR REPLACE TABLE users_json (
    id INT,
    name STRING,
    age INT,
    preferences VARIANT
);
"""
cur.execute(create_table_query)
print("Table created successfully!")

# Step 4: Stage the JSON file
put_file_query = f"PUT file://{os.path.abspath(json_file_path)} @%users_json"
cur.execute(put_file_query)
print("File staged successfully!")

# Step 5: Load the JSON data into the table
copy_into_query = """
COPY INTO users_json
FROM @%users_json
FILE_FORMAT = (TYPE = 'JSON');
"""
cur.execute(copy_into_query)
print("JSON data loaded into Snowflake successfully!")

# Step 6: Verify the data
cur.execute("SELECT * FROM users_json")
rows = cur.fetchall()
print("Data in the 'users_json' table:")
for row in rows:
    print(row)

# Step 7: Clean up (optional)
cur.close()
conn.close()

# Remove the JSON file if you want to clean up
if os.path.exists(json_file_path):
    os.remove(json_file_path)
    print(f"Deleted sample JSON file: {json_file_path}")
