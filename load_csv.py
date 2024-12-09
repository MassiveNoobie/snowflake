import snowflake.connector
import pandas as pd
import os

# Step 1: Create sample CSV data
csv_file_path = "sample_data.csv"
sample_data = [
    {"id": 1, "name": "Alice", "age": 25, "signup_date": "2023-05-15"},
    {"id": 2, "name": "Bob", "age": 30, "signup_date": "2023-06-20"},
    {"id": 3, "name": "Charlie", "age": 35, "signup_date": "2023-07-05"},
    {"id": 4, "name": "Diana", "age": 28, "signup_date": "2023-08-12"},
    {"id": 5, "name": "Eve", "age": 32, "signup_date": "2023-09-01"}
]

# Save the data to a CSV file
df = pd.DataFrame(sample_data)
df.to_csv(csv_file_path, index=False)

print(f"Sample CSV created at: {csv_file_path}")

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
CREATE OR REPLACE TABLE users (
    id INT,
    name VARCHAR,
    age INT,
    signup_date DATE
);
"""
cur.execute(create_table_query)
print("Table created successfully!")

# Step 4: Stage the CSV file
put_file_query = f"PUT file://{os.path.abspath(csv_file_path)} @%users"
cur.execute(put_file_query)
print("File staged successfully!")

# Step 5: Load the data into the table
copy_into_query = """
COPY INTO users
FROM @%users
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
"""
cur.execute(copy_into_query)
print("Data loaded into Snowflake successfully!")

# Step 6: Verify the data
cur.execute("SELECT * FROM users")
rows = cur.fetchall()
print("Data in the 'users' table:")
for row in rows:
    print(row)

# Step 7: Clean up (optional)
cur.close()
conn.close()

# Remove the CSV file if you want to clean up
if os.path.exists(csv_file_path):
    os.remove(csv_file_path)
    print(f"Deleted sample CSV file: {csv_file_path}")
