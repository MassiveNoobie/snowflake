import snowflake.connector

# Connect to Snowflake using your credentials
conn = snowflake.connector.connect(
  user='user_name',
  password='55553333',
  account='letters-xx5555'
)

# Create a cursor to execute queries
cursor = conn.cursor()

# Define your query to join two tables
query = "SELECT postal_code, country,date_valid_std,min_wind_speed_10m_mps,avg_wind_speed_10m_mps,max_wind_speed_10m_mps FROM SNOWPARK_FOR_PYTHON__HANDSONLAB__WEATHER_DATA.onpoint_id.history_day WHERE date_valid_std >= DATEADD(day,-21,current_date) AND EXTRACT(dayofweekiso,date_valid_std) = 5 ORDER BY postal_code,country,date_valid_std;"

# Execute the query
cursor.execute(query)

# Fetch and print the results
results = cursor.fetchall()
for result in results:
    print(result)

# Close the cursor and connection
cursor.close()
conn.close()
