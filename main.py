import psycopg2

# Establish a connection
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="Loop_database",
    user="postgres",
    password="Ovd@0312"
)

# Create a cursor
cursor = conn.cursor()

# Execute a query
query = "SELECT * FROM business_hours"
cursor.execute(query)

# Fetch the results
rows = cursor.fetchall()

# Process the results
print(rows[0])
# Close the cursor and connection
cursor.close()
conn.close()
