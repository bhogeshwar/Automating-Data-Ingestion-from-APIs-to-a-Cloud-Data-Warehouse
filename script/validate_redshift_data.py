import psycopg2
from script.redshift_create_table import db_name, db_user, db_password, redshift_endpoint, db_port

# Connect to Redshift
try:
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=redshift_endpoint,
        port=db_port
    )
    cur = conn.cursor()


    # Schema validation: Check if table exists with the correct columns
    cur.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'weather_data';
    """)
    columns = cur.fetchall()
    expected_schema = [('city', 'character varying'), 
                        ('temperature', 'double precision'), 
                        ('weather', 'character varying'), 
                        ('humidity', 'integer'), 
                        ('timestamp', 'timestamp without time zone')]
    
    #The schema validation is failing because the columns are not sorted consistently on both sides. 
    # Even if the columns are present on both sides, their order discrepancy is causing the validation to fail.
    
    columns_sorted = sorted(columns) 
    expected_schema_sorted = sorted(expected_schema)

    if columns_sorted != expected_schema_sorted:
        print(f"Schema validation failed. Expected: {expected_schema}, but got: {columns}")
    else:
        print("Schema validation passed.")
    # Example validation query: Check row count
    cur.execute("SELECT COUNT(*) FROM weather_data;")
    row_count = cur.fetchone()[0]
    print(f"Row count in weather_data table: {row_count}")

    # Check for null values in important columns
    cur.execute("SELECT COUNT(*) FROM weather_data WHERE city IS NULL;")
    null_city_count = cur.fetchone()[0]
    if null_city_count > 0:
            print(f"Data integrity validation failed. Null values found in 'city' column: {null_city_count}")
    else:
            print("Data integrity validation passed. No null values in 'city' column.")

    # Example data type validation
    print("Data types in Redshift table:", columns)

except Exception as e:
    print(f"Error validating data in Redshift: {e}")

finally:
    if cur:
        cur.close()
    if conn:
        conn.close()
