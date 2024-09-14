import boto3
import json
import psycopg2
import logging

logging.basicConfig(
    filename='app.log',  # Log file name
    level=logging.ERROR,  # Set the logging level to ERROR
    format='%(asctime)s - %(levelname)s - %(message)s'  # Log message format
)


# Specify your secret name (replace with your actual secret name or ARN)
secret_name = "redshift!redshift-cluster-1-awsuser"  # Replace with your full ARN or secret name
region_name = "us-east-2"  # Ensure this matches the region of your secret

# Initialize a session using your AWS credentials
client = boto3.client(
    service_name='secretsmanager',
    region_name=region_name
)

try:
    # Retrieve the secret value
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    # Extract the secret string
    secret = json.loads(get_secret_value_response['SecretString'])
    db_password = secret['password']  # Assuming your secret stores the password with the key 'password'
    db_user = secret['username']
    # Set up your connection details
    redshift_endpoint = 'redshift-cluster-1.cysdecvubmqw.us-east-2.redshift.amazonaws.com'
    db_name = 'dev'
    db_port = '5439'

    # Connect to Redshift
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=redshift_endpoint,
        port=db_port
    )
    cur = conn.cursor()
    print("Connected to Redshift")


    cur.execute("select  tablename from pg_tables where schemaname = 'public' and tablename = 'weather_data'")
    table_exists = cur.fetchone()
    
    if table_exists :
        print( "table arleady exists in the database")
    else :
    # Example query: Create a table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS weather_data (
            city VARCHAR(50),
            temperature FLOAT,
            weather VARCHAR(50),
            humidity INT,
            timestamp TIMESTAMP
        );
        """
        cur.execute(create_table_query)
        conn.commit()
        print("Table 'weather_data' created successfully.")

except Exception as e:
    # Log the error message to the log file
    logging.error(f"Error connecting to Redshift or creating table: {e}")
    print(f"Error: {e}")

finally:
    if conn:
        cur.close()
        conn.close()


