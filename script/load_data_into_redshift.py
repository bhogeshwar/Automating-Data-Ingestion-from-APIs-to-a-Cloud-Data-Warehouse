# Load data from S3 to Redshift
import psycopg2
from script.redshift_create_table import db_name, db_user, db_password, redshift_endpoint, db_port
try:
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=redshift_endpoint,
        port=db_port
    )
    cur = conn.cursor()

    # Define the COPY command
    copy_command = """
    COPY weather_data
    FROM 's3://bk-automation-project-1/raw_data/weather_data.csv'
    IAM_ROLE 'arn:aws:iam::acc_id:role/DataIngestion-s3-to-redshift'
    CSV
    IGNOREHEADER 1;
    """
    
    # Execute the COPY command
    cur.execute(copy_command)
    conn.commit()
    print("Data successfully loaded into Redshift.")

except Exception as e:
    print(f"Error loading data into Redshift: {e}")

finally:
    if conn:
        cur.close()
        conn.close()
