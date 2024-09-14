import boto3,json,uuid,psycopg2,logging,pandas as pd,requests,string,secrets,time,os
from io import StringIO
# Configure logging
logging.basicConfig(
    filename='app.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Function to validate S3 data
def validate_s3_data(bucket_name, file_key, expected_columns):
    s3 = boto3.client('s3')
    try:
        logging.info(f"Validating S3 data for bucket: {bucket_name}, key: {file_key}")
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        data = response['Body'].read().decode('utf-8')

        df = pd.read_csv(StringIO(data))

        if list(df.columns) != expected_columns:
            logging.error(f"Schema validation failed. Expected columns: {expected_columns}, but got: {list(df.columns)}")
            return False
        else:
            logging.info("Schema validation passed.")

        missing_values = df.isnull().sum().sum()
        if missing_values > 0:
            logging.error(f"Data integrity validation failed. Missing values found: {missing_values}")
            return False
        else:
            logging.info("Data integrity validation passed. No missing values.")

        logging.info(f"Data types: {df.dtypes}")
        row_count = len(df)
        logging.info(f"Row count: {row_count}")
        return True

    except Exception as e:
        logging.error(f"Error validating S3 data: {e}")
        return False

# Function to create a table in Redshift
def create_table_in_redshift(conn):
    try:
        cur = conn.cursor()
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
        logging.info("Table 'weather_data' created successfully.")
    except Exception as e:
        logging.error(f"Error creating table in Redshift: {e}")
    finally:
        cur.close()

# Function to load data from S3 to Redshift
def load_data_from_s3_to_redshift(conn, bucket_name, object_name, iam_role_arn):
    try:
        cur = conn.cursor()
        copy_command = f"""
        COPY weather_data
        FROM 's3://{bucket_name}/{object_name}'
        IAM_ROLE '{iam_role_arn}'
        CSV
        IGNOREHEADER 1;
        """
        cur.execute(copy_command)
        conn.commit()
        logging.info("Data successfully loaded into Redshift.")
    except Exception as e:
        logging.error(f"Error loading data into Redshift: {e}")
    finally:
        cur.close()

# Function to validate data in Redshift
def validate_redshift_data(conn):
    try:
        cur = conn.cursor()
        cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'weather_data';")
        columns = cur.fetchall()
        expected_schema = [('city', 'character varying'), ('temperature', 'double precision'),
                           ('weather', 'character varying'), ('humidity', 'integer'),
                           ('timestamp', 'timestamp without time zone')]

        if sorted(columns) != sorted(expected_schema):
            logging.error(f"Schema validation failed. Expected: {expected_schema}, but got: {columns}")
        else:
            logging.info("Schema validation passed.")

        cur.execute("SELECT COUNT(*) FROM weather_data;")
        row_count = cur.fetchone()[0]
        logging.info(f"Row count in Redshift: {row_count}")

    except Exception as e:
        logging.error(f"Error validating Redshift data: {e}")
    finally:
        if cur:
            cur.close()

# Generate a unique Redshift cluster identifier
def generate_cluster_identifier():
    cluster_id = f"redshift-cluster-{uuid.uuid4().hex[:8]}"
    logging.info(f"Generated cluster identifier: {cluster_id}")
    return cluster_id

# Generate a secure password
def generate_password(length=16):
    # Ensure the password contains at least one uppercase, one lowercase, and one digit
    alphabet = string.ascii_letters + string.digits + "!#$%&()*+,-.:;<=>?[]^_`{|}~"  # Excludes invalid characters
    password = ''.join(secrets.choice(alphabet) for _ in range(length - 3))

    # Manually ensure inclusion of required character types
    password += secrets.choice(string.ascii_uppercase)  # Add at least one uppercase letter
    password += secrets.choice(string.ascii_lowercase)  # Add at least one lowercase letter
    password += secrets.choice(string.digits)           # Add at least one digit

    # Shuffle to prevent predictable patterns
    password = ''.join(secrets.choice(password) for _ in range(len(password)))

    logging.info("Generated a secure password that meets Redshift requirements.")
    return password

# Store the generated password in Secrets Manager
def store_secret(secret_name, secret_value, region_name='us-east-2'):
    client = boto3.client('secretsmanager', region_name=region_name)
    try:
        # Check if secret already exists
        existing_secret = client.describe_secret(SecretId=secret_name)
        if existing_secret:
            logging.info(f"Secret {secret_name} already exists.")
            return
    except client.exceptions.ResourceNotFoundException:
        pass  # Secret does not exist; proceed with creation

    try:
        response = client.create_secret(
            Name=secret_name,
            SecretString=json.dumps({'password': secret_value, 'username': 'awsuser'})
        )
        logging.info(f"Secret stored in Secrets Manager with ARN: {response['ARN']}")
    except client.exceptions.ResourceExistsException:
        logging.error(f"Secret {secret_name} already exists.")
    except Exception as e:
        logging.error(f"Error storing secret: {e}")

# Create an IAM role for Redshift with access to S3
def create_iam_role(role_name):
    iam_client = boto3.client('iam')
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "redshift.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    try:
        # Check if the role already exists
        existing_role = iam_client.get_role(RoleName=role_name)
        role_arn = existing_role['Role']['Arn']
        logging.info(f"IAM Role already exists with ARN: {role_arn}")
        return role_arn
    except iam_client.exceptions.NoSuchEntityException:
        logging.info(f"IAM Role {role_name} does not exist. Creating new role...")

    try:
        # Create the role
        response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='IAM Role for Redshift with S3 read and full Redshift access'
        )
        role_arn = response['Role']['Arn']
        logging.info(f"IAM Role created with ARN: {role_arn}")

        # Ensure the role is available before attaching policies
        time.sleep(10)  # Add a delay to ensure the role is fully available
        
        # Attach necessary policies
        iam_client.attach_role_policy(RoleName=role_name, PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')
        iam_client.attach_role_policy(RoleName=role_name, PolicyArn='arn:aws:iam::aws:policy/AmazonRedshiftFullAccess')
        logging.info("Attached S3 ReadOnly and Redshift FullAccess policies to the IAM role.")

        # Validate that the role and policies are correctly set up
        time.sleep(5)  # Add a small delay to ensure policies are fully attached
        return role_arn

    except iam_client.exceptions.MalformedPolicyDocumentException as e:
        logging.error(f"Malformed policy document: {e}")
    except iam_client.exceptions.LimitExceededException as e:
        logging.error(f"Role creation limit exceeded: {e}")
    except iam_client.exceptions.ServiceFailureException as e:
        logging.error(f"Service failure: {e}")
    except Exception as e:
        logging.error(f"Error creating IAM role: {e}")
        return None

# Create an S3 bucket
def create_s3_bucket(bucket_name, region='us-east-2'):
    s3 = boto3.client('s3', region_name=region)
    try:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': region}
        )
        logging.info(f"S3 bucket '{bucket_name}' created successfully.")
    except Exception as e:
        logging.error(f"Error creating S3 bucket: {e}")

# Upload data to S3
def upload_data_to_s3(bucket_name, object_name):
    api_key = 'c10a5507a579764c92e8529cee4428a1'

    city = 'London'
    api_url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"

    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()

        weather_data = {
            'City': data['name'],
            'Temperature': data['main']['temp'],
            'Weather': data['weather'][0]['description'],
            'Humidity': data['main']['humidity'],
            'Timestamp': pd.to_datetime('now'),
        }
        df = pd.DataFrame([weather_data])

        s3 = boto3.client('s3')
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=object_name, Body=csv_buffer.getvalue())
        logging.info(f"Data successfully uploaded to S3 bucket '{bucket_name}' with object name '{object_name}'.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching weather data: {e}")
    except Exception as e:
        logging.error(f"Error uploading data to S3: {e}")

# Create the Redshift cluster
# Create the Redshift cluster
def create_redshift_cluster(cluster_identifier, master_user_password, iam_role_arn):
    redshift = boto3.client('redshift', region_name='us-east-2')
    if not iam_role_arn:
        logging.error("IAM Role ARN is None. Cannot create Redshift cluster.")
        return
    try:
        response = redshift.create_cluster(
            ClusterIdentifier=cluster_identifier,
            NodeType='dc2.large',
            MasterUsername='awsuser',
            MasterUserPassword=master_user_password,
            DBName='dev',
            ClusterType='single-node',
            IamRoles=[iam_role_arn], 
            PubliclyAccessible=True
        )
        logging.info(f"Cluster {cluster_identifier} is being created. This may take a few minutes.")
    except Exception as e:
        logging.error(f"Error creating cluster: {e}")
        logging.error(f"Error details: {e.response['Error'] if hasattr(e, 'response') else 'No additional error details available.'}")


# Wait for the cluster to be available and retrieve its endpoint
def get_redshift_endpoint(cluster_identifier):
    redshift = boto3.client('redshift', region_name='us-east-2')
    initial_wait_time = 60  # Wait for 1 minute before the first check
    retry_wait_time = 30  # Wait time between subsequent checks

    # Initial wait before the first status check
    logging.info(f"Waiting for {initial_wait_time} seconds before checking cluster status...")
    time.sleep(initial_wait_time)
    while True:
        try:
            response = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)
            cluster_status = response['Clusters'][0]['ClusterStatus']
            if cluster_status == 'available':
                endpoint = response['Clusters'][0]['Endpoint']['Address']
                port = response['Clusters'][0]['Endpoint']['Port']
                logging.info(f"Cluster {cluster_identifier} is available at {endpoint}:{port}.")
                return endpoint, port
            else:
                logging.info(f"Waiting for cluster {cluster_identifier} to become available...")
                time.sleep(retry_wait_time)
        except redshift.exceptions.ClusterNotFoundFault:
            logging.error(f"Cluster {cluster_identifier} not found. Retrying...")
            time.sleep(retry_wait_time)
        except Exception as e:
            logging.error(f"Error checking cluster status: {e}")
            time.sleep(retry_wait_time)

# Main workflow for full automation
if __name__ == "__main__":
    # Step 1: Create the IAM role
    role_name = "DataIngestionRole"
    iam_role_arn = create_iam_role(role_name)
    if iam_role_arn:
        print(f"Role created successfully with ARN: {iam_role_arn}")
    else:
        print("Failed to create IAM role. Check logs for details.")
    # Step 2: Generate cluster identifier and secure password, and store in Secrets Manager
    cluster_identifier = generate_cluster_identifier()
    master_user_password = generate_password()
    secret_name = "redshift-cluster-password"
    store_secret(secret_name, master_user_password)

    # Step 3: Create the S3 bucket and upload data
    bucket_name = f"bk-automation-{uuid.uuid4().hex[:8]}"
    object_name = f"raw_data/weather_data_{uuid.uuid4().hex[:8]}.csv"
    create_s3_bucket(bucket_name)
    upload_data_to_s3(bucket_name, object_name)

    # Step 4: Validate S3 Data before proceeding
    expected_columns = ['City', 'Temperature', 'Weather', 'Humidity', 'Timestamp']
    logging.info("Starting S3 data validation...")
    if validate_s3_data(bucket_name, object_name, expected_columns):
        logging.info("S3 data validation passed. Proceeding to Redshift data loading...")

        # Step 5: Create the Redshift cluster
        create_redshift_cluster(cluster_identifier, master_user_password, iam_role_arn)

        # Step 6: Wait for Redshift cluster to be available and get endpoint
        endpoint, port = get_redshift_endpoint(cluster_identifier)

        # Step 7: Connect to Redshift, create table, and load data
        try:
            conn = psycopg2.connect(
                dbname='dev',
                user='awsuser',
                password=master_user_password,
                host=endpoint,
                port=port
            )
            create_table_in_redshift(conn)
            load_data_from_s3_to_redshift(conn, bucket_name, object_name, iam_role_arn)
            validate_redshift_data(conn)
        except Exception as e:
            logging.error(f"Error connecting to Redshift or executing queries: {e}")
        finally:
            if conn:
                conn.close()
    else:
        logging.error("S3 data validation failed. Terminating process.")
