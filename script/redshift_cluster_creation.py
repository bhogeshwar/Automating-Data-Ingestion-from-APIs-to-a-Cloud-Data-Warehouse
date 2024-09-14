import boto3
import uuid
import json

# Initialize Redshift client
redshift = boto3.client('redshift', region_name='us-east-2')  # Specify your AWS region

def generate_cluster_identifier():
    return f"redshift-cluster-{uuid.uuid4().hex[:8]}"

# AWS Secrets Manager Configuration
secret_name = "redshift!redshift-cluster-1-awsuser" 
region_name = "us-east-2" 

client = boto3.client(
    service_name='secretsmanager',
    region_name=region_name
)

# Create the Redshift cluster
try:
    # Retrieve the secret value
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    # Extract the secret string
    secret = json.loads(get_secret_value_response['SecretString'])
    master_user_password = secret['password']  # Assuming your secret stores the password with the key 'password'
    master_username = secret['username']
    cluster_identifier = generate_cluster_identifier()
    db_name = 'dev'  # Use the correct database name, default is 'dev'
    node_type = 'dc2.large'  # Match the node type you are using
    
    # Create the Redshift cluster with Public Access enabled
    response = redshift.create_cluster(
        ClusterIdentifier=cluster_identifier,
        NodeType=node_type,
        MasterUsername=master_username,
        MasterUserPassword=master_user_password,
        DBName=db_name,
        ClusterType='single-node',  # Ensure this matches your cluster setup, adjust if needed
        IamRoles=['arn:aws:iam::act_id:role/DataIngestion-s3-to-redshift'],  # Use your actual IAM role ARN
        PubliclyAccessible=True  # Make the cluster publicly accessible
    )
    print(f"Cluster {cluster_identifier} is being created. This may take a few minutes.")
except Exception as e:
    print(f"Error creating cluster: {e}")
