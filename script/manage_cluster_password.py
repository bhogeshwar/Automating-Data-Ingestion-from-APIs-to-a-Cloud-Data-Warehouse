import boto3
import secrets
import string

# Generate a secure password
def generate_password(length=16):
    alphabet = string.ascii_letters + string.digits + string.punctuation
    password = ''.join(secrets.choice(alphabet) for _ in range(length))
    return password

# Store the generated password in Secrets Manager
def store_secret(secret_name, secret_value, region_name='us-east-2'):
    client = boto3.client('secretsmanager', region_name=region_name)
    
    try:
        # Create a new secret in Secrets Manager
        response = client.create_secret(
            Name=secret_name,
            SecretString=secret_value
        )
        print(f"Secret stored in Secrets Manager with ARN: {response['ARN']}")
    except Exception as e:
        print(f"Error storing secret: {e}")

# Generate a secure password
master_user_password = generate_password()

# Store the password in Secrets Manager
secret_name = "redshift-cluster-password"
store_secret(secret_name, master_user_password)
