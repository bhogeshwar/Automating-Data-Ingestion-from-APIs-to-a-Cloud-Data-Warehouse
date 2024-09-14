import boto3
import json
import logging
import time

# Configure logging
logging.basicConfig(
    filename='app.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Function to create an IAM role for Redshift with access to S3
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

# Run the IAM role creation
if __name__ == "__main__":
    role_name = "DataIngestionRole"
    role_arn = create_iam_role(role_name)
    if role_arn:
        print(f"Role created successfully with ARN: {role_arn}")
    else:
        print("Failed to create IAM role. Check logs for details.")
