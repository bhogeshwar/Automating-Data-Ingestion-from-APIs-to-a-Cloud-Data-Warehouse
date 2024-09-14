import boto3
import pandas as pd
from io import StringIO

# Initialize S3 client
s3 = boto3.client('s3')

# Function to validate S3 data
def validate_s3_data(bucket_name, file_key, expected_columns):
    try:
        # Download the file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        data = response['Body'].read().decode('utf-8')

        # Load the data into a pandas DataFrame
        df = pd.read_csv(StringIO(data))

        # Check if the DataFrame has the expected columns
        if list(df.columns) != expected_columns:
            print(f"Schema validation failed. Expected columns: {expected_columns}, but got: {list(df.columns)}")
        else:
            print("Schema validation passed.")

        # Check for missing values
        missing_values = df.isnull().sum().sum()
        if missing_values > 0:
            print(f"Data integrity validation failed. Missing values found: {missing_values}")
        else:
            print("Data integrity validation passed. No missing values.")

        # Check data types if needed
        print("Data types:", df.dtypes)

        # Example row count validation
        row_count = len(df)
        print(f"Row count: {row_count}")

    except Exception as e:
        print(f"Error validating S3 data: {e}")

# Example usage
bucket_name = 'bk-automation-project-1'
file_key = 'raw_data/weather_data.csv'
expected_columns = ['city', 'temperature', 'weather', 'humidity', 'timestamp']
validate_s3_data(bucket_name, file_key, expected_columns)
