import requests
import pandas as pd
import boto3
from io import StringIO
import uuid

# Function to generate a unique bucket name
def generate_bucket_name():
    return f"bk-automation-{uuid.uuid4().hex[:8]}"

# Function to generate a unique object name
def generate_object_name():
    return f"raw_data/weather_data_{uuid.uuid4().hex[:8]}.csv"

# Create an S3 bucket
def create_s3_bucket(bucket_name, region='us-east-2'):
    s3 = boto3.client('s3', region_name=region)
    try:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': region
            }
        )
        print(f"S3 bucket '{bucket_name}' created successfully.")
    except Exception as e:
        print(f"Error creating S3 bucket: {e}")

# API Configuration
api_key = 'c10a5507a579764c92e8529cee4428a1'  # Replace with your OpenWeather API key
city = 'London'
api_url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"

# Fetch Data from API
response = requests.get(api_url)
data = response.json()

# Process Data into a DataFrame
weather_data = {
    'City': data['name'],
    'Temperature': data['main']['temp'],
    'Weather': data['weather'][0]['description'],
    'Humidity': data['main']['humidity'],
    'Timestamp': pd.to_datetime('now'),
}
df = pd.DataFrame([weather_data])

# Save Data to S3
s3 = boto3.client('s3')

# Generate random bucket and object names
bucket_name = generate_bucket_name()
object_name = generate_object_name()

# Create the S3 bucket
create_s3_bucket(bucket_name)

# Convert DataFrame to CSV and upload to S3
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

# Upload to S3
try:
    s3.put_object(Bucket=bucket_name, Key=object_name, Body=csv_buffer.getvalue())
    print(f"Data successfully uploaded to S3 bucket '{bucket_name}' with object name '{object_name}'.")
except Exception as e:
    print(f"Error uploading data to S3: {e}")
