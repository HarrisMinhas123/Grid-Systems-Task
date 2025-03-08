import boto3
import csv
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure boto3 with credentials from environment variables
session = boto3.Session(
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
)

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("IoTRawData")

def batch_write_to_dynamodb(items):
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)

with open("iot_sample_data.csv", "r") as f:
    reader = csv.DictReader(f)
    batch = []
    for row in reader:
        item = {
            "deviceId": row["deviceId"],
            "timestamp": row["timestamp"],
            "sensor1data": float(row["sensor1data"]),
            "sensor2data": float(row["sensor2data"]),
            "sensor3data": float(row["sensor3data"]),
            "sensor4data": float(row["sensor4data"]),
            "sensor5data": float(row["sensor5data"])
        }
        batch.append(item)
        if len(batch) == 20:
            batch_write_to_dynamodb(batch)
            print(f"Wrote 20 items to DynamoDB")
            batch = []
    
    if batch:
        batch_write_to_dynamodb(batch)
        print(f"Wrote {len(batch)} remaining items to DynamoDB")