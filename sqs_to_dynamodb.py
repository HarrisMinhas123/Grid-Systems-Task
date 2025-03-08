import boto3
import json
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

def lambda_handler(event, context):
    batch = []
    for record in event["Records"]:
        # Parse SQS message (assuming IoT Core sends JSON)
        message = json.loads(record["body"])
        item = {
            "deviceId": message["deviceId"],
            "timestamp": message["timestamp"],
            "sensor1data": float(message["sensor1data"]),
            "sensor2data": float(message["sensor2data"]),
            "sensor3data": float(message["sensor3data"]),
            "sensor4data": float(message["sensor4data"]),
            "sensor5data": float(message["sensor5data"])
        }
        batch.append(item)
        
        if len(batch) == 20:
            batch_write_to_dynamodb(batch)
            batch = []
    
    if batch:
        batch_write_to_dynamodb(batch)
    
    return {"statusCode": 200, "body": json.dumps({"message": "Processed SQS messages"})}