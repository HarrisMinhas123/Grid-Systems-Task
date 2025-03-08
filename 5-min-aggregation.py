import boto3
import datetime
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure boto3 with credentials from environment variables
session = boto3.Session(
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
)

dynamodb = boto3.client("dynamodb")
timestream = boto3.client("timestream-write")
DATABASE_NAME = "IoTTimeSeriesDB"
TABLE_NAME = "five-min-table"

def lambda_handler(event, context):
    now = datetime.datetime.utcnow()
    one_hour_ago = now - datetime.timedelta(hours=1)
    
    response = dynamodb.scan(
        TableName="IoTRawData",
        FilterExpression="timestamp BETWEEN :start AND :end",
        ExpressionAttributeValues={
            ":start": {"S": one_hour_ago.isoformat()},
            ":end": {"S": now.isoformat()}
        }
    )
    
    aggregates = {}
    for item in response["Items"]:
        ts = datetime.datetime.fromisoformat(item["timestamp"]["S"])
        window = ts - datetime.timedelta(minutes=ts.minute % 5, seconds=ts.second, microseconds=ts.microsecond)
        key = (item["deviceId"]["S"], window.isoformat())
        if key not in aggregates:
            aggregates[key] = {"s1": 0, "s2": 0, "s3": 0, "s4": 0, "s5": 0, "count": 0}
        agg = aggregates[key]
        agg["s1"] += float(item["sensor1data"]["N"])
        agg["s2"] += float(item["sensor2data"]["N"])
        agg["s3"] += float(item["sensor3data"]["N"])
        agg["s4"] += float(item["sensor4data"]["N"])
        agg["s5"] += float(item["sensor5data"]["N"])
        agg["count"] += 1
    
    records = []
    for (device_id, timestamp), agg in aggregates.items():
        records.append({
            "Dimensions": [{"Name": "deviceId", "Value": device_id}],
            "MeasureName": "sensor_data",
            "MeasureValues": [
                {"Name": "sensor1", "Value": str(agg["s1"] / agg["count"]), "Type": "DOUBLE"},
                {"Name": "sensor2", "Value": str(agg["s2"] / agg["count"]), "Type": "DOUBLE"},
                {"Name": "sensor3", "Value": str(agg["s3"] / agg["count"]), "Type": "DOUBLE"},
                {"Name": "sensor4", "Value": str(agg["s4"] / agg["count"]), "Type": "DOUBLE"},
                {"Name": "sensor5", "Value": str(agg["s5"] / agg["count"]), "Type": "DOUBLE"}
            ],
            "Time": str(int(datetime.datetime.fromisoformat(timestamp).timestamp() * 1000))
        })
    
    if records:
        timestream.write_records(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME, Records=records)
    
    return {"statusCode": 200, "body": "Aggregation completed"}