import subprocess
import boto3
import time
import os
import json

dynamodb = boto3.client("dynamodb")
timestream = boto3.client("timestream-query")
sqs = boto3.client("sqs")
QUEUE_URL = "your-sqs-queue-url"  # Replace with actual SQS queue URL

def test_end_to_end():
    # Step 1: Generate CSV
    subprocess.run(["python", "generate_csv.py"])
    assert "iot_sample_data.csv" in os.listdir(), "CSV generation failed"
    
    # Step 2: Simulate SQS ingestion
    with open("iot_sample_data.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(row)
            )
    time.sleep(5)  # Wait for Lambda to process SQS
    
    # Check DynamoDB
    response = dynamodb.scan(TableName="IoTRawData")
    assert response["Count"] > 0, "DynamoDB load failed"
    
    # Step 3: Run hourly aggregation (simulate Lambda)
    subprocess.run(["python", "hourly_aggregation.py"])
    time.sleep(5)  # Wait for Timestream write
    query = f"SELECT COUNT(*) FROM five-min-table\""
    result = timestream.query(QueryString=query)
    assert int(result["Rows"][0]["Data"][0]["ScalarValue"]) > 0, "Timestream aggregation failed"
    
    print("End-to-end test passed!")

if __name__ == "__main__":
    test_end_to_end()