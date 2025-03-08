import boto3
import datetime

timestream_query = boto3.client("timestream-query")
timestream_write = boto3.client("timestream-write")
DATABASE_NAME = "IoTTimeSeriesDB"
TABLE_NAME = "one-hour-table"

def lambda_handler(event, context):
    yesterday = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    query = f"""
        SELECT deviceId, BIN(time, 1h) as hour, 
               AVG(measure_value::double) as avg_sensor1,
               AVG(measure_value::double) as avg_sensor2,
               AVG(measure_value::double) as avg_sensor3,
               AVG(measure_value::double) as avg_sensor4,
               AVG(measure_value::double) as avg_sensor5
        FROM "{DATABASE_NAME}"."five-min-table"
        WHERE time >= '{yesterday.isoformat()}' AND time < '{yesterday + datetime.timedelta(days=1)}'
        GROUP BY deviceId, BIN(time, 1h)
    """
    response = timestream_query.query(QueryString=query)
    
    records = []
    for row in response["Rows"]:
        device_id = row["Data"][0]["ScalarValue"]
        hour = row["Data"][1]["ScalarValue"]
        records.append({
            "Dimensions": [{"Name": "deviceId", "Value": device_id}],
            "MeasureName": "sensor_data",
            "MeasureValues": [
                {"Name": "sensor1", "Value": row["Data"][2]["ScalarValue"], "Type": "DOUBLE"},
                {"Name": "sensor2", "Value": row["Data"][3]["ScalarValue"], "Type": "DOUBLE"},
                {"Name": "sensor3", "Value": row["Data"][4]["ScalarValue"], "Type": "DOUBLE"},
                {"Name": "sensor4", "Value": row["Data"][5]["ScalarValue"], "Type": "DOUBLE"},
                {"Name": "sensor5", "Value": row["Data"][6]["ScalarValue"], "Type": "DOUBLE"}
            ],
            "Time": str(int(datetime.datetime.fromisoformat(hour).timestamp() * 1000))
        })
    
    if records:
        timestream_write.write_records(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME, Records=records)
    
    return {"statusCode": 200, "body": "Hourly aggregation completed"}