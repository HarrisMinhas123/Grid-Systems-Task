import csv
import datetime
import random

devices = [f"device_{i:03d}" for i in range(1, 51)]
start_time = datetime.datetime(2025, 3, 8, 0, 0, 0)
interval = datetime.timedelta(minutes=5)

with open("iot_sample_data.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["deviceId", "timestamp", "sensor1data", "sensor2data", "sensor3data", "sensor4data", "sensor5data"])
    
    for device in devices:
        current_time = start_time
        for _ in range(288):  # 24 hours / 5 minutes = 288 intervals
            row = [
                device,
                current_time.isoformat(),
                random.uniform(0, 100),
                random.uniform(0, 100),
                random.uniform(0, 100),
                random.uniform(0, 100),
                random.uniform(0, 100)
            ]
            writer.writerow(row)
            current_time += interval

print("Sample CSV file generated: iot_sample_data.csv")