# Q1

```bash
docker-compose exec redpanda-1 rpk version
```

it shows that the version for redpanda is 24.2.18. The full output is
Version:     v24.2.18
Git ref:     f9a22d4430
Build date:  2025-02-14T12:52:55Z
OS/Arch:     linux/amd64
Go version:  go1.23.1

Redpanda Cluster
node-1  v24.2.18 - f9a22d443087b824803638623d6b7492ec8221f9

# Q2

```bash
docker-compose exec redpanda-1 rpk topic create green-trips
```

The output is 

TOPIC       STATUS
green-trips  OK


# Q3

The output after running the command is True.


# Q4

```python
import json
import time 
import csv
import time

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()

selected_columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount'
]

topic_name = 'green-data'
csv_file = 'data/green_tripdata_2019-10.csv'  # change to your CSV file path if needed

start_time = time.time()

with open(csv_file, 'r', newline='', encoding='utf-8') as file:
    reader = csv.DictReader(file)

    for row in reader:
        # Each row will be a dictionary keyed by the CSV headers
        # Send data to Kafka topic "green-data"
        filtered_row = {key: row[key] for key in selected_columns}

        producer.send(topic_name, value=filtered_row)

# Make sure any remaining messages are delivered
producer.flush()
producer.close()

end_time = time.time()

# Calculate the time taken in seconds (rounded to a whole number)
time_taken_seconds = round(end_time - start_time)

print(f"✅ Successfully sent all data to Kafka in {time_taken_seconds} seconds.")
```

The output was around 70 seconds.


# Q5

Using hw6.pynb to load the data and session_job.py to load data from kafka to postgres, the pickup/dropoff location was id = 95 (borough = "Queens", zone = "Forest Hills") with 44 unbroken streak.