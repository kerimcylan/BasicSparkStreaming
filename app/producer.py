import csv
import json
import time
from confluent_kafka import Producer

producer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'queue.buffering.max.messages': 100000,
    'queue.buffering.max.ms': 1000,
    'batch.num.messages': 1000,
    'request.timeout.ms': 60000,
    'delivery.timeout.ms': 120000
}
producer = Producer(producer_conf)

csv_file_path = '/app/Datasets/NYC_TaxiRide.csv'

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

with open(csv_file_path, 'r') as file:
    reader = csv.DictReader(file)
    count = 0
    for row in reader:
        json_data = json.dumps(row)
        while True:
            try:
                producer.produce('nyc_taxi_rides', value=json_data, callback=delivery_report)
                producer.poll(0)
                count += 1
                break
            except BufferError as e:
                print(f'Buffer full, waiting: {str(e)}')
                producer.poll(5)
                time.sleep(1)  
            time.sleep(1)

    print(f"Total messages produced: {count}")
    producer.flush()
