import csv
import time
from confluent_kafka import Producer

# Kafka producer 
producer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'queue.buffering.max.messages': 100000,  
    'queue.buffering.max.ms': 1000,  
    'batch.num.messages': 1000,  
    'request.timeout.ms': 120000,  
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
    for row in reader:
        try:
            producer.produce('nyc_taxi_rides', value=str(row), callback=delivery_report)
            producer.poll(0)
        except BufferError as e:
            print(f'Buffer full, waiting: {str(e)}')
            producer.poll(5)
            producer.produce('nyc_taxi_rides', value=str(row), callback=delivery_report)

        time.sleep(1)  


producer.flush()
