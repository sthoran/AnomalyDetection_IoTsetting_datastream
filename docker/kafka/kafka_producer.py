from confluent_kafka import Producer
import json
import random
import time
import pandas as pd

# Kafka configuration
BROKER = 'kafka:9092'
TOPIC = 'fictive_sensor_data'

# Callback for delivery reports
def delivery_report(err, msg):
    """Delivery report handler for produced messages."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Initialize Kafka producer
producer = Producer({'bootstrap.servers': BROKER})


#load json data batch
# Function to load JSON file
def load_json_file(filepath):
    with open(filepath, 'r') as file:
        return json.load(file)

# Main function to simulate the data stream
if __name__ == "__main__":
    json_file = "sensor_data.json"  # Path to the JSON file
    batch_data = load_json_file(json_file)  # Load the JSON file into a list of records

    try:
        for record in batch_data:
            # Send each record to the Kafka topic
            print(f"Sending record: {record}")
            producer.send(TOPIC, value=record)

            # Simulate a delay between records to mimic streaming
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nData stream stopped by user.")
    finally:
        producer.close()

