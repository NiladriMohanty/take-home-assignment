import requests
import json
import time
from confluent_kafka import Producer

# Replace with your Kafka broker and topic details
bootstrap_servers = ''  # Change this if you are using Confluent Cloud
topic = 'station_status'

# Function to fetch data from the API
def fetch_data():
    url = 'https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_status.json'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data from API: {response.status_code}")

# Function to publish data to Kafka
def publish_to_kafka(producer, data):
    try:
        message = json.dumps(data)
        producer.produce(topic, value=message)
        producer.poll(0.0)  # Allow the producer to handle any delivery reports
        print("Message published to Kafka")
    except Exception as e:
        print(f"Failed to publish message: {e}")


# Main function
def main():
    producer = Producer({'bootstrap.servers': bootstrap_servers})

    try:
        while True:
            data = fetch_data()
            publish_to_kafka(producer, data)
            print("Data ingested and published to Kafka")

            # Delay between subsequent fetches (e.g., 10 seconds)
            time.sleep(10)
    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        # Ensure all outstanding messages are sent to Kafka
        producer.flush()


if __name__ == "__main__":
    main()
