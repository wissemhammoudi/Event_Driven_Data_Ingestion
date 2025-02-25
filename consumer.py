from confluent_kafka import Consumer, KafkaError
import airbyte_api
from airbyte_api import models
import os
from dotenv import load_dotenv
import time
# Load environment variables from .env file
load_dotenv()

# Access the variables
password = os.getenv("password")
name = os.getenv("name")

# Initialize the Airbyte API client
s = airbyte_api.AirbyteAPI(
    server_url='http://localhost:8000/api/public/v1',
    security=models.Security(
        basic_auth=models.SchemeBasicAuth(
            password=password,
            username=name,
        ),
    ),
)

def custom_function():
    """Trigger an Airbyte sync job."""
    res = s.jobs.create_job(request=models.JobCreateRequest(
        connection_id='5c7f26a5-62c6-471e-9fce-4812b51c9cd2',  # Update with your connection ID
        job_type=models.JobTypeEnum.SYNC,
    ))
    if res.job_response is not None:
        print("✅ Airbyte sync triggered successfully.")
    else:
        print("❌ Failed to trigger Airbyte sync.")

def process_message(msg):
    """Process the received Kafka message and trigger the sync."""
    try:
        # Decode and print the message (for logging or debugging)
        message_str = msg.value().decode('utf-8')
        print(f"✅ Received message: {message_str}")
        
        # Trigger the Airbyte sync job
        custom_function()
    except Exception as e:
        print(f"❌ Error processing message: {e}")

def start_consumer(kafka_broker, kafka_topic):
    """Initialize and start the Kafka consumer."""
    consumer = Consumer({
        'bootstrap.servers': kafka_broker,
        'group.id': 'weather-consumer-group',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([kafka_topic])
    print(f"✅ Kafka consumer started for topic: {kafka_topic}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"🔚 End of partition reached: {msg.partition()}")
                else:
                    print(f"❌ Error consuming message: {msg.error()}")
            else:
                process_message(msg)
    except Exception as e:
        print(f"❌ Exception in consumer loop: {e}")
    finally:
        consumer.close()
        print("❌ Kafka consumer stopped.")

if __name__ == "__main__":
    kafka_broker = "localhost:9092"  # Update with your broker if different
    kafka_topic = "demo"             # Update with your topic name if needed
    start_consumer(kafka_broker, kafka_topic)
