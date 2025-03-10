from pydantic import BaseModel
from confluent_kafka import Consumer, KafkaError
import threading
import logging
from functools import wraps
from typing import Optional
from pydantic import BaseModel
from typing import Optional

class ConsumerCreationRequest(BaseModel):
    consumer_id: str
    kafka_topic: str
    connection_id: str
    job_type: str
    auto_offset_reset: Optional[str] = "earliest"  # Defaulting to "earliest"

# Exception Handling Decorator
def handle_exceptions(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Error in {func.__name__}: {e}")
            return {"message": f"An error occurred while processing the request: {str(e)}"}
    return wrapper

# Pydantic Model for Consumer Creation
class ConsumerCreationRequest(BaseModel):
    consumer_id: str
    kafka_topic: str
    connection_id: str
    job_type: str
    auto_offset_reset: Optional[str] = "earliest"  # Defaulting to "earliest"

class KafkaConsumerService:
    def __init__(self, kafka_broker: str, job_service: JobService):
        self.kafka_broker = kafka_broker
        self.job_service = job_service
        self.consumers = {}
        self.consumers_lock = threading.Lock()

    @handle_exceptions
    def start_consumer(self, consumer_id: str, kafka_topic: str, connection_id: str, job_type: str):
        with self.consumers_lock:
            if consumer_id in self.consumers:
                return {"message": f"Consumer with ID '{consumer_id}' is already running."}

            consumer = Consumer({
                'bootstrap.servers': self.kafka_broker,
                'group.id': f'{consumer_id}-group',
                'auto.offset.reset': 'earliest'
            })
            self.consumers[consumer_id] = {'consumer': consumer, 'running': True, 'topic': kafka_topic}

            thread = threading.Thread(
                target=self.consume_messages, 
                args=(consumer_id, kafka_topic, connection_id, job_type),
                daemon=True
            )
            self.consumers[consumer_id]['thread'] = thread
            thread.start()

        return {"message": f"Started consumer with ID '{consumer_id}' for Kafka topic '{kafka_topic}'."}

    def get_consumer_info(self, consumer_id: str):
        with self.consumers_lock:
            consumer_info = self.consumers.get(consumer_id)

        if not consumer_info:
            return {"message": f"Consumer with ID '{consumer_id}' is not running."}

        consumer = consumer_info.get('consumer')
        topic = consumer_info.get('topic')
        running = consumer_info.get('running')

        topic_partitions = consumer.assignment()
        offset_info = {}
        for partition in topic_partitions:
            offset_info[partition] = consumer.position(partition)

        consumer_group = consumer.config.get('group.id')
        
        return {
            "consumer_id": consumer_id,
            "topic": topic,
            "group_id": consumer_group,
            "running": running,
            "thread": "running" if running else "stopped",
            "offsets": offset_info,
            "poll_timeout": consumer.config.get('fetch.wait.max.ms', 'not set'),
            "consumer_info": str(consumer),
        }
