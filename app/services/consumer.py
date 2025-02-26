from confluent_kafka import Consumer, KafkaError
import threading
from .job import JobService


class KafkaConsumerService:
    def __init__(self, kafka_broker: str, job_service: JobService):
        self.kafka_broker = kafka_broker
        self.consumers = {}
        self.job_service = job_service
    def consume_messages(self, consumer_id: str, kafka_topic: str,connection_id:str,job_type:str):
        """Consumes messages from the specified Kafka topic."""
        consumer = self.consumers[consumer_id]['consumer']
        consumer.subscribe([kafka_topic])
        print(f"✅ Kafka consumer '{consumer_id}' started for topic: {kafka_topic}")

        try:
            while True:
                if not self.consumers.get(consumer_id, {}).get('running', False):
                    break

                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"🔚 End of partition reached: {msg.partition()}")
                    else:
                        print(f"❌ Error consuming message: {msg.error()}")
                else:
                    self.process_message(msg,connection_id,job_type)
        except Exception as e:
            print(f"❌ Exception in consumer loop: {e}")
        finally:
            consumer.close()
            print(f"❌ Kafka consumer '{consumer_id}' stopped.")

    def process_message(self, msg,connection_id,job_type, ):
        """Process and store the received Kafka message (string)."""
        try:
            # The message is expected to be a string, not JSON
            message = msg.value().decode('utf-8')
            print(f"✅ Received message: {message}")
            sync_response=self.job_service.start_job(connection_id=connection_id, job_type=job_type)
            print(f"🔄 Airbyte sync triggered: {sync_response}")
        except Exception as e:
            print(f"❌ Error processing message: {e}")

    def start_consumer(self, consumer_id: str, kafka_topic: str,connection_id,job_type):
        """Starts a new Kafka consumer for the specified topic."""
        if consumer_id in self.consumers:
            return {"message": f"Consumer with ID '{consumer_id}' is already running."}

        consumer = Consumer({
            'bootstrap.servers': self.kafka_broker,
            'group.id': f'{consumer_id}-group',
            'auto.offset.reset': 'earliest'
        })
        self.consumers[consumer_id] = {'consumer': consumer, 'running': True}

        thread = threading.Thread(target=self.consume_messages, args=(consumer_id, kafka_topic,connection_id,job_type), daemon=True)
        self.consumers[consumer_id]['thread'] = thread
        thread.start()

        return {"message": f"Started consumer with ID '{consumer_id}' for Kafka topic '{kafka_topic}'."}

    def stop_consumer(self, consumer_id: str):
        """Stops the specified Kafka consumer."""
        if consumer_id not in self.consumers:
            return {"message": f"Consumer with ID '{consumer_id}' is not running."}

        # Safely stop the consumer and its thread
        self.consumers[consumer_id]['running'] = False
        self.consumers[consumer_id]['thread'].join()
        consumer = self.consumers[consumer_id]['consumer']
        consumer.close()  # Ensure the consumer is properly closed
        del self.consumers[consumer_id]

        return {"message": f"Consumer with ID '{consumer_id}' stopped successfully."}

    def list_consumers(self):
        """Lists all active consumers."""
        active_consumers = [consumer_id for consumer_id, info in self.consumers.items() if info.get('running', False)]
        return {"active_consumers": active_consumers}
