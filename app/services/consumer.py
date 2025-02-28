import threading
from confluent_kafka import Consumer, KafkaError
from .job import JobService

class KafkaConsumerService:
    def __init__(self, kafka_broker: str, job_service: JobService):
        self.kafka_broker = kafka_broker
        self.job_service = job_service
        self.consumers = {}
        self.consumers_lock = threading.Lock()  # Lock to protect shared data

    def consume_messages(self, consumer_id: str, kafka_topic: str, connection_id: str, job_type: str):
        with self.consumers_lock:
            consumer = self.consumers[consumer_id]['consumer']
        consumer.subscribe([kafka_topic])
        print(f"✅ Kafka consumer '{consumer_id}' started for topic: {kafka_topic}")

        try:
            while True:
                with self.consumers_lock:
                    running = self.consumers.get(consumer_id, {}).get('running', False)
                if not running:
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
                    self.process_message(msg, connection_id, job_type)
        except Exception as e:
            print(f"❌ Exception in consumer loop: {e}")
        finally:
            consumer.close()
            print(f"❌ Kafka consumer '{consumer_id}' stopped.")

    def process_message(self, msg, connection_id, job_type):
        try:
            # Decode the message assuming it is a UTF-8 encoded string.
            message = msg.value().decode('utf-8')
            print(f"✅ Received message: {message}")
            sync_response = self.job_service.start_job(connection_id=connection_id, job_type=job_type)
            print(f"🔄 Airbyte sync triggered: {sync_response}")
        except Exception as e:
            print(f"❌ Error processing message: {e}")

    def start_consumer(self, consumer_id: str, kafka_topic: str, connection_id: str, job_type: str):
        with self.consumers_lock:
            if consumer_id in self.consumers:
                return {"message": f"Consumer with ID '{consumer_id}' is already running."}

            consumer = Consumer({
                'bootstrap.servers': self.kafka_broker,
                'group.id': f'{consumer_id}-group',
                'auto.offset.reset': 'earliest'
            })
            # Register the consumer as running.
            self.consumers[consumer_id] = {'consumer': consumer, 'running': True}

            # Create and start the consumer thread.
            thread = threading.Thread(
                target=self.consume_messages, 
                args=(consumer_id, kafka_topic, connection_id, job_type),
                daemon=True
            )
            self.consumers[consumer_id]['thread'] = thread
            thread.start()

        return {"message": f"Started consumer with ID '{consumer_id}' for Kafka topic '{kafka_topic}'."}

    def stop_consumer(self, consumer_id: str):
        with self.consumers_lock:
            if consumer_id not in self.consumers:
                return {"message": f"Consumer with ID '{consumer_id}' is not running."}
            # Signal the consumer loop to stop.
            self.consumers[consumer_id]['running'] = False
            thread = self.consumers[consumer_id]['thread']

        # Wait for the consumer thread to exit.
        thread.join()

        with self.consumers_lock:
            # Remove the consumer from the registry.
            del self.consumers[consumer_id]

        return {"message": f"Consumer with ID '{consumer_id}' stopped successfully."}

    def list_consumers(self):
        with self.consumers_lock:
            active_consumers = [cid for cid, info in self.consumers.items() if info.get('running', False)]
        return {"active_consumers": active_consumers}
