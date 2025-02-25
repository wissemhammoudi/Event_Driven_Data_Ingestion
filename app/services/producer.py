from confluent_kafka import Producer
import time
import threading

class KafkaProducerService:
    def __init__(self, kafka_broker: str):
        self.kafka_broker = kafka_broker
        # Dictionary to keep track of active producers.
        # Key: producer_id, Value: dict with producer instance, thread, topic, sending_mode, running flag.
        self.producers = {}

    def produce_update_notification(self, producer_id: str, kafka_topic: str, sending_mode: str = "synchronous"):
        """Continuously sends a notification message indicating a database update."""
        while self.producers[producer_id]['running']:
            message = "An update to the database has occurred."
            serialized_data = message.encode('utf-8')
            producer_instance = self.producers[producer_id]['producer']
            
            if sending_mode == "fire-and-forget":
                producer_instance.produce(kafka_topic, value=serialized_data)
            elif sending_mode == "synchronous":
                producer_instance.produce(kafka_topic, value=serialized_data)
                producer_instance.flush()
            elif sending_mode == "asynchronous":
                def delivery_report(err, msg):
                    if err is not None:
                        print(f"Delivery failed for message {msg.value()}: {err}")
                    else:
                        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                producer_instance.produce(kafka_topic, value=serialized_data, callback=delivery_report)
            else:
                print("Invalid sending mode specified.")
            
            time.sleep(10)  # Adjust the sleep duration as needed

    def start_producer(self, producer_id: str, kafka_topic: str, sending_mode: str = "synchronous"):
        """Starts a new Kafka producer with the given producer_id."""
        if producer_id in self.producers and self.producers[producer_id]['running']:
            return {"message": f"Producer '{producer_id}' is already running."}
        
        # Create a new Producer instance.
        p = Producer({'bootstrap.servers': self.kafka_broker})
        # Create and start a thread for producing messages.
        producer_thread = threading.Thread(
            target=self.produce_update_notification,
            args=(producer_id, kafka_topic, sending_mode),
            daemon=True
        )
        # Register the producer details.
        self.producers[producer_id] = {
            'producer': p,
            'thread': producer_thread,
            'running': True,
            'topic': kafka_topic,
            'sending_mode': sending_mode
        }
        producer_thread.start()
        return {"message": f"Started producer '{producer_id}' for Kafka topic '{kafka_topic}'."}

    def stop_producer(self, producer_id: str):
        """Stops the Kafka producer with the given producer_id."""
        if producer_id not in self.producers or not self.producers[producer_id]['running']:
            return {"message": f"Producer '{producer_id}' is not running."}
        
        # Signal the producer to stop.
        self.producers[producer_id]['running'] = False
        # Optionally wait for the thread to finish.
        self.producers[producer_id]['thread'].join(timeout=5)
        # Ensure all messages are flushed.
        self.producers[producer_id]['producer'].flush()
        # Remove the producer from the registry.
        del self.producers[producer_id]
        return {"message": f"Producer '{producer_id}' stopped successfully."}

    def list_producers(self):
        """Lists all active producers."""
        active = []
        for producer_id, info in self.producers.items():
            if info['running']:
                active.append({
                    'producer_id': producer_id,
                    'kafka_topic': info['topic'],
                    'sending_mode': info['sending_mode']
                })
        return {"active_producers": active}
