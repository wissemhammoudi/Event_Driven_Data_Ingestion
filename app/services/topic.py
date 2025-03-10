from fastapi import FastAPI
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ResourceType
from confluent_kafka import KafkaException
from typing import Optional
from functools import wraps
from model.topic import CreateTopicRequest,UpdateTopicRequest


# Initialize FastAPI app
app = FastAPI()

# Decorator for error handling
def handle_kafka_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise ValueError(f"Validation error(s): {', '.join(error_messages)}") from e
        except KafkaException as e:
            raise Exception(f"Kafka error: {e}") from e
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}") from e
        except Exception as e:
            raise Exception(f"Unexpected error: {e}") from e
    return wrapper



# KafkaTopicService class
class KafkaTopicService:
    def __init__(self, kafka_broker: str):
        self.kafka_broker = kafka_broker  # Store the Kafka broker as an instance variable

    def get_admin_client(self):
        """Create and return an AdminClient instance using the stored Kafka broker."""
        return AdminClient({'bootstrap.servers': self.kafka_broker})

    @handle_kafka_errors
    def create_topic(self, request: CreateTopicRequest):
        """Creates a Kafka topic dynamically with user-defined partitions, replication factor, and retention policies."""
        admin_client = self.get_admin_client() 
        topic_name = request.topic_name
        num_partitions = request.num_partitions
        replication_factor = request.replication_factor
        retention_ms = request.retention_ms
        retention_bytes = request.retention_bytes

        config = {}
        if retention_ms is not None:
            config["retention.ms"] = str(retention_ms)
        if retention_bytes is not None:
            config["retention.bytes"] = str(retention_bytes)

        topic_list = [NewTopic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config=config
        )]

        futures = admin_client.create_topics(topic_list)
        for topic, future in futures.items():
            future.result(timeout=30)
        return {
            "message": f"Topic '{topic_name}' created successfully",
            "partitions": num_partitions,
            "replication_factor": replication_factor,
            "retention_policy": config
        }

    @handle_kafka_errors
    def list_topics(self):
        """Fetches all Kafka topics available in the broker."""
        admin_client = self.get_admin_client()  # Use the instance's kafka_broker
        topic_metadata = admin_client.list_topics(timeout=10)
        topics = list(topic_metadata.topics.keys())
        return {"topics": topics}

    @handle_kafka_errors
    def delete_topic(self, topic_name: str):
        """Deletes a Kafka topic."""
        if not topic_name.strip():
            raise ValueError("Topic name cannot be empty.")
        admin_client = self.get_admin_client()  # Use the instance's kafka_broker

        futures = admin_client.delete_topics([topic_name])
        for topic, future in futures.items():
            future.result(timeout=30)
        return {"message": f"Topic '{topic_name}' deleted successfully"}

    @handle_kafka_errors
    def update_topic(self, request: UpdateTopicRequest):
        """Updates the retention policies of an existing Kafka topic."""
        topic_name = request.topic_name.strip()
        if not topic_name:
            raise ValueError("Topic name cannot be empty.")

        admin_client = self.get_admin_client()  # Use the instance's kafka_broker

        config = {}
        if request.retention_ms is not None:
            config["retention.ms"] = str(request.retention_ms)
        if request.retention_bytes is not None:
            config["retention.bytes"] = str(request.retention_bytes)

        if not config:
            raise ValueError("At least one retention policy must be specified.")

        resources = [ConfigResource(ResourceType.TOPIC, topic_name, config)]
        futures = admin_client.alter_configs(resources)
        for resource, future in futures.items():
            future.result(timeout=30)

        return {"message": f"Topic '{topic_name}' updated successfully with new retention policies"}

    @handle_kafka_errors
    def get_topic_config(self, topic_name: str):
        """Fetches the configuration of a specific Kafka topic."""
        if not topic_name.strip():
            raise ValueError("Topic name cannot be empty.")
        admin_client = self.get_admin_client()  # Use the instance's kafka_broker

        config_resource = ConfigResource(ResourceType.TOPIC, topic_name)
        futures = admin_client.describe_configs([config_resource])
        result = futures[config_resource].result()

        configs = {}
        for key, entry in result.items():
            if entry.is_default:
                continue
            configs[key] = {
                "value": entry.value,
                "source": str(entry.source),
                "is_default": entry.is_default,
                "is_read_only": entry.is_read_only,
                "is_sensitive": entry.is_sensitive
            }
        return {"topic_name": topic_name, "config": configs}
