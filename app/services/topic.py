from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ResourceType
from confluent_kafka import KafkaException
from typing import Optional

class KafkaTopicService:
    def __init__(self, kafka_broker: str):
        self.kafka_broker = kafka_broker

    def create_topic(self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1,
                     retention_ms: Optional[int] = None, retention_bytes: Optional[int] = None):
        """Creates a Kafka topic dynamically with user-defined partitions, replication factor, and retention policies."""
        if not topic_name.strip():
            raise ValueError("Topic name cannot be empty.")
        if num_partitions < 1:
            raise ValueError("Number of partitions must be at least 1.")
        if replication_factor < 1:
            raise ValueError("Replication factor must be at least 1.")

        # Build the configuration dictionary for topic-level settings
        config = {}
        if retention_ms is not None:
            config["retention.ms"] = str(retention_ms)
        if retention_bytes is not None:
            config["retention.bytes"] = str(retention_bytes)

        admin_client = AdminClient({'bootstrap.servers': self.kafka_broker})
        topic_list = [NewTopic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config=config
        )]

        try:
            futures = admin_client.create_topics(topic_list)
            for topic, future in futures.items():
                future.result(timeout=30)  # Wait for topic creation
            return {
                "message": f"Topic '{topic_name}' created successfully",
                "partitions": num_partitions,
                "replication_factor": replication_factor,
                "retention_policy": config
            }
        except KafkaException as e:
            raise Exception(f"Kafka error: {e}")
        except Exception as e:
            raise Exception(f"Unexpected error: {e}")

    def list_topics(self):
        """Fetches all Kafka topics available in the broker."""
        try:
            admin_client = AdminClient({'bootstrap.servers': self.kafka_broker})
            topic_metadata = admin_client.list_topics(timeout=10)
            topics = list(topic_metadata.topics.keys())
            return {"topics": topics}
        except KafkaException as e:
            raise Exception(f"Error fetching topics: {e}")
        except Exception as e:
            raise Exception(f"Unexpected error: {e}")

    def delete_topic(self, topic_name: str):
        """Deletes a Kafka topic."""
        if not topic_name.strip():
            raise ValueError("Topic name cannot be empty.")

        admin_client = AdminClient({'bootstrap.servers': self.kafka_broker})
        try:
            futures = admin_client.delete_topics([topic_name])
            for topic, future in futures.items():
                future.result(timeout=30)  # Wait for topic deletion
            return {"message": f"Topic '{topic_name}' deleted successfully"}
        except KafkaException as e:
            raise Exception(f"Kafka error: {e}")
        except Exception as e:
            raise Exception(f"Unexpected error: {e}")

    def update_topic(self, topic_name: str, retention_ms: Optional[int] = None, retention_bytes: Optional[int] = None):
        """Updates the retention policies of an existing Kafka topic."""
        if not topic_name.strip():
            raise ValueError("Topic name cannot be empty.")

        config = {}
        if retention_ms is not None:
            config["retention.ms"] = str(retention_ms)
        if retention_bytes is not None:
            config["retention.bytes"] = str(retention_bytes)

        if not config:
            raise ValueError("At least one retention policy must be specified.")

        admin_client = AdminClient({'bootstrap.servers': self.kafka_broker})
        try:
            resources = [ConfigResource(ResourceType.TOPIC, topic_name, config)]
            futures = admin_client.alter_configs(resources)
            for resource, future in futures.items():
                future.result(timeout=30)  # Wait for config update
            return {"message": f"Topic '{topic_name}' updated successfully with new retention policies"}
        except KafkaException as e:
            raise Exception(f"Kafka error: {e}")
        except Exception as e:
            raise Exception(f"Unexpected error: {e}")
    def get_topic_config(self, topic_name: str):
        """Fetches the configuration of a specific Kafka topic."""
        if not topic_name.strip():
            raise ValueError("Topic name cannot be empty.")

        admin_client = AdminClient({'bootstrap.servers': self.kafka_broker})
        try:
            # Create a ConfigResource for the topic
            config_resource = ConfigResource(ResourceType.TOPIC, topic_name)
            # Fetch configurations 
            futures = admin_client.describe_configs([config_resource])
            # Wait for the result (timeout in seconds)
            result = futures[config_resource].result()

            # Extract configurations
            configs = {}
            for key, entry in result.items():
                # Skip internal or default configurations if desired
                if entry.is_default:
                    continue
                configs[key] = {
                    "value": entry.value,
                    "source": str(entry.source),  # Source of the config (e.g., DYNAMIC_TOPIC_CONFIG)
                    "is_default": entry.is_default,
                    "is_read_only": entry.is_read_only,
                    "is_sensitive": entry.is_sensitive
                }
            return {"topic_name": topic_name, "config": configs}
        except KafkaException as e:
            raise Exception(f"Kafka error: {e}")
        except Exception as e:
            raise Exception(f"Unexpected error: {e}")