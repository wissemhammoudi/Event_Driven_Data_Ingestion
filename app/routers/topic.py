from fastapi import APIRouter
from services.topic import KafkaTopicService
from core.config import Settings
from model.topic import CreateTopicRequest, UpdateTopicRequest

# Initialize router and settings
router = APIRouter(prefix="/topic", tags=["Topics"])
settings = Settings()
kafka_broker = settings.KAFKA_BROKER
topic_service = KafkaTopicService(kafka_broker)

@router.get("/list")
def list_topics():
    """Fetches all Kafka topics available in the broker."""
    return topic_service.list_topics()

@router.get("/config")
def get_topic_config(topic_name: str):
    """Fetches the configuration of a specific Kafka topic."""
    return topic_service.get_topic_config(topic_name)

@router.post("/create", status_code=201)
def create_topic(request: CreateTopicRequest):
    """Creates a Kafka topic dynamically with user-defined partitions, replication factor, and retention policies."""
    return topic_service.create_topic(request)

@router.delete("/delete", status_code=204)
def delete_topic(topic_name: str):
    """Deletes a Kafka topic."""
    return topic_service.delete_topic(topic_name)

@router.patch("/update")
def update_topic(request: UpdateTopicRequest):
    """Updates the retention policies of an existing Kafka topic."""
    return topic_service.update_topic(request)
