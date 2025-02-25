from fastapi import APIRouter, HTTPException
from services.topic import KafkaTopicService
from core.config import Settings
from typing import Optional

router = APIRouter(prefix="/topic", tags=["Topics"])
settings = Settings()
kafka_broker = settings.KAFKA_BROKER
topic_service = KafkaTopicService(kafka_broker)

@router.get("/list")
def list_topics():
    """Fetches all Kafka topics available in the broker."""
    try:
        return topic_service.list_topics()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/config")
def get_topic_config(topic_name: str):
    """Fetches the configuration of a specific Kafka topic."""
    try:
        return topic_service.get_topic_config(topic_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/create")
def create_topic(
    topic_name: str,
    num_partitions: int = 1,
    replication_factor: int = 1,
    retention_ms: Optional[int] = None,
    retention_bytes: Optional[int] = None
):
    """Creates a Kafka topic dynamically with user-defined partitions, replication factor, and retention policies."""
    try:
        return topic_service.create_topic(
            topic_name,
            num_partitions,
            replication_factor,
            retention_ms,
            retention_bytes
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/delete")
def delete_topic(topic_name: str):
    """Deletes a Kafka topic."""
    try:
        return topic_service.delete_topic(topic_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.patch("/update")
def update_topic(
    topic_name: str,
    retention_ms: Optional[int] = None,
    retention_bytes: Optional[int] = None
):
    """Updates the retention policies of an existing Kafka topic."""
    try:
        return topic_service.update_topic(
            topic_name,
            retention_ms,
            retention_bytes
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
