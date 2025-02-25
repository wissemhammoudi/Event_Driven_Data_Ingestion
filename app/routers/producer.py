from fastapi import APIRouter, HTTPException
from services.producer import KafkaProducerService
from core.config import Settings

router = APIRouter(prefix="/producer", tags=["Producers"])
settings = Settings()
kafka_broker = settings.KAFKA_BROKER
producer_service = KafkaProducerService(kafka_broker)

@router.post("/start/")
def start_producer(producer_id: str, kafka_topic: str, sending_mode: str = "synchronous"):
    """
    Starts a new Kafka producer.
    
    - **producer_id**: Unique identifier for the producer.
    - **kafka_topic**: The Kafka topic to send notifications.
    - **sending_mode**: "synchronous", "asynchronous", or "fire-and-forget".
    """
    try:
        response = producer_service.start_producer(producer_id, kafka_topic, sending_mode)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/stop/")
def stop_producer(producer_id: str):
    """
    Stops the Kafka producer identified by producer_id.
    """
    try:
        response = producer_service.stop_producer(producer_id)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/list/")
def list_producers():
    """
    Lists all active Kafka producers.
    """
    try:
        response = producer_service.list_producers()
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
