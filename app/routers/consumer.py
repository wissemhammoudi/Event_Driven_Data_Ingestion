# routers/weather_router.py
from fastapi import APIRouter,Depends
from services.consumer import KafkaConsumerService
import threading
from core.config import Settings



router = APIRouter(prefix="/consumer", tags=["consumer"])
settings = Settings()
kafka_broker = settings.KAFKA_BROKER
kafka_service = KafkaConsumerService(kafka_broker)
@router.post("/start_consumer/",response_model=None)
def start_consumer(kafka_topic: str, consumer_id: str):
    """Starts the Kafka consumer thread."""
    response = kafka_service.start_consumer(consumer_id,kafka_topic)
    
    return response

@router.post("/stop/")
def stop_consumer( consumer_id: str):
    """Endpoint to stop the Kafka consumer for a specific topic."""
    response = kafka_service.stop_consumer(consumer_id)
    return response

@router.get("/list/")
def list_consumers():
    """Endpoint to list all active Kafka consumers."""
    response = kafka_service.list_consumers()
    return response