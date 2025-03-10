from fastapi import APIRouter, Depends, HTTPException
from core.config import Settings
from services.consumer import KafkaConsumerService
from services.job import JobService
from functools import lru_cache

router = APIRouter(prefix="/consumer", tags=["consumer"])

def get_settings() -> Settings:
    return Settings()

@lru_cache()
def get_kafka_service_singleton() -> KafkaConsumerService:
    settings = get_settings()
    job_service = JobService()  # Initialize the JobService here
    return KafkaConsumerService(settings.KAFKA_BROKER, job_service)

def get_kafka_service(
    kafka_service: KafkaConsumerService = Depends(get_kafka_service_singleton)
) -> KafkaConsumerService:
    return kafka_service

@router.post("/start", response_model=dict)
def start_consumer(
    kafka_topic: str,
    consumer_id: str,
    connection_id: str,
    job_type: str,
    kafka_service: KafkaConsumerService = Depends(get_kafka_service)
):
    try:
        response = kafka_service.start_consumer(consumer_id, kafka_topic, connection_id, job_type)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/stop", response_model=dict)
def stop_consumer(
    consumer_id: str,
    kafka_service: KafkaConsumerService = Depends(get_kafka_service)
):
    try:
        response = kafka_service.stop_consumer(consumer_id)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/list", response_model=dict)
def list_consumers(
    kafka_service: KafkaConsumerService = Depends(get_kafka_service)
):
    try:
        response = kafka_service.list_consumers()
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/info/{consumer_id}", response_model=dict)
def get_consumer_info(
    consumer_id: str,
    kafka_service: KafkaConsumerService = Depends(get_kafka_service)
):
    try:
        response = kafka_service.get_consumer_info(consumer_id)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
