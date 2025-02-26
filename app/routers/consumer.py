from fastapi import APIRouter, Depends
from services.consumer import KafkaConsumerService
from core.config import Settings
from services.job import JobService

router = APIRouter(prefix="/consumer", tags=["consumer"])

# Dependency to get Settings
def get_settings():
    return Settings()

# Dependency to get KafkaConsumerService
def get_kafka_service(
    settings: Settings = Depends(get_settings),
    job_service: JobService = Depends(JobService)
):
    return KafkaConsumerService(settings.KAFKA_BROKER, job_service)

@router.post("/start_consumer/", response_model=None)
def start_consumer(
    kafka_topic: str,
    consumer_id: str,
    connection_id: str,
    job_type: str,
    kafka_service: KafkaConsumerService = Depends(get_kafka_service)
):
    response = kafka_service.start_consumer(consumer_id, kafka_topic, connection_id, job_type)
    return response

@router.post("/stop/")
def stop_consumer(
    consumer_id: str,
    kafka_service: KafkaConsumerService = Depends(get_kafka_service)
):
    response = kafka_service.stop_consumer(consumer_id)
    return response

@router.get("/list/")
def list_consumers(
    kafka_service: KafkaConsumerService = Depends(get_kafka_service)
):
    response = kafka_service.list_consumers()
    return response