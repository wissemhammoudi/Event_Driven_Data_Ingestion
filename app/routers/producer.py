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
@router.post("/startdebezuim/")
def start_producer(connector_url: str = "http://host.docker.internal:8083/connectors", connector_payload: dict= {}):
    """
    Starts a new Kafka producer.
    
    - **producer_id**: Unique identifier for the producer.
    """
    try:
        response = producer_service.start_producer_Debezium(connector_url, connector_payload)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@router.get("/list/Debezium/")
def list_debeziumConnectors(connector_url: str = "http://host.docker.internal:8083/connectors"):
    """
    Lists all active debezium connectors.
    """
    try:
        # Ensure URL has protocol
        if not connector_url.startswith(('http://', 'https://')):
            connector_url = f"http://{connector_url}"
        response = producer_service.list_debeziumConnectors(connector_url)
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
@router.post("/stop/Debezium/")
def stop_debeziumConnector(connector_name: str):
    """
    Stops the debezium conn ector identified by connector_name.
    """
    try:
        response = producer_service.stop_debeziumConnector(connector_name)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
