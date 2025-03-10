from fastapi import APIRouter, HTTPException
from services.debezium import DebeziumService
from model.debezium import DebeziumConnectorPayload

router = APIRouter(prefix="/Debezium", tags=["Debezium"])

# Initialize the DebeziumService
producer_service = DebeziumService()

@router.post("/startdebezium/")
def start_producer(connector_url: str = "http://host.docker.internal:8083/connectors", connector_payload: DebeziumConnectorPayload = None):
    """
    Starts a new Kafka producer.

    - **connector_url**: The URL for the connector API.
    - **connector_payload**: The JSON payload to create the connector.
    """
    try:
        response = producer_service.start_producer_debezium(connector_url=connector_url, connector_payload=connector_payload)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/list/")
def list_debezium_connectors(connector_url: str = "http://host.docker.internal:8083/connectors"):
    """
    Lists all active Debezium connectors.
    
    - **connector_url**: Base URL for the connector API.
    """
    try:
        if not connector_url.startswith(('http://', 'https://')):
            connector_url = f"http://{connector_url}"

        response = producer_service.list_debezium_connectors(connector_url)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/connector/info/")
def get_connector_info(connector_url: str = "http://host.docker.internal:8083/connectors", connector_name: str = ""):
    """
    Fetches information about a specific Debezium connector.

    - **connector_url**: Base URL for the connector API.
    - **connector_name**: Name of the connector.
    """
    try:
        response = producer_service.get_debezium_connector_info(connector_url=connector_url, connector_name=connector_name)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/connector/stop/")
def stop_connector(connector_url: str = "http://host.docker.internal:8083/connectors", connector_name: str = ""):
    """
    Stops a specific Debezium connector.

    - **connector_url**: Base URL for the connector API.
    - **connector_name**: Name of the connector to stop.
    """
    try:
        response = producer_service.delete_debezium_connector(connector_url=connector_url, connector_name=connector_name)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
