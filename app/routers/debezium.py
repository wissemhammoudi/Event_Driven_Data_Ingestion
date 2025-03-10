from fastapi import APIRouter
from services.debezium import DebeziumService
from model.debezium import DebeziumConnectorPayload

router = APIRouter(prefix="/debezium", tags=["Debezium"])

# Initialize the DebeziumService
debezium_service = DebeziumService()

@router.post("/start/", status_code=201)
def start_producer(connector_payload: DebeziumConnectorPayload):
    """Starts a new Debezium connector."""
    return debezium_service.start_producer_debezium(connector_payload=connector_payload)

@router.get("/list/")
def list_debezium_connectors():
    """Lists all active Debezium connectors."""
    return debezium_service.list_debezium_connectors()

@router.get("/connector/{connector_name}/info/")
def get_connector_info(connector_name: str):
    """Fetches information about a specific Debezium connector."""
    return debezium_service.get_debezium_connector_info(connector_name=connector_name)

@router.delete("/connector/{connector_name}/delete/", status_code=204)
def stop_connector(connector_name: str):
    """Stops a specific Debezium connector."""
    return debezium_service.delete_debezium_connector(connector_name=connector_name)
