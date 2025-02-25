from fastapi import APIRouter, Depends
from services.connection import ConnectionService
from pydantic import BaseModel
from typing import List

router = APIRouter(prefix="/connections", tags=["Connections"])

def get_connection_service():
    return ConnectionService()

# Pydantic model for connection creation request
class ConnectionCreateSchema(BaseModel):
    name: str
    source_id: str
    destination_id: str
    workspace_id: str
    # List of stream configuration dictionaries.
    configurations: List[dict]
    # Schedule configuration dictionary.
    schedule: dict

@router.post("/")
def create_connection(request: ConnectionCreateSchema, service: ConnectionService = Depends(get_connection_service)):
    return service.create_connection(
        name=request.name,
        source_id=request.source_id,
        destination_id=request.destination_id,
        workspace_id=request.workspace_id,
        configuration=request.configurations,
        schedule=request.schedule
    )

@router.get("/")
def list_connections(workspace_id: str, service: ConnectionService = Depends(get_connection_service)):
    return service.list_connections(workspace_id)

@router.get("/{connection_id}")
def get_connection(connection_id: str, service: ConnectionService = Depends(get_connection_service)):
    return service.get_connection(connection_id)

@router.delete("/{connection_id}")
def delete_connection(connection_id: str, service: ConnectionService = Depends(get_connection_service)):
    return service.delete_connection(connection_id)
