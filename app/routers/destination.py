from fastapi import APIRouter, Depends
from services.destination import DestinationService

router = APIRouter(prefix="/destinations", tags=["Destinations"])

def get_destination_service():
    return DestinationService()

# 📌 Create a Destination
@router.post("/")
def create_destination(
    name: str,
    workspace_id: str,
    destination_type: str,  # Specify the type of destination (e.g., "postgres", "bigquery", etc.)
    configuration: dict,
    service: DestinationService = Depends(get_destination_service)
):
    return service.create_destination(name, workspace_id, destination_type, configuration)
# 📌 List all Destinations
@router.get("/")
def list_destinations(workspace_id: str, service: DestinationService = Depends(get_destination_service)):
    return service.list_destinations(workspace_id)

# 📌 Get a Destination by ID
@router.get("/{destination_id}")
def get_destination(destination_id: str, service: DestinationService = Depends(get_destination_service)):
    return service.get_destination(destination_id)

# 📌 Delete a Destination
@router.delete("/{destination_id}")
def delete_destination(destination_id: str, service: DestinationService = Depends(get_destination_service)):
    return service.delete_destination(destination_id)
