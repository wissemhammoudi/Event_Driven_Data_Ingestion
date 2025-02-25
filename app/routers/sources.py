from fastapi import APIRouter, Depends
from services.source import SourceService

router = APIRouter(prefix="/sources", tags=["Sources"])

def get_source_service():
    return SourceService()

# 📌 Create a Source
@router.post("/")
def create_source(name: str, workspace_id: str,source_type: str, configuration: dict, service: SourceService = Depends(get_source_service)):
    return service.create_source(name, workspace_id, source_type,configuration)

# 📌 List all Sources
@router.get("/")
def list_sources(workspace_id: str, service: SourceService = Depends(get_source_service)):
    return service.list_sources(workspace_id)

# 📌 Get a Source by ID
@router.get("/{source_id}")
def get_source(source_id: str, service: SourceService = Depends(get_source_service)):
    return service.get_source(source_id)

# 📌 Delete a Source
@router.delete("/{source_id}")
def delete_source(source_id: str, service: SourceService = Depends(get_source_service)):
    return service.delete_source(source_id)
