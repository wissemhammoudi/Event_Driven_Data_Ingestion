from fastapi import APIRouter, Depends
from services.workspace import WorkspaceService
from airbyte_api import models, api

router = APIRouter(prefix="/workspace", tags=["Workspace"])


@router.post("/")
def create_workspace(name: str, service: WorkspaceService = Depends()):
    return service.create_workspace(name)


@router.get("/")
def list_workspaces(service: WorkspaceService = Depends()):
    return service.list_workspaces()


@router.get("/{workspace_id}")
def get_workspace(workspace_id: str, service: WorkspaceService = Depends()):
    return service.get_workspace(workspace_id)


@router.put("/{workspace_id}")
def update_workspace(workspace_id: str, new_name: str, service: WorkspaceService = Depends()):
    return service.update_workspace(workspace_id, new_name)


@router.delete("/{workspace_id}")
def delete_workspace(workspace_id: str, service: WorkspaceService = Depends()):
    return service.delete_workspace(workspace_id)
