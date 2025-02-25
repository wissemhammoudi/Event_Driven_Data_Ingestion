import os
from dotenv import load_dotenv
from fastapi import HTTPException
import airbyte_api
from airbyte_api import models, api
from core.config import Settings
settings = Settings()
kafka_broker = settings.KAFKA_BROKER
airbyte_username = settings.USERNAME
airbyte_password = settings.PASSWORD
airbyte_url = settings.URL



class WorkspaceService:
    def __init__(self):
        try:
            self.client = airbyte_api.AirbyteAPI(
                server_url=airbyte_url,
                security=models.Security(
                    basic_auth=models.SchemeBasicAuth(
                        password=airbyte_password,
                        username=airbyte_username,
                    ),
                ),
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to initialize Airbyte client: {str(e)}")

    def create_workspace(self, name: str):
        try:
            res = self.client.workspaces.create_workspace(
                request=models.WorkspaceCreateRequest(name=name)
            )
            if res.workspace_response:
                return res.workspace_response
            raise HTTPException(status_code=400, detail="Workspace creation failed")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error creating workspace: {str(e)}")

    def list_workspaces(self):
        try:
            res = self.client.workspaces.list_workspaces(request=api.ListWorkspacesRequest())
            if res.workspaces_response:
                return res.workspaces_response
            raise HTTPException(status_code=404, detail="No workspaces found")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error fetching workspaces: {str(e)}")

    def get_workspace(self, workspace_id: str):
        try:
            res = self.client.workspaces.get_workspace(
                request=api.GetWorkspaceRequest(workspace_id=workspace_id)
            )
            if res.workspace_response:
                return res.workspace_response
            raise HTTPException(status_code=404, detail="Workspace not found")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error retrieving workspace: {str(e)}")

    def update_workspace(self, workspace_id: str, new_name: str):
        try:
            res = self.client.workspaces.update_workspace(
                request=api.UpdateWorkspaceRequest(
                    workspace_update_request=models.WorkspaceUpdateRequest(name=new_name),
                    workspace_id=workspace_id,
                )
            )
            if res:
                return {"message": "Workspace updated successfully"}
            raise HTTPException(status_code=400, detail="Workspace update failed")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error updating workspace: {str(e)}")

    def delete_workspace(self, workspace_id: str):
        try:
            res = self.client.workspaces.delete_workspace(
                request=api.DeleteWorkspaceRequest(workspace_id=workspace_id)
            )
            if res:
                return {"message": "Workspace deleted successfully"}
            raise HTTPException(status_code=400, detail="Workspace deletion failed")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error deleting workspace: {str(e)}")
