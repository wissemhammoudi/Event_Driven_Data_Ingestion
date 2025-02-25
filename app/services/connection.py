import airbyte_api
from airbyte_api import api, models
from core.config import Settings

settings = Settings()
airbyte_username = settings.USERNAME
airbyte_password = settings.PASSWORD
airbyte_url = settings.URL

class ConnectionService:
    def __init__(self):
        self.client = airbyte_api.AirbyteAPI(
            server_url=airbyte_url,
            security=models.Security(
                basic_auth=models.SchemeBasicAuth(
                    username=airbyte_username,
                    password=airbyte_password
                )
            )
        )

    def create_connection(self, name: str, source_id: str, destination_id: str, workspace_id: str, configuration: list, schedule: dict):
        """
        Create a connection.
        - `configuration`: a list of dictionaries where each dict represents a stream's configuration.
          For example: [{"name": "example_stream", "sync_mode": "full_refresh_overwrite"}]
        - `schedule`: a dict for the schedule configuration, e.g. {"schedule_type": "manual"}
        """
        try:
            # Convert list of configuration dicts to a list of StreamConfiguration objects
            stream_configs = []
            for conf in configuration:
                stream_configs.append(
                    models.StreamConfiguration(
                        name=conf.get("name"),
                        sync_mode=conf.get("sync_mode")
                    )
                )
            # Create the connection request.
            # (Note: The SDK might expect a different model for a full sync catalog;
            # adjust based on your actual Airbyte deployment.)
            req = models.ConnectionCreateRequest(
                name=name,
                source_id=source_id,
                destination_id=destination_id,
                workspace_id=workspace_id,
                configurations=models.StreamConfiguration(stream_configs),
                schedule=models.AirbyteAPIConnectionSchedule(
                    schedule_type=schedule.get("schedule_type")
                )
            )
            response = self.client.connections.create_connection(request=req)
            return response.connection_response
        except Exception as e:
            return {"error": f"Failed to create connection: {str(e)}"}

    def list_connections(self, workspace_id: str):
        try:
            response = self.client.connections.list_connections(
                request=api.ListConnectionsRequest(workspace_ids=[workspace_id])
            )
            return response.connections_response.data if response.connections_response else []
        except Exception as e:
            return {"error": f"Failed to list connections: {str(e)}"}

    def get_connection(self, connection_id: str):
        try:
            response = self.client.connections.get_connection(
                request=api.GetConnectionRequest(connection_id=connection_id)
            )
            return response.connection_response
        except Exception as e:
            return {"error": f"Failed to get connection: {str(e)}"}

    def delete_connection(self, connection_id: str):
        try:
            response = self.client.connections.delete_connection(
                request=api.DeleteConnectionRequest(connection_id=connection_id)
            )
            return {"message": "Connection deleted successfully"} if response else {"error": "Failed to delete connection"}
        except Exception as e:
            return {"error": f"Failed to delete connection: {str(e)}"}
