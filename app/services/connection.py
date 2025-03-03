import airbyte_api
from airbyte_api import api, models
from core.config import Settings
import json
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

    def create_connection(self, name: str, source_id: str, destination_id: str, configuration: list, schedule: dict):
        """
        Create a connection in Airbyte.

        - `configuration`: A list of dictionaries, each representing a stream's configuration.
        Example:
        ```json
        [
            {
                "name": "example_stream",
                "syncMode": "incremental_append",
                "cursorField": ["updated_at"],
                "primaryKey": [["id"]]
            }
        ]
        ```

        - `schedule`: A dictionary specifying the schedule configuration.
        Example: `{"schedule_type": "cron", "cron_expression": "0 0 * * *"}`.
        """
        try:
            stream_configs = []

            for conf in configuration:
                # Ensure syncMode exists and is valid
                sync_mode = conf.get("syncMode", "full_refresh_overwrite").upper()  # default to full_refresh_overwrite
                if sync_mode not in models.ConnectionSyncModeEnum.__members__:
                    raise ValueError(f"Invalid syncMode: {sync_mode}")

                # Build StreamConfiguration object
                stream_config = models.StreamConfiguration(
                    name=conf["name"],  # Required field
                    sync_mode=models.ConnectionSyncModeEnum[sync_mode],
                    cursor_field=conf.get("cursorField", []),  # Default to empty list if not provided
                    primary_key=conf.get("primaryKey", [])  # Default to empty list if not provided
                )
                stream_configs.append(stream_config)

            # Validate and map schedule type
            schedule_type = schedule.get("schedule_type", "manual").upper()
            if schedule_type not in models.ScheduleTypeEnum.__members__:
                raise ValueError(f"Invalid schedule_type: {schedule_type}")

            # Build schedule object
            schedule_obj = models.AirbyteAPIConnectionSchedule(
                schedule_type=models.ScheduleTypeEnum[schedule_type],
                cron_expression=schedule.get("cron_expression") if schedule_type == "CRON" else None
            )

            # Create the connection request
            req = models.ConnectionCreateRequest(
                name=name,
                source_id=source_id,
                destination_id=destination_id,
                configurations=models.StreamConfigurations(streams=stream_configs),  # Corrected assignment
                schedule=schedule_obj  # Corrected schedule handling
            )
           
            # Send request
            response = self.client.connections.create_connection(request=req)

            # If you get a successful response, return the connection response object
            if response and hasattr(response, 'connection_response'):
                return response.connection_response
            else:
                raise Exception("Unexpected response format")

        except ValueError as ve:
            # Catch any validation errors and return them
            return {"error": f"Validation Error: {str(ve)}"}
        except Exception as e:
            # Catch all other errors and provide a helpful error message
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
