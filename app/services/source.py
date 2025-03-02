import airbyte_api
from airbyte_api import api, models
from core.config import Settings

settings = Settings()
airbyte_username = settings.USERNAME
airbyte_password = settings.PASSWORD
airbyte_url = settings.URL


class SourceService:
    def __init__(self):
        """Initialize Airbyte API client with authentication"""
        self.client = airbyte_api.AirbyteAPI(
            server_url=airbyte_url,
            security=models.Security(
                basic_auth=models.SchemeBasicAuth(
                    username=airbyte_username,
                    password=airbyte_password
                )
            )
        )

    # 📌 Create a Source (Generic)
    def create_source(self, name: str, workspace_id: str, source_type: str, config: dict):
        """
        Create a source with the given configuration.
        
        Parameters:
          - name: Name of the source.
          - workspace_id: The workspace in which to create the source.
          - source_type: A string indicating which source type to use (e.g., "postgres", "mysql", "mongodb").
          - config: A dictionary containing the configuration values for the source.
        
        Returns:
          The response from the Airbyte API or an error message.
        """
        try:
            # Build the source configuration object based on the provided type.
            config_obj = self._build_source_configuration(source_type, config)
            response = self.client.sources.create_source(
                request=models.SourceCreateRequest(
                    configuration=config_obj,
                    name=name,
                    workspace_id=workspace_id
                )
            )
            return response.source_response
        except Exception as e:
            return {"error": f"Failed to create source: {str(e)}"}
    def _build_source_configuration(self, source_type: str, config: dict):
        """
        Convert a dictionary into the appropriate Airbyte source configuration model.
        
        Supported types (extend as needed):
          - "postgres"  -> models.SourcePostgres
          - "mysql"     -> models.SourceMysql
          - "mongodb"   -> models.SourceMongodb
          # Add more mappings for other supported source types.
        """
        mapping = {
            "postgres": models.SourcePostgres,
            "mysql": models.SourceMysql,
            # Add additional mappings here as required.
        }
        key = source_type.replace(" ", "").lower()
        if key not in mapping:
            raise ValueError(f"Unsupported source type: {source_type}")
        config_class = mapping[key]
        # Create an instance of the configuration model using the dictionary values.
        return config_class(**config)
    # 📌 List all Sources
    def list_sources(self, workspace_id: str):
        try:
            response = self.client.sources.list_sources(
                request=api.ListSourcesRequest(workspace_ids=[workspace_id])
            )
            print("Full Response:", response)
            return response.sources_response.data if response.sources_response else []
        except Exception as e:
            return {"error": f"Failed to list sources: {str(e)}"}


    # 📌 Get a Source by ID
    def get_source(self, source_id: str):
        try:
            response = self.client.sources.get_source(
                request=api.GetSourceRequest(source_id=source_id)
            )
            return response.source_response
        except Exception as e:
            return {"error": f"Failed to get source: {str(e)}"}

    # 📌 Delete a Source
    def delete_source(self, source_id: str):
        try:
            response = self.client.sources.delete_source(
                request=api.DeleteSourceRequest(source_id=source_id)
            )
            return {"message": "Source deleted successfully"} if response else {"error": "Failed to delete source"}
        except Exception as e:
            return {"error": f"Failed to delete source: {str(e)}"}
