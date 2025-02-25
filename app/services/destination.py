import os
from dotenv import load_dotenv
import airbyte_api
from airbyte_api import api, models
from core.config import Settings

settings = Settings()
airbyte_username = settings.USERNAME
airbyte_password = settings.PASSWORD
airbyte_url = settings.URL

class DestinationService:
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

    # 📌 Create a Destination (Generic)
    def create_destination(self, name: str, workspace_id: str, destination_type: str, config: dict):
        """
        Create a destination.
        
        Parameters:
          - name: Name of the destination.
          - workspace_id: The workspace where the destination will reside.
          - destination_type: A string indicating which destination type to use (e.g., "postgres", "bigquery").
          - config: A dictionary with the configuration values required for the destination.
        
        Returns:
          The response from Airbyte's API or an error message.
        """
        try:
            # Build the configuration object based on the provided type.
            config_obj = self._build_destination_configuration(destination_type, config)
            response = self.client.destinations.create_destination(
                request=models.DestinationCreateRequest(
                    configuration=config_obj,
                    name=name,
                    workspace_id=workspace_id
                )
            )
            return response.destination_response
        except Exception as e:
            return {"error": f"Failed to create destination: {str(e)}"}
    
    def _build_destination_configuration(self, destination_type: str, config: dict):
        """
        Convert a dictionary into the appropriate Airbyte destination configuration
        based on the destination type.
        
        Supported types are defined in the mapping below.
        """
        mapping = {
            "googlesheets": models.DestinationGoogleSheets,
            "aws_datalake": models.DestinationAwsDatalake,
            "azure_blob_storage": models.DestinationAzureBlobStorage,
            "bigquery": models.DestinationBigquery,
            "databricks": models.DestinationDatabricks,
            "duckdb": models.DestinationDuckdb,
            "dynamodb": models.DestinationDynamodb,
            "elasticsearch": models.DestinationElasticsearch,
            "mongodb": models.DestinationMongodb,
            "mssql": models.DestinationMssql,
            "mysql": models.DestinationMysql,
            "oracle": models.DestinationOracle,
            "postgres": models.DestinationPostgres,
            "redis": models.DestinationRedis,
            "redshift": models.DestinationRedshift,
            "s3": models.DestinationS3,
            "snowflake": models.DestinationSnowflake,
            "snowflakecortex": models.DestinationSnowflakeCortex,
            
        }
        # Normalize the type string (for example, convert to lowercase without spaces).
        key = destination_type.replace(" ", "").lower()
        if key not in mapping:
            raise ValueError(f"Unsupported destination type: {destination_type}")
        config_class = mapping[key]
        # Create an instance of the configuration model using the dictionary values.
        return config_class(**config)
    # 📌 List all Destinations
    def list_destinations(self, workspace_id: str):
        try:
            response = self.client.destinations.list_destinations(
                request=api.ListDestinationsRequest()
            )
            return response.destinations_response.data if response.destinations_response else []
        except Exception as e:
            return {"error": f"Failed to list destinations: {str(e)}"}

    # 📌 Get a Destination by ID
    def get_destination(self, destination_id: str):
        try:
            response = self.client.destinations.get_destination(
                request=api.GetDestinationRequest(destination_id=destination_id)
            )
            return response.destination_response
        except Exception as e:
            return {"error": f"Failed to get destination: {str(e)}"}

    # 📌 Delete a Destination
    def delete_destination(self, destination_id: str):
        try:
            response = self.client.destinations.delete_destination(
                request=api.DeleteDestinationRequest(destination_id=destination_id)
            )
            return {"message": "Destination deleted successfully"} if response else {"error": "Failed to delete destination"}
        except Exception as e:
            return {"error": f"Failed to delete destination: {str(e)}"}
