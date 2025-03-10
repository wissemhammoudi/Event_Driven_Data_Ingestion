from pydantic import ValidationError
import httpx
from typing import Optional
from time import sleep
from model.debezium import DebeziumConnectorPayload
class DebeziumService:
    def __init__(self):
        pass
    def start_producer_debezium(self, connector_url: str = "http://host.docker.internal:8083/connectors", connector_payload: Optional[DebeziumConnectorPayload] = None):
        """
        Creates a Debezium connector by POSTing the provided payload, retrieves its configuration to construct the
        Kafka topic name, and starts a producer using that topic.
        
        Parameters:
            connector_url (str): Base URL for the connector API (e.g., "http://localhost:8083/connectors").
            connector_payload (DebeziumConnectorPayload): Pydantic model instance for the connector payload.
        
        Returns:
            dict: A message indicating success or error details.
        """
        try:
            # Validate the payload using Pydantic
            connector_payload = DebeziumConnectorPayload(**connector_payload.dict())  # Ensures proper validation
            payload_dict = connector_payload.dict()
            json_payload = {
                        "name": payload_dict["name"],  # Keep 'name' unchanged
                        "config": {
                            str(key).replace("_", "."): value for key, value in payload_dict["config"].items()
                        }
                    }
            # Create the Debezium connector
            create_response = httpx.post(
                connector_url,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                json=json_payload,
                timeout=5.0
            )

            # Handle response
            if create_response.status_code == 201:
                # Extract the response body, if successful
                response_json = create_response.json()
                topic_name = response_json.get("name", "Unknown topic")
                return {"result": f"The producer is working and connector name is : {topic_name}"}
            else:
                # If the request failed, return detailed error info
                return {"error": f"HTTP error occurred: {create_response.status_code}, {create_response.text}"}
    

        except ValidationError as ve:
            return {"error": f"Validation error: {str(ve)}"}
        except httpx.HTTPStatusError as e:
            return {"error": f"HTTP error occurred: {e.response.text}"}
        except httpx.RequestError as e:
            return {"error": f"Request failed: {str(e)}"}
        except Exception as e:
            return {"error": f"Unexpected error: {str(e)}"}

    def list_debezium_connectors(self, connector_url: str = "http://host.docker.internal:8083/connectors"):
        try:
            # Ensure URL has protocol
            if not connector_url.startswith(('http://', 'https://')):
                connector_url = f"http://{connector_url}"

            # Get list of Debezium connectors
            response =  httpx.get(
                connector_url,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                timeout=5.0
            )
            response.raise_for_status()

            # Return the JSON response directly since it contains the list of connectors
            return {"result": response.json()}

        except httpx.HTTPStatusError as e:
            return {"error": f"HTTP error occurred: {e.response.text}"}
        except httpx.RequestError as e:
            return {"error": f"Request failed: {str(e)}"}
        except Exception as e:
            return {"error": f"Unexpected error: {str(e)}"}

    def get_debezium_connector_info(self, connector_url: str = "http://host.docker.internal:8083/connectors", connector_name: str = ""):
        try:
           
            connector_info_url = f"{connector_url}/{connector_name}"
            response =  httpx.get(connector_info_url, timeout=5.0)
            response.raise_for_status()

            # Return the information about the connector
            return {"result": response.json()}

        except httpx.HTTPStatusError as e:
            return {"error": f"HTTP error occurred: {e.response.text}"}
        except httpx.RequestError as e:
            return {"error": f"Request failed: {str(e)}"}
        except Exception as e:
            return {"error": f"Unexpected error: {str(e)}"}

    def delete_debezium_connector(self, connector_url: str = "http://host.docker.internal:8083/connectors", connector_name: str = ""):
        try:
            

            stop_url = f"{connector_url}/{connector_name}"
            stop_response =  httpx.delete(
                stop_url,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                timeout=5.0
            )
            stop_response.raise_for_status()

            return {"result": f"Connector '{connector_name}' deleted successfully."}

        except httpx.HTTPStatusError as e:
            return {"error": f"HTTP error occurred: {e.response.text}"}
        except httpx.RequestError as e:
            return {"error": f"Request failed: {str(e)}"}
        except Exception as e:
            return {"error": f"Unexpected error: {str(e)}"}
