from pydantic import BaseModel, Field, validator, root_validator
from typing import Optional

class DebeziumConfig(BaseModel):
    connector_class: str 
    tasks_max: str 
    database_hostname: str 
    database_port: str 
    database_user: str 
    database_password: str 
    database_dbname: str 
    topic_prefix: str 
    schema_include_list: Optional[str] 
    publication_autocreate_mode: Optional[str] 
    table_include_list: Optional[str] 
    plugin_name: Optional[str] 
    snapshot_mode: Optional[str] 

   

 

class DebeziumConnectorPayload(BaseModel):
    name: str
    config: DebeziumConfig

    # Custom validation for 'name' to ensure it's not empty
    @validator('name')
    def validate_name(cls, value):
        if not value.strip():
            raise ValueError('Name cannot be empty.')
        return value
