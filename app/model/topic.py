# Pydantic model for topic creation
from pydantic import BaseModel, Field
from typing import Optional

from pydantic import BaseModel, Field, validator, ValidationError

class CreateTopicRequest(BaseModel):
    topic_name: str = Field(..., min_length=1, strip_whitespace=True, description="Name of the Kafka topic")
    num_partitions: int = Field(default=1, ge=1, description="Number of partitions for the topic")
    replication_factor: int = Field(default=1, ge=1, description="Replication factor for the topic")
    retention_ms: Optional[int] = Field(default=None, ge=0, description="Retention time in milliseconds")
    retention_bytes: Optional[int] = Field(default=None, ge=0, description="Retention size in bytes")

    @validator("topic_name")
    def validate_topic_name(cls, value):
        if not value.strip():
            raise ValueError("Topic name cannot be empty.")
        return value

class UpdateTopicRequest(BaseModel):
    topic_name: str  # Include the topic_name in the request body
    retention_ms: Optional[int] = None
    retention_bytes: Optional[int] = None