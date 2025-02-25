import os
from dotenv import load_dotenv
import airbyte_api
from airbyte_api import api, models

load_dotenv()
AIRBYTE_URL = os.getenv("AIRBYTE_URL", "http://localhost:8001/api/v1")
USERNAME = os.getenv("AIRBYTE_USERNAME", "airbyte")
PASSWORD = os.getenv("AIRBYTE_PASSWORD", "airbyte")

class JobService:
    def __init__(self):
        self.client = airbyte_api.AirbyteAPI(
            server_url=AIRBYTE_URL,
            security=models.Security(
                basic_auth=models.SchemeBasicAuth(
                    username=USERNAME,
                    password=PASSWORD
                )
            )
        )

    def create_job(self, connection_id: str, job_type: str = "sync"):
        """
        Create a job for the given connection.
        `job_type` should be a valid value. For sync jobs, use "sync" to default to models.JobTypeEnum.SYNC.
        """
        try:
            job_type_enum = models.JobTypeEnum.SYNC if job_type.lower() == "sync" else job_type
            req = models.JobCreateRequest(
                connection_id=connection_id,
                job_type=job_type_enum
            )
            response = self.client.jobs.create_job(request=req)
            return response.job_response
        except Exception as e:
            return {"error": f"Failed to create job: {str(e)}"}

    def get_job(self, job_id: str):
        try:
            response = self.client.jobs.get_job(
                request=api.GetJobRequest(job_id=job_id)
            )
            return response.job_response
        except Exception as e:
            return {"error": f"Failed to get job: {str(e)}"}
