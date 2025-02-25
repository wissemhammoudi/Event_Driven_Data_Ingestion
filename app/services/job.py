import airbyte_api
from airbyte_api import api, models
from core.config import Settings
from typing import List, Optional
from datetime import datetime
settings = Settings()
airbyte_username = settings.USERNAME
airbyte_password = settings.PASSWORD
airbyte_url = settings.URL
class JobService:
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

    # Start new sync job
    def start_job(self, connection_id: str, job_type: str = "sync"):
        """
        Start a job of a specific type.
        :param connection_id: The connection ID to run the job on.
        :param job_type: Type of job (sync, reset, refresh, clear).
        :return: Job response or None if failed.
        """
        try:
            # Map job_type string to JobTypeEnum
            job_type_upper = job_type.upper()
            if job_type_upper not in models.JobTypeEnum.__members__:
                raise ValueError(f"Invalid job type: {job_type}. Must be one of: sync, reset, refresh, clear.")

            job_type_enum = models.JobTypeEnum[job_type_upper]
            
            # Create and start the job
            job = self.client.jobs.create_job(
                models.JobCreateRequest(
                    connection_id=connection_id,
                    job_type=job_type_enum
                )
            )
            return job.job_response
        except Exception as e:
            print(f"Error starting job: {e}")
            return None
    # Get job status
    def check_job(self, job_id):
        try:
            status = self.client.jobs.get_job(api.GetJobRequest(job_id=job_id))
            return status.job_response
        except Exception as e:
            print(f"Error checking job: {e}")
            return None

    # Stop running job
    def stop_job(self, job_id):
        try:
            result = self.client.jobs.cancel_job(api.CancelJobRequest(job_id=job_id))
            return result.job_response
        except Exception as e:
            print(f"Error stopping job: {e}")
            return None

    # List recent jobs
    def list_jobs(
        self,
        connection_id: Optional[str] = None,
        created_at_start: Optional[datetime] = None,
        created_at_end: Optional[datetime] = None,
        job_type: Optional[models.JobTypeEnum] = None,
        limit: Optional[int] = 20,
        offset: Optional[int] = 0,
        order_by: Optional[str] = "updatedAt|DESC",
        status: Optional[models.JobStatusEnum] = None,
        updated_at_start: Optional[datetime] = None,
        updated_at_end: Optional[datetime] = None,
        workspace_ids: Optional[List[str]] = None
    ):
        """
        List jobs with filtering, sorting, and pagination.
        :param connection_id: Filter by connection ID.
        :param created_at_start: Filter jobs created after this date.
        :param created_at_end: Filter jobs created before this date.
        :param job_type: Filter by job type (e.g., SYNC, RESET).
        :param limit: Number of jobs to return (default: 20).
        :param offset: Pagination offset (default: 0).
        :param order_by: Sorting order (e.g., "updatedAt|DESC").
        :param status: Filter by job status (e.g., SUCCEEDED, FAILED).
        :param updated_at_start: Filter jobs updated after this date.
        :param updated_at_end: Filter jobs updated before this date.
        :param workspace_ids: Filter by workspace IDs.
        :return: List of jobs or None if failed.
        """
        try:
            # Build the request
            request = api.ListJobsRequest(
                connection_id=connection_id,
                created_at_start=created_at_start,
                created_at_end=created_at_end,
                job_type=job_type,
                limit=limit,
                offset=offset,
                order_by=order_by,
                status=status,
                updated_at_start=updated_at_start,
                updated_at_end=updated_at_end,
                workspace_ids=workspace_ids
            )

            # Fetch jobs
            response = self.client.jobs.list_jobs(request=request)
            return response.jobs_response.data
        except Exception as e:
            print(f"Error listing jobs: {e}")
            return None