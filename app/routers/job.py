from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional, List
from datetime import datetime
from services.job import JobService

router = APIRouter(prefix="/jobs", tags=["Jobs"])

# Dependency to get JobService instance
def get_job_service():
    return JobService()

# Create a new job
@router.post("/")
def create_job(connection_id: str, job_type: str = "sync", service: JobService = Depends(get_job_service)):
    """
    Create a new job (sync, reset, refresh, or clear).
    - **connection_id**: The connection ID to run the job on.
    - **job_type**: Type of job (sync, reset, refresh, clear). Defaults to "sync".
    """
    job = service.create_job(connection_id=connection_id, job_type=job_type)
    if not job:
        raise HTTPException(status_code=400, detail="Failed to create job.")
    return job

# Get job details by ID
@router.get("/{job_id}")
def get_job(job_id: str, service: JobService = Depends(get_job_service)):
    """
    Get details of a specific job by its ID.
    - **job_id**: The ID of the job to retrieve.
    """
    job = service.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    return job

# List jobs with filters
@router.get("/")
def list_jobs(
    connection_id: Optional[str] = Query(None, description="Filter by connection ID."),
    created_at_start: Optional[datetime] = Query(None, description="Filter jobs created after this date."),
    created_at_end: Optional[datetime] = Query(None, description="Filter jobs created before this date."),
    job_type: Optional[str] = Query(None, description="Filter by job type (sync, reset, refresh, clear)."),
    limit: Optional[int] = Query(20, description="Number of jobs to return (default: 20)."),
    offset: Optional[int] = Query(0, description="Pagination offset (default: 0)."),
    order_by: Optional[str] = Query("updatedAt|DESC", description="Sorting order (e.g., 'updatedAt|DESC')."),
    status: Optional[str] = Query(None, description="Filter by job status (e.g., SUCCEEDED, FAILED)."),
    updated_at_start: Optional[datetime] = Query(None, description="Filter jobs updated after this date."),
    updated_at_end: Optional[datetime] = Query(None, description="Filter jobs updated before this date."),
    workspace_ids: Optional[List[str]] = Query(None, description="Filter by workspace IDs."),
    service: JobService = Depends(get_job_service)
):
    """
    List jobs with optional filters and pagination.
    """
    jobs = service.list_jobs(
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
    if not jobs:
        raise HTTPException(status_code=404, detail="No jobs found matching the criteria.")
    return jobs