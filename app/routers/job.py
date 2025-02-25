from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional, List
from datetime import datetime
from services.job import JobService
from airbyte_api import models

router = APIRouter(prefix="/jobs", tags=["Jobs"])

# Dependency to get JobService instance
def get_job_service():
    return JobService()

# ✅ Create a new job
@router.post("/")
def create_job(connection_id: str, job_type: str = "sync", service: JobService = Depends(get_job_service)):
    """Create a new job (sync, reset, refresh, or clear)."""
    try:
        job = service.start_job(connection_id=connection_id, job_type=job_type)
        if not job:
            raise HTTPException(status_code=400, detail="Failed to create job.")
        return job
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

# ✅ Get job details by ID (Renamed `check_job` to `get_job_status`)
@router.get("/{job_id}")
def get_job_status(job_id: str, service: JobService = Depends(get_job_service)):
    """Get the status of a specific job by its ID."""
    job = service.check_job(job_id)  # Fixed method name
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    return job

# ✅ Stop a running job
@router.post("/{job_id}/stop")
def stop_job(job_id: str, service: JobService = Depends(get_job_service)):
    """Stop a running job by its ID."""
    try:
        result = service.stop_job(job_id)
        if not result:
            raise HTTPException(status_code=400, detail="Failed to stop job.")
        return {"message": "Job stopped successfully", "job": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ✅ List jobs with filters
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
    """List jobs with optional filters and pagination."""
    try:
        job_type_enum = models.JobTypeEnum[job_type.upper()] if job_type else None
        status_enum = models.JobStatusEnum[status.upper()] if status else None

        jobs = service.list_jobs(
            connection_id=connection_id,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
            job_type=job_type_enum,
            limit=limit,
            offset=offset,
            order_by=order_by,
            status=status_enum,
            updated_at_start=updated_at_start,
            updated_at_end=updated_at_end,
            workspace_ids=workspace_ids
        )

        if not jobs:
            raise HTTPException(status_code=404, detail="No jobs found matching the criteria.")
        return jobs
    except KeyError:
        raise HTTPException(status_code=400, detail="Invalid job_type or status value.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
