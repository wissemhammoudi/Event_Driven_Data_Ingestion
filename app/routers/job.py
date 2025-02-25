from fastapi import APIRouter, Depends
from services.job_service import JobService
from pydantic import BaseModel

router = APIRouter(prefix="/jobs", tags=["Jobs"])

def get_job_service():
    return JobService()

# Pydantic model for job creation request
class JobCreateSchema(BaseModel):
    connection_id: str
    job_type: str = "sync"  # Default to sync jobs

@router.post("/")
def create_job(request: JobCreateSchema, service: JobService = Depends(get_job_service)):
    return service.create_job(
        connection_id=request.connection_id,
        job_type=request.job_type
    )

@router.get("/{job_id}")
def get_job(job_id: str, service: JobService = Depends(get_job_service)):
    return service.get_job(job_id)
