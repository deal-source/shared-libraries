from fastapi import APIRouter
from app.business.pipeline import run_pipeline

router = APIRouter()

@router.post("/run")
def run_pipeline_endpoint():
    run_pipeline()
    return {"status": "Pipeline triggered"}