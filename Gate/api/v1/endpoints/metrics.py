import psutil

from fastapi import APIRouter, Response
from prometheus_client import generate_latest

from settings import prometheus_metrics


router = APIRouter(prefix="/metrics")


@router.get("")
async def metrics():
    prometheus_metrics.CPU_USAGE.set(psutil.cpu_percent())
    prometheus_metrics.MEMORY_USAGE.set(psutil.virtual_memory().percent)
    return Response(generate_latest(), media_type="text/plain")
