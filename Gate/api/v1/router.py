from fastapi import APIRouter

from api.v1.endpoints import generation_router, prediction_router

router = APIRouter(prefix='/api/v1')

router.include_router(generation_router)
router.include_router(prediction_router)
