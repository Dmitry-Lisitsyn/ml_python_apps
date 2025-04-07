from api.v1.endpoints.metrics import router as metrics_router
from api.v1.endpoints.generation import router as generation_router
from api.v1.endpoints.prediction import router as prediction_router

__all__ = [
    'metrics_router',
    'generation_router',
    'prediction_router',
]
