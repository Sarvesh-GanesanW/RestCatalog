from fastapi import APIRouter
from app.api.v1 import namespace_router, table_router, config_router

apiRouter = APIRouter()

apiRouter.include_router(config_router.router, tags=["Configuration"])
apiRouter.include_router(namespace_router.router)
apiRouter.include_router(table_router.router)