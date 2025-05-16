from fastapi import APIRouter
from app.models.iceberg_models import CatalogConfig
from app.core.config import settings

router = APIRouter()

@router.get("/config", response_model=CatalogConfig)
async def getConfig():
    defaultProperties = {
        "warehouse": settings.icebergWarehousePath,
        "catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
        "table-default.write.format.default": "parquet",
        "table-default.write.wap.enabled": "false" 
    }
    
    overrideProperties = {
    }
    
    return CatalogConfig(
        default=defaultProperties,
        override=overrideProperties
    )