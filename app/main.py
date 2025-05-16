from fastapi import FastAPI, Request, status as httpStatus
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from sqlalchemy.exc import SQLAlchemyError
import traceback # For stack traces if needed for logging

from app.api.router import apiRouter
from app.core.exceptions import (
    BaseIcebergException, ErrorResponse, IcebergErrorModel,
    InternalServerErrorException, ValidationException # Added ValidationException for direct use
)
from app.db.models_db import Base 
from app.db.session import engine # Required for create_all

app = FastAPI(
    title="Iceberg REST Catalog",
    version="0.1.0",
    description="A REST Catalog implementation for Apache Iceberg compliant with the REST API spec.",
)

@app.on_event("startup")
async def onStartup():
    async with engine.begin() as conn:
        pass
        # await conn.run_sync(Base.metadata.drop_all) # Use with caution in dev
        # await conn.run_sync(Base.metadata.create_all)


@app.exception_handler(BaseIcebergException)
async def icebergExceptionHandler(request: Request, exc: BaseIcebergException):
    errorDetail = exc.detail["error"] if isinstance(exc.detail, dict) and "error" in exc.detail else {
        "message": exc.detail if isinstance(exc.detail, str) else str(exc.detail), # Fallback if detail is not standard
        "type": exc.errorType if hasattr(exc, 'errorType') else type(exc).__name__,
        "code": exc.status_code
    }
    if exc.stack and isinstance(errorDetail, dict): # Add stack if available and detail is dict
        errorDetail["stack"] = exc.stack

    return JSONResponse(
        status_code=exc.status_code,
        content={"error": errorDetail}
    )

@app.exception_handler(RequestValidationError)
async def validationExceptionHandler(request: Request, exc: RequestValidationError):
    errorMessages = []
    for error in exc.errors():
        loc = []
        for item in error["loc"]:
            if isinstance(item, int): # handle list indices
                loc.append(f"[{item}]")
            else:
                loc.append(str(item))
        
        locPath = ".".join(loc).replace(".[", "[") # body.[0].field -> body[0].field
        errorMessages.append(f"Field '{locPath}': {error['msg']}")
    
    # Use the ValidationException for consistent error structure
    validationError = ValidationException(message="Validation Error: " + "; ".join(errorMessages))

    return JSONResponse(
        status_code=validationError.status_code,
        content={"error": validationError.detail["error"]} 
    )

@app.exception_handler(SQLAlchemyError)
async def sqlalchemyExceptionHandler(request: Request, exc: SQLAlchemyError):
    
    
    serverError = InternalServerErrorException(message="A database error occurred.")
    serverError.errorType = type(exc).__name__ # Be more specific about the DB error type
    
    return JSONResponse(
        status_code=serverError.status_code,
        content={"error": serverError.detail["error"]}
    )

@app.exception_handler(Exception)
async def genericExceptionHandler(request: Request, exc: Exception):
    
    
    serverError = InternalServerErrorException(message="An unexpected internal server error occurred.")
    serverError.errorType = type(exc).__name__
    
    return JSONResponse(
        status_code=serverError.status_code,
        content={"error": serverError.detail["error"]}
    )

app.include_router(apiRouter)

@app.get("/", include_in_schema=False)
async def root():
    return {"message": "Welcome to the Iceberg REST Catalog!"}