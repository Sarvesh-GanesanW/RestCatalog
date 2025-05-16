from fastapi import APIRouter, Depends, status, Body, Query, Path
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional

from app.db.session import getDb
from app.crud.crud_namespace import crudNamespace
from app.models.iceberg_models import (
    Namespace, CreateNamespaceRequest, UpdateNamespacePropertiesRequest,
    UpdateNamespacePropertiesResponse
)
from app.core.exceptions import (
    BaseIcebergException, ErrorResponse, NoSuchNamespaceException,
    InternalServerErrorException
)

router = APIRouter(
    prefix="/v1/namespaces",
    tags=["Namespaces"],
    responses={
        400: {"model": ErrorResponse}, 401: {"model": ErrorResponse},
        403: {"model": ErrorResponse}, 404: {"model": ErrorResponse},
        409: {"model": ErrorResponse}, 
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
        503: {"model": ErrorResponse, "description": "Service Unavailable"}
    }
)

@router.get("", response_model=List[Namespace])
async def listNamespacesEndpoint(
    db: AsyncSession = Depends(getDb),
    parent: Optional[str] = Query(None, description="Parent namespace to list children of, e.g., 'db1' or 'db1.schema1'")
):
    parentLevels = parent.split('.') if parent else [] # Ensure parentLevels is always a list
    if parent == "": # Handle case where parent is an empty string, meaning root
        parentLevels = []
    elif parent is None: # Query(None) means list all top-level if no specific parent logic
        parentLevels = []


    try:
        dbNamespaces = await crudNamespace.listNamespaces(db, parent=parentLevels if parent is not None else None)
        return [Namespace(namespace=n.levels, properties=n.properties) for n in dbNamespaces]
    except BaseIcebergException as e:
        raise e
    except Exception as e:
        raise InternalServerErrorException(message=str(e))

@router.post("", response_model=Namespace, status_code=status.HTTP_200_OK)
async def createNamespaceEndpoint(
    request: CreateNamespaceRequest,
    db: AsyncSession = Depends(getDb)
):
    try:
        dbNamespace = await crudNamespace.createNamespace(db, request)
        return Namespace(namespace=dbNamespace.levels, properties=dbNamespace.properties)
    except BaseIcebergException as e:
        raise e
    except Exception as e:
        raise InternalServerErrorException(message=str(e))

@router.get("/{namespaceStr:path}", response_model=Namespace)
async def loadNamespaceEndpoint(
    namespaceStr: str = Path(..., description="Namespace string, e.g., 'db' or 'db.schema'"),
    db: AsyncSession = Depends(getDb)
):
    levels = namespaceStr.split('.')
    try:
        dbNamespace = await crudNamespace.getNamespaceByLevels(db, levels)
        if not dbNamespace:
            raise NoSuchNamespaceException(namespace=levels)
        return Namespace(namespace=dbNamespace.levels, properties=dbNamespace.properties)
    except BaseIcebergException as e:
        raise e
    except Exception as e:
        raise InternalServerErrorException(message=str(e))

@router.post("/{namespaceStr:path}/properties", response_model=UpdateNamespacePropertiesResponse)
async def updateNamespacePropertiesEndpoint(
    namespaceStr: str = Path(..., description="Namespace string"),
    request: UpdateNamespacePropertiesRequest = Body(...),
    db: AsyncSession = Depends(getDb)
):
    levels = namespaceStr.split('.')
    try:
        originalNamespace = await crudNamespace.getNamespaceByLevels(db, levels)
        if not originalNamespace:
            raise NoSuchNamespaceException(namespace=levels)
        
        originalProps = originalNamespace.properties or {}
        
        await crudNamespace.updateNamespaceProperties(
            db, levels, updates=request.updates or {}, removals=request.removals or []
        )
        
        updatedKeys = list(request.updates.keys()) if request.updates else []
        removedKeys = [key for key in (request.removals or []) if key in originalProps]
        missingKeys = [key for key in (request.removals or []) if key not in originalProps]

        return UpdateNamespacePropertiesResponse(
            updated=updatedKeys,
            removed=removedKeys,
            missing=missingKeys if missingKeys else None
        )
    except BaseIcebergException as e:
        raise e
    except Exception as e:
        raise InternalServerErrorException(message=str(e))

@router.delete("/{namespaceStr:path}", status_code=status.HTTP_204_NO_CONTENT)
async def dropNamespaceEndpoint(
    namespaceStr: str = Path(..., description="Namespace string"),
    db: AsyncSession = Depends(getDb)
):
    levels = namespaceStr.split('.')
    try:
        await crudNamespace.deleteNamespace(db, levels)
        return 
    except BaseIcebergException as e:
        raise e
    except Exception as e:
        raise InternalServerErrorException(message=str(e))

@router.head("/{namespaceStr:path}", status_code=status.HTTP_200_OK)
async def namespaceExistsEndpoint(
    namespaceStr: str = Path(..., description="Namespace string"),
    db: AsyncSession = Depends(getDb)
):
    levels = namespaceStr.split('.')
    try:
        dbNamespace = await crudNamespace.getNamespaceByLevels(db, levels)
        if not dbNamespace:
            raise NoSuchNamespaceException(namespace=levels)
        return 
    except BaseIcebergException as e:
        raise e
    except Exception as e:
        raise InternalServerErrorException(message=str(e))