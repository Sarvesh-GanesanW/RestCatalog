from fastapi import APIRouter, Depends, status, Query, Body, Path, Response
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional, Dict, Any
import os

from app.db.session import getDb
from app.crud.crud_table import crudTable
from app.crud.crud_namespace import crudNamespace
from app.models.iceberg_models import (
    TableIdentifier, CreateTableRequest, LoadTableResult, TableMetadata,
    CommitTableRequest, CommitTableResponse, RegisterTableRequest, RenameTableRequest, 
    SetLocationUpdate, AssertCreate, TableRequirement
)
from app.core.exceptions import (
    BaseIcebergException, ErrorResponse, NoSuchNamespaceException, ValidationException,
    NoSuchTableException, TableAlreadyExistsException, CommitFailedException,
    InternalServerErrorException, BadRequestException
)
from app.services.storage_accessor import storageAccessor
from app.services.metadata_manager import metadataManager
from app.core.config import settings


router = APIRouter(
    prefix="/v1/namespaces/{namespaceStr:path}/tables",
    tags=["Tables"],
    responses={
        400: {"model": ErrorResponse, "description": "Bad Request"},
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        403: {"model": ErrorResponse, "description": "Forbidden"},
        404: {"model": ErrorResponse, "description": "Not Found"},
        406: {"model": ErrorResponse, "description": "Not Acceptable"},
        409: {"model": ErrorResponse, "description": "Conflict"},
        419: {"model": ErrorResponse, "description": "Authentication Timeout (non-standard)"}, # typically 401
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
        503: {"model": ErrorResponse, "description": "Service Unavailable"},
    }
)

def parseNamespace(namespaceStr: str) -> List[str]:
    if not namespaceStr:
        return []
    return namespaceStr.split('.')


@router.get("", response_model=List[TableIdentifier])
async def listTablesEndpoint(
    namespaceStr: str = Path(..., description="Namespace string, e.g., 'db' or 'db.schema'"),
    db: AsyncSession = Depends(getDb)
):
    namespaceLevels = parseNamespace(namespaceStr)
    try:
        if not await crudNamespace.namespaceExists(db, namespaceLevels):
            raise NoSuchNamespaceException(namespace=namespaceLevels)
            
        dbTables = await crudTable.listTables(db, namespaceLevels)
        return [TableIdentifier(namespace=namespaceLevels, name=t.name) for t in dbTables]
    except BaseIcebergException as e:
        raise e
    except Exception as e:
        raise InternalServerErrorException(message=str(e))


@router.post("", response_model=LoadTableResult, status_code=status.HTTP_200_OK)
async def createTableEndpoint(
    namespaceStr: str = Path(..., description="Namespace string"),
    request: CreateTableRequest = Body(...),
    db: AsyncSession = Depends(getDb)
):
    namespaceLevels = parseNamespace(namespaceStr)
    try:
        dbNamespace = await crudNamespace.getNamespaceByLevels(db, namespaceLevels)
        if not dbNamespace:
            raise NoSuchNamespaceException(namespace=namespaceLevels)

        tableBaseLocation = request.location
        if not tableBaseLocation:
            # Ensure settings.icebergWarehousePath is absolute or handle accordingly
            warehousePath = os.path.abspath(settings.icebergWarehousePath)
            tableBaseLocation = os.path.join(warehousePath, *namespaceLevels, request.name)
        
        initialMetadata, metadataFileLocation = metadataManager.buildInitialTableMetadata(
            tableName=request.name,
            schema=request.schemaModel,
            partitionSpec=request.partitionSpec,
            sortOrder=request.writeOrder,
            properties=request.properties,
            tableLocation=tableBaseLocation
        )

        if not request.stageCreate:
            await storageAccessor.writeJsonFile(metadataFileLocation, initialMetadata.model_dump(by_alias=True, exclude_none=True))
            
            await crudTable.createTable(
                db, namespaceLevels, request.name, metadataFileLocation, catalogProperties=None 
            )
        

        return LoadTableResult(
            metadataLocation=metadataFileLocation,
            metadata=initialMetadata,
            config={"created-by": "iceberg-rest-catalog"}
        )

    except BaseIcebergException as e:
        raise e
    except Exception as e:
        raise InternalServerErrorException(message=str(e))


@router.post("/register", response_model=LoadTableResult, status_code=status.HTTP_200_OK)
async def registerTableEndpoint(
    namespaceStr: str = Path(..., description="Namespace string"),
    request: RegisterTableRequest = Body(...),
    db: AsyncSession = Depends(getDb)
):
    namespaceLevels = parseNamespace(namespaceStr)
    tableName = request.name
    if not tableName:
        
        try:
            baseName = os.path.basename(os.path.dirname(request.metadataLocation))
            if baseName != "metadata": 
                tableName = baseName
            else: # try one level up if it's .../table_name/metadata/file.json
                grandparentDir = os.path.basename(os.path.dirname(os.path.dirname(request.metadataLocation)))
                if grandparentDir: # Check if grandparentDir is not empty
                     tableName = grandparentDir
                else:
                    raise ValidationException("Table name must be provided or inferable from metadataLocation.")
        except Exception:
             raise ValidationException("Could not infer table name from metadataLocation. Please provide it explicitly.")


    try:
        dbNamespace = await crudNamespace.getNamespaceByLevels(db, namespaceLevels)
        if not dbNamespace:
            raise NoSuchNamespaceException(namespace=namespaceLevels)

        dbTableModel = await crudTable.registerTable(
            db, namespaceLevels, tableName, request.metadataLocation
        )
        
        tableMetadataJson = await storageAccessor.readJsonFile(dbTableModel.metadataLocation)
        tableMetadata = TableMetadata(**tableMetadataJson)

        return LoadTableResult(
            metadataLocation=dbTableModel.metadataLocation,
            metadata=tableMetadata,
            config={"registered-by": "iceberg-rest-catalog"}
        )
    except BaseIcebergException as e:
        raise e
    except Exception as e:
        raise InternalServerErrorException(message=str(e))


@router.get("/{tableName}", response_model=LoadTableResult)
async def loadTableEndpoint(
    namespaceStr: str = Path(..., description="Namespace string"),
    tableName: str = Path(..., description="Table name"),
    snapshotRef: Optional[str] = Query(None, alias="snapshot-ref", description="Snapshot reference (branch or tag) or snapshot ID"),
    db: AsyncSession = Depends(getDb)
):
    namespaceLevels = parseNamespace(namespaceStr)
    identifier = TableIdentifier(namespace=namespaceLevels, name=tableName)
    try:
        dbTable = await crudTable.getTableByIdentifier(db, identifier)
        if not dbTable:
            raise NoSuchTableException(tableIdentifier=[*namespaceLevels, tableName])
        
        metadataJson = await storageAccessor.readJsonFile(dbTable.metadataLocation)
        tableMetadata = TableMetadata(**metadataJson)
        
        
        if snapshotRef:
            targetSnapshotId = None
            if snapshotRef.isdigit(): # Assumed to be snapshot ID
                targetSnapshotId = int(snapshotRef)
            elif tableMetadata.refs and snapshotRef in tableMetadata.refs: # Branch or Tag name
                targetSnapshotId = tableMetadata.refs[snapshotRef].snapshotId
            else: # Check if it's a historical snapshot ID not in refs
                if tableMetadata.snapshots:
                    for snap in tableMetadata.snapshots:
                        if str(snap.snapshotId) == snapshotRef:
                            targetSnapshotId = snap.snapshotId
                            break
            
            if targetSnapshotId is None:
                raise NoSuchTableException(tableIdentifier=[*namespaceLevels, tableName, f"ref:{snapshotRef}"]) # Or a more specific ref not found

            
            currentSnap = None
            if tableMetadata.snapshots:
                for s in tableMetadata.snapshots:
                    if s.snapshotId == targetSnapshotId:
                        currentSnap = s
                        break
            if not currentSnap:
                raise CommitFailedException(f"Snapshot ID {targetSnapshotId} referenced by '{snapshotRef}' not found in metadata.")

            
            tableMetadata.currentSnapshotId = targetSnapshotId
            if currentSnap.schemaId is not None:
                 tableMetadata.currentSchemaId = currentSnap.schemaId


        return LoadTableResult(
            metadataLocation=dbTable.metadataLocation,
            metadata=tableMetadata,
            config=dbTable.properties # These are catalog properties, not Iceberg table properties
        )
    except BaseIcebergException as e:
        raise e
    except Exception as e:
        raise InternalServerErrorException(message=str(e))


@router.post("/{tableName}/commit", response_model=CommitTableResponse) # Changed path to match spec better
async def commitTableEndpoint(
    namespaceStr: str = Path(..., description="Namespace string"),
    tableName: str = Path(..., description="Table name"),
    request: CommitTableRequest = Body(...),
    db: AsyncSession = Depends(getDb)
):
    namespaceLevels = parseNamespace(namespaceStr)
    identifierFromPath = TableIdentifier(namespace=namespaceLevels, name=tableName)
    
    
    if request.identifier and (request.identifier.name != tableName or request.identifier.namespace != namespaceLevels):
        raise BadRequestException(
            f"Table identifier in path ('{'.'.join(namespaceLevels)}.{tableName}') "
            f"does not match identifier in request body ('{'.'.join(request.identifier.namespace)}.{request.identifier.name}')."
        )
    
    identifier = identifierFromPath # Use identifier from path as primary

    isAssertCreate = any(isinstance(req, AssertCreate) for req in request.requirements)
    
    currentDbTable = await crudTable.getTableByIdentifier(db, identifier)

    if isAssertCreate:
        if currentDbTable:
            raise TableAlreadyExistsException(tableIdentifier=[*identifier.namespace, identifier.name])
        
        
        if not request.updates: # Create usually means the first commit creates the metadata file
             raise BadRequestException("AssertCreate requires at least one update to form initial metadata.")

        
        createUpdate = request.updates[0] # This is an assumption; better to find the AddSchema or initial snapshot
        
        schemaToAdd = None
        initialSnapshot = None
        tableLocation = None
        props = {}

        for update_action in request.updates:
            if update_action.action == "add-schema":
                schemaToAdd = update_action.schemaModel # Corrected alias
            elif update_action.action == "add-snapshot":
                initialSnapshot = update_action.snapshot
            elif update_action.action == "set-properties":
                props.update(update_action.updates)
            elif update_action.action == "set-location":
                tableLocation = update_action.location
        
        if not schemaToAdd:
            raise BadRequestException("CreateTable (via AssertCreate) requires an 'add-schema' update.")
        
        if not tableLocation:
            warehousePath = os.path.abspath(settings.icebergWarehousePath)
            tableLocation = os.path.join(warehousePath, *namespaceLevels, tableName)

        currentMetadata, newMetadataFileLocation = metadataManager.buildInitialTableMetadata(
            tableName=tableName,
            schema=schemaToAdd,
            partitionSpec=None, 
            sortOrder=None,    
            properties=props,
            tableLocation=tableLocation
        )
         # Apply remaining updates if any, beyond the handled ones for creation
        if len(request.updates) > 0: # If there were other updates besides schema/snapshot/props/location for create
            try:
                currentMetadata = await metadataManager.applyUpdatesToMetadata(
                    currentMetadata, request.updates # Pass all updates again, let manager sort it out
                )
            except Exception as e:
                raise CommitFailedException(f"Failed to apply initial updates for table creation: {e}")
    
    else: # Not AssertCreate, so table must exist
        if not currentDbTable:
            raise NoSuchTableException(tableIdentifier=[*identifier.namespace, identifier.name])

        oldMetadataLocation = currentDbTable.metadataLocation
        try:
            currentMetadataJson = await storageAccessor.readJsonFile(oldMetadataLocation)
            currentMetadata = TableMetadata(**currentMetadataJson)
        except Exception as e: # More specific exceptions like NotFoundException or ValidationException are handled by storageAccessor
            raise CommitFailedException(f"Failed to load current metadata for commit: {e}")

        
        for req in request.requirements:
            if req.type == "assert-table-uuid":
                if currentMetadata.tableUuid != req.uuid:
                    raise CommitFailedException(f"Table UUID requirement failed. Expected: {req.uuid}, Found: {currentMetadata.tableUuid}")
            

        newTableBaseLocationForSetLocation = currentMetadata.location
        for update in request.updates:
            if isinstance(update, SetLocationUpdate):
                newTableBaseLocationForSetLocation = update.location
                break # Assume only one SetLocation update

        try:
            currentMetadata = await metadataManager.applyUpdatesToMetadata(
                currentMetadata, request.updates, newTableBaseLocationForSetLocation
            )
        except BaseIcebergException as e: # Catch specific app errors
            raise e
        except Exception as e:
            raise CommitFailedException(f"Failed to apply updates to metadata: {e}")

        newMetadataFileLocation = metadataManager.generateNewMetadataLocation(
            currentMetadata.location, # Use the potentially updated location
            oldMetadataLocation=oldMetadataLocation if currentDbTable else None
        )

    if currentMetadata.metadataLog is None: currentMetadata.metadataLog = []
    currentMetadata.metadataLog.append({
        "timestamp-ms": currentMetadata.lastUpdatedMs,
        "metadata-file": newMetadataFileLocation
    })
    
    await storageAccessor.writeJsonFile(newMetadataFileLocation, currentMetadata.model_dump(by_alias=True, exclude_none=True))

    try:
        if isAssertCreate:
             await crudTable.createTable(db, identifier.namespace, identifier.name, newMetadataFileLocation, currentMetadata.properties)
        else: # Regular commit on existing table
            await crudTable.updateTableMetadataLocation(
                db, identifier, newMetadataFileLocation, oldMetadataLocationForCheck=currentDbTable.metadataLocation if currentDbTable else None
            )
    except CommitFailedException as e: # From optimistic lock in CRUD
        await storageAccessor.deleteFile(newMetadataFileLocation) # Rollback file creation
        raise e
    except Exception as e:
        await storageAccessor.deleteFile(newMetadataFileLocation) # Rollback file creation
        raise CommitFailedException(f"Failed to update catalog database: {e}")

    return CommitTableResponse(
        metadataLocation=newMetadataFileLocation,
        metadata=currentMetadata
    )


@router.delete("/{tableName}", status_code=status.HTTP_204_NO_CONTENT)
async def dropTableEndpoint(
    namespaceStr: str = Path(..., description="Namespace string"),
    tableName: str = Path(..., description="Table name"),
    purge: Optional[bool] = Query(False, description="Delete all data and metadata for the table"),
    db: AsyncSession = Depends(getDb)
):
    namespaceLevels = parseNamespace(namespaceStr)
    identifier = TableIdentifier(namespace=namespaceLevels, name=tableName)
    try:
        dbTable = await crudTable.getTableByIdentifier(db, identifier)
        if not dbTable:
            raise NoSuchTableException(tableIdentifier=[*identifier.namespace, identifier.name])

        if purge:
            try:
                
                metadataJson = await storageAccessor.readJsonFile(dbTable.metadataLocation)
                tableMetadata = TableMetadata(**metadataJson)
                
                
                allMetadataFiles = [logEntry["metadata-file"] for logEntry in tableMetadata.metadataLog or []]
                if dbTable.metadataLocation not in allMetadataFiles: # current one might not be in log if log is short
                    allMetadataFiles.append(dbTable.metadataLocation)

                for mfLocation in set(allMetadataFiles): # Use set to avoid deleting same file multiple times
                    try:
                        await storageAccessor.deleteFile(mfLocation)
                    except Exception as e_purge_file:
                        
                        pass # Decide if this should raise or just warn
                
                
                
                
            except Exception as e_purge:
                
                pass # Decide if this should raise or just warn


        await crudTable.deleteTable(db, identifier) 
        return

    except BaseIcebergException as e:
        raise e
    except Exception as e:
        raise InternalServerErrorException(message=str(e))


@router.post("/rename", response_model=None, status_code=status.HTTP_204_NO_CONTENT) # Adjusted to match spec for POST /v1/tables/rename
async def renameTableGlobalEndpoint( # This should be at /v1/tables/rename, not nested
    request: RenameTableRequest = Body(...),
    db: AsyncSession = Depends(getDb)
):
    try:
        await crudTable.renameTable(db, request.source, request.destination)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except BaseIcebergException as e:
        raise e
    except Exception as e:
        raise InternalServerErrorException(message=str(e))


@router.head("/{tableName}", status_code=status.HTTP_200_OK)
async def tableExistsEndpoint(
    namespaceStr: str = Path(..., description="Namespace string"),
    tableName: str = Path(..., description="Table name"),
    db: AsyncSession = Depends(getDb)
):
    namespaceLevels = parseNamespace(namespaceStr)
    identifier = TableIdentifier(namespace=namespaceLevels, name=tableName)
    try:
        if not await crudTable.tableExists(db, identifier):
             raise NoSuchTableException(tableIdentifier=[*identifier.namespace, identifier.name])
        return 
    except BaseIcebergException as e:
        raise e
    except Exception as e:
        raise InternalServerErrorException(message=str(e))