import uuid
import time
import os
from copy import deepcopy
from typing import List, Optional, Tuple, Dict, Any

from app.models.iceberg_models import (
    TableMetadata, Schema, PartitionSpec, SortOrder, Snapshot, TableUpdate,
    AssignUUIDUpdate, UpgradeFormatVersionUpdate, AddSchemaUpdate, SetCurrentSchemaUpdate,
    AddPartitionSpecUpdate, SetDefaultSpecUpdate, AddSortOrderUpdate, SetDefaultSortOrderUpdate,
    AddSnapshotUpdate, SetPropertiesUpdate, RemovePropertiesUpdate, SetLocationUpdate,
    SetSnapshotRefUpdate 
)
from app.services.storage_accessor import storageAccessor
from app.core.config import settings
from app.core.exceptions import ValidationException, CommitFailedException, BadRequestException, InternalServerErrorException

class MetadataManager:
    def generateNewMetadataLocation(self, tableLocation: str, oldMetadataLocation: Optional[str] = None) -> str:
        version = 0
        if oldMetadataLocation:
            try:
                filename = os.path.basename(oldMetadataLocation)
                versionStr = filename.split('-')[0]
                version = int(versionStr) + 1
            except (IndexError, ValueError):
                pass 

        metadataDir = os.path.join(tableLocation, "metadata")
        newFilename = f"{version:05d}-{uuid.uuid4()}.metadata.json"
        return os.path.join(metadataDir, newFilename)

    def buildInitialTableMetadata(
        self,
        tableName: str,
        schema: Schema,
        partitionSpec: Optional[PartitionSpec],
        sortOrder: Optional[SortOrder],
        properties: Optional[Dict[str, str]],
        tableLocation: str,
    ) -> Tuple[TableMetadata, str]:
        maxId = 0
        if schema.fields:
            
            
            
            
            def getMaxIdRecursive(fields: List[Any]) -> int:
                currentMax = 0
                for f in fields:
                    currentMax = max(currentMax, f.id)
                    if hasattr(f.type, 'fields') and f.type.fields: # StructType
                        currentMax = max(currentMax, getMaxIdRecursive(f.type.fields))
                    
                return currentMax
            maxId = getMaxIdRecursive(schema.fields)


        if schema.schemaId is None:
            schema.schemaId = 0

        tableUuid = str(uuid.uuid4())
        currentTimeMs = int(time.time() * 1000)
        
        actualPartitionSpecFields = partitionSpec.fields if partitionSpec and partitionSpec.fields else []
        lastPartitionId = 0
        if actualPartitionSpecFields:
             lastPartitionId = max(p.fieldId for p in actualPartitionSpecFields)


        initialMetadata = TableMetadata(
            formatVersion=1,
            tableUuid=tableUuid,
            location=tableLocation,
            lastUpdatedMs=currentTimeMs,
            lastColumnId=maxId,
            schemas=[schema],
            currentSchemaId=schema.schemaId,
            partitionSpecs=[partitionSpec] if partitionSpec else [],
            defaultSpecId=partitionSpec.specId if partitionSpec else 0,
            lastPartitionId=lastPartitionId,
            properties=properties or {},
            sortOrders=[sortOrder] if sortOrder else [],
            defaultSortOrderId=sortOrder.orderId if sortOrder else 0,
            currentSnapshotId=None,
            snapshots=[],
            snapshotLog=[],
            metadataLog=[],
            refs={},
        )
        
        metadataJsonFileLocation = self.generateNewMetadataLocation(tableLocation)
        
        initialMetadata.metadataLog = [{"timestamp-ms": currentTimeMs, "metadata-file": metadataJsonFileLocation}]
        
        return initialMetadata, metadataJsonFileLocation


    async def applyUpdatesToMetadata(
        self,
        currentMetadata: TableMetadata,
        updates: List[TableUpdate],
        newTableLocationForSetLocation: Optional[str] = None
    ) -> TableMetadata:
        newMetadata = currentMetadata.model_copy(deep=True)

        newMetadata.lastUpdatedMs = int(time.time() * 1000)

        def getMaxFieldIdFromSchema(schema: Schema) -> int:
            maxId = 0
            if schema.fields:
                for f in schema.fields:
                    maxId = max(maxId, f.id)
                    if hasattr(f.type, 'fields') and f.type.fields: # StructType
                         maxId = max(maxId, getMaxFieldIdFromSchema(f.type)) 
            return maxId

        for update in updates:
            if isinstance(update, AssignUUIDUpdate):
                newMetadata.tableUuid = update.uuid
            elif isinstance(update, UpgradeFormatVersionUpdate):
                if update.formatVersion > newMetadata.formatVersion:
                    newMetadata.formatVersion = update.formatVersion
                else:
                    raise CommitFailedException(f"Cannot downgrade format version from {newMetadata.formatVersion} to {update.formatVersion}")
            elif isinstance(update, AddSchemaUpdate):
                if any(s.schemaId == update.schemaModel.schemaId for s in newMetadata.schemas):
                    raise CommitFailedException(f"Schema with id {update.schemaModel.schemaId} already exists.")
                newMetadata.schemas.append(update.schemaModel)
                maxExistingFieldId = newMetadata.lastColumnId
                newSchemaMaxFieldId = getMaxFieldIdFromSchema(update.schemaModel)
                newMetadata.lastColumnId = max(maxExistingFieldId, newSchemaMaxFieldId)
                if update.lastAssignedFieldId is not None:
                     newMetadata.lastColumnId = max(newMetadata.lastColumnId, update.lastAssignedFieldId)

            elif isinstance(update, SetCurrentSchemaUpdate):
                if not any(s.schemaId == update.schemaId for s in newMetadata.schemas):
                    raise CommitFailedException(f"Schema with id {update.schemaId} not found in existing schemas.")
                newMetadata.currentSchemaId = update.schemaId
            elif isinstance(update, AddPartitionSpecUpdate):
                if any(ps.specId == update.spec.specId for ps in newMetadata.partitionSpecs):
                     raise CommitFailedException(f"Partition spec with id {update.spec.specId} already exists.")
                newMetadata.partitionSpecs.append(update.spec)
                maxExistingPartitionId = newMetadata.lastPartitionId
                newSpecMaxFieldId = 0
                if update.spec.fields:
                    newSpecMaxFieldId = max(p.fieldId for p in update.spec.fields)
                newMetadata.lastPartitionId = max(maxExistingPartitionId, newSpecMaxFieldId)
            elif isinstance(update, SetDefaultSpecUpdate):
                if not any(s.specId == update.specId for s in newMetadata.partitionSpecs):
                     raise CommitFailedException(f"Partition spec with id {update.specId} not found.")
                newMetadata.defaultSpecId = update.specId
            elif isinstance(update, AddSortOrderUpdate):
                if any(so.orderId == update.sortOrder.orderId for so in newMetadata.sortOrders):
                    raise CommitFailedException(f"Sort order with id {update.sortOrder.orderId} already exists.")
                newMetadata.sortOrders.append(update.sortOrder)
            elif isinstance(update, SetDefaultSortOrderUpdate):
                if not any(s.orderId == update.sortOrderId for s in newMetadata.sortOrders):
                    raise CommitFailedException(f"Sort order with id {update.sortOrderId} not found.")
                newMetadata.defaultSortOrderId = update.sortOrderId
            elif isinstance(update, AddSnapshotUpdate):
                if newMetadata.snapshots is None: newMetadata.snapshots = []
                if newMetadata.snapshotLog is None: newMetadata.snapshotLog = []
                
                newMetadata.snapshots.append(update.snapshot)
                newMetadata.currentSnapshotId = update.snapshot.snapshotId
                newMetadata.snapshotLog.append({
                    "timestamp-ms": update.snapshot.timestampMs,
                    "snapshot-id": update.snapshot.snapshotId
                })
                if newMetadata.refs is None: newMetadata.refs = {}
                
                
                mainRefName = "main" 
                newMetadata.refs[mainRefName] = SetSnapshotRefUpdate(
                    refName=mainRefName,
                    snapshotId=update.snapshot.snapshotId,
                    type="branch" 
                ).model_dump(by_alias=True, exclude={'action', 'refName'})


            elif isinstance(update, SetSnapshotRefUpdate):
                if newMetadata.refs is None: newMetadata.refs = {}
                newMetadata.refs[update.refName] = update.model_dump(by_alias=True, exclude={'action', 'refName'})
            elif isinstance(update, SetPropertiesUpdate):
                if newMetadata.properties is None: newMetadata.properties = {}
                newMetadata.properties.update(update.updates)
            elif isinstance(update, RemovePropertiesUpdate):
                if newMetadata.properties:
                    for propKey in update.removals:
                        newMetadata.properties.pop(propKey, None)
            elif isinstance(update, SetLocationUpdate):
                if newTableLocationForSetLocation:
                    newMetadata.location = newTableLocationForSetLocation
                else: 
                    newMetadata.location = update.location
            else:
                actionValue = update.action if hasattr(update, 'action') else type(update).__name__
                raise BadRequestException(f"Unsupported table update action: '{actionValue}'.")
        
        return newMetadata

metadataManager = MetadataManager()