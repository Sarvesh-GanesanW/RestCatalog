from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError
from app.db.models_db import TableModel
from app.models.iceberg_models import TableIdentifier, TableMetadata
from app.core.exceptions import (
    NoSuchTableException, TableAlreadyExistsException, NoSuchNamespaceException, ValidationException,
    CommitFailedException
)
from app.services.storage_accessor import storageAccessor
from app.services.metadata_manager import metadataManager
from app.crud.crud_namespace import crudNamespace
from app.core.config import settings
from typing import List, Dict, Optional


class CRUDTable:
    async def getTableByIdentifier(self, db: AsyncSession, identifier: TableIdentifier) -> Optional[TableModel]:
        namespaceModel = await crudNamespace.getNamespaceByLevels(db, identifier.namespace)
        if not namespaceModel:
            return None
        
        stmt = select(TableModel).where(
            TableModel.namespaceId == namespaceModel.id,
            TableModel.name == identifier.name
        )
        result = await db.execute(stmt)
        return result.scalars().first()

    async def listTables(self, db: AsyncSession, namespaceLevels: List[str]) -> List[TableModel]:
        namespaceModel = await crudNamespace.getNamespaceByLevels(db, namespaceLevels)
        if not namespaceModel:
            raise NoSuchNamespaceException(namespace=namespaceLevels)
        
        stmt = select(TableModel).where(TableModel.namespaceId == namespaceModel.id).order_by(TableModel.name)
        result = await db.execute(stmt)
        return result.scalars().all()

    async def createTable(
        self, 
        db: AsyncSession, 
        namespaceLevels: List[str],
        tableName: str,
        initialMetadata: TableMetadata,
        initialMetadataLocation: str,
        catalogProperties: Optional[Dict[str,str]] = None
    ) -> TableModel:
        namespaceModel = await crudNamespace.getNamespaceByLevels(db, namespaceLevels)
        if not namespaceModel:
            raise NoSuchNamespaceException(namespace=namespaceLevels)

        if await self.getTableByIdentifier(db, TableIdentifier(namespace=namespaceLevels, name=tableName)):
            raise TableAlreadyExistsException(tableIdentifier=[*namespaceLevels, tableName])
        
        dbTable = TableModel(
            namespaceId=namespaceModel.id,
            name=tableName,
            metadataLocation=initialMetadataLocation,
            properties=catalogProperties
        )
        try:
            db.add(dbTable)
            await db.commit()
            await db.refresh(dbTable)
            return dbTable
        except IntegrityError:
            await db.rollback()
            raise TableAlreadyExistsException(tableIdentifier=[*namespaceLevels, tableName])

    async def registerTable(
        self,
        db: AsyncSession,
        namespaceLevels: List[str],
        tableName: str,
        metadataLocation: str,
        catalogProperties: Optional[Dict[str,str]] = None
    ) -> TableModel:
        namespaceModel = await crudNamespace.getNamespaceByLevels(db, namespaceLevels)
        if not namespaceModel:
            raise NoSuchNamespaceException(namespace=namespaceLevels)

        if await self.getTableByIdentifier(db, TableIdentifier(namespace=namespaceLevels, name=tableName)):
            raise TableAlreadyExistsException(tableIdentifier=[*namespaceLevels, tableName])

        if not await storageAccessor.fileExists(metadataLocation):
            raise ValidationException(f"Metadata file for registration does not exist at: {metadataLocation}")

        dbTable = TableModel(
            namespaceId=namespaceModel.id,
            name=tableName,
            metadataLocation=metadataLocation,
            properties=catalogProperties
        )
        try:
            db.add(dbTable)
            await db.commit()
            await db.refresh(dbTable)
            return dbTable
        except IntegrityError:
            await db.rollback()
            raise TableAlreadyExistsException(tableIdentifier=[*namespaceLevels, tableName])

    async def deleteTable(self, db: AsyncSession, identifier: TableIdentifier, purge: bool = False) -> None:
        dbTable = await self.getTableByIdentifier(db, identifier)
        if not dbTable:
            raise NoSuchTableException(tableIdentifier=[*identifier.namespace, identifier.name])
        if purge:
            try:
                await storageAccessor.deleteFile(dbTable.metadataLocation)
            except Exception as e:
                print(f"Warning: Failed to purge metadata file {dbTable.metadataLocation}: {e}")

        await db.delete(dbTable)
        await db.commit()
    
    async def updateTableMetadataLocation(
        self, 
        db: AsyncSession, 
        identifier: TableIdentifier, 
        newMetadataLocation: str,
        oldMetadataLocationForCheck: Optional[str] = None
    ) -> TableModel:
        dbTable = await self.getTableByIdentifier(db, identifier)
        if not dbTable:
            raise NoSuchTableException(tableIdentifier=[*identifier.namespace, identifier.name])

        if oldMetadataLocationForCheck and dbTable.metadataLocation != oldMetadataLocationForCheck:
            raise CommitFailedException(
                message="Optimistic lock failed: metadata location has changed.",
                reason=f"Expected {oldMetadataLocationForCheck}, found {dbTable.metadataLocation}"
            )
        
        dbTable.metadataLocation = newMetadataLocation
        
        await db.commit()
        await db.refresh(dbTable)
        return dbTable
    
    async def renameTable(self, db: AsyncSession, source: TableIdentifier, destination: TableIdentifier) -> None:
        sourceDbTable = await self.getTableByIdentifier(db, source)
        if not sourceDbTable:
            raise NoSuchTableException(tableIdentifier=[*source.namespace, source.name])

        if await self.getTableByIdentifier(db, destination):
            raise TableAlreadyExistsException(tableIdentifier=[*destination.namespace, destination.name])

        destNamespaceModel = await crudNamespace.getNamespaceByLevels(db, destination.namespace)
        if not destNamespaceModel:
            raise NoSuchNamespaceException(namespace=destination.namespace)

        sourceDbTable.name = destination.name
        sourceDbTable.namespaceId = destNamespaceModel.id
        
        try:
            await db.commit()
        except IntegrityError:
            await db.rollback()
            raise TableAlreadyExistsException(tableIdentifier=[*destination.namespace, destination.name])

crudTable = CRUDTable()