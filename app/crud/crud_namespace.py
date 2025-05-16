from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError
from typing import List, Optional, Dict
from app.db.models_db import NamespaceModel
from app.models.iceberg_models import CreateNamespaceRequest
from app.core.exceptions import NamespaceAlreadyExistsException, NoSuchNamespaceException, ValidationException

class CRUDNamespace:
    async def getNamespaceByLevels(self, db: AsyncSession, levels: List[str]) -> Optional[NamespaceModel]:
        stmt = select(NamespaceModel).where(NamespaceModel.levels == levels)
        result = await db.execute(stmt)
        return result.scalars().first()

    async def createNamespace(self, db: AsyncSession, namespaceCreate: CreateNamespaceRequest) -> NamespaceModel:
        dbNamespace = NamespaceModel(
            levels=namespaceCreate.namespace,
            properties=namespaceCreate.properties
        )
        try:
            db.add(dbNamespace)
            await db.commit()
            await db.refresh(dbNamespace)
            return dbNamespace
        except IntegrityError:
            await db.rollback()
            raise NamespaceAlreadyExistsException(namespace=namespaceCreate.namespace)

    async def listNamespaces(self, db: AsyncSession, parent: Optional[List[str]] = None) -> List[NamespaceModel]:
        stmt = select(NamespaceModel)
        if parent:
            pass
        
        result = await db.execute(stmt.order_by(NamespaceModel.levels))
        return result.scalars().all()

    async def updateNamespaceProperties(
        self, db: AsyncSession, levels: List[str], updates: Dict[str, str], removals: List[str]
    ) -> NamespaceModel:
        dbNamespace = await self.getNamespaceByLevels(db, levels)
        if not dbNamespace:
            raise NoSuchNamespaceException(namespace=levels)

        currentProperties = dbNamespace.properties or {}
        if removals:
            for key in removals:
                currentProperties.pop(key, None)
        if updates:
            currentProperties.update(updates)
        
        dbNamespace.properties = currentProperties
        await db.commit()
        await db.refresh(dbNamespace)
        return dbNamespace

    async def deleteNamespace(self, db: AsyncSession, levels: List[str]) -> None:
        dbNamespace = await self.getNamespaceByLevels(db, levels)
        if not dbNamespace:
            raise NoSuchNamespaceException(namespace=levels)
        
        if dbNamespace.tables:
            raise ValidationException(f"Namespace {'.'.join(levels)} is not empty. Contains tables.")

        await db.delete(dbNamespace)
        await db.commit()

crudNamespace = CRUDNamespace()