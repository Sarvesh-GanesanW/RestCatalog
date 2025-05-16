import json
import os
import aiofiles
import aiofiles.os as aios
from typing import Dict, Any, Union
from app.core.config import settings
from app.core.exceptions import ValidationException, InternalServerErrorException, NotFoundException

class StorageAccessor:
    def __init__(self, baseWarehousePath: str):
        self.baseWarehousePath = baseWarehousePath

    def _resolvePath(self, relativeOrAbsolutePath: str) -> str:
        if "://" in relativeOrAbsolutePath or os.path.isabs(relativeOrAbsolutePath):
            if relativeOrAbsolutePath.startswith("file://"):
                 return relativeOrAbsolutePath[len("file://"):]
            return relativeOrAbsolutePath
        
        fullPath = os.path.abspath(os.path.join(self.baseWarehousePath, relativeOrAbsolutePath))
        if not fullPath.startswith(os.path.abspath(self.baseWarehousePath)):
            raise ValidationException(f"Path traversal attempt detected for relative path: {relativeOrAbsolutePath}")
        return fullPath


    async def readJsonFile(self, path: str) -> Dict[str, Any]:
        resolvedPath = self._resolvePath(path)
        try:
            async with aiofiles.open(resolvedPath, mode='r', encoding='utf-8') as f:
                content = await f.read()
            return json.loads(content)
        except FileNotFoundError:
            raise NotFoundException(resourceType="File", identifier=resolvedPath)
        except json.JSONDecodeError as e:
            raise ValidationException(f"Could not parse JSON from {resolvedPath}: {e}")
        except Exception as e:
            raise InternalServerErrorException(f"Failed to read JSON file from {resolvedPath}: {str(e)}")

    async def writeJsonFile(self, path: str, data: Union[Dict[str, Any], List[Any]]):
        resolvedPath = self._resolvePath(path)
        try:
            parentDir = os.path.dirname(resolvedPath)
            if not await aios.path.exists(parentDir):
                await aios.makedirs(parentDir, exist_ok=True)
            async with aiofiles.open(resolvedPath, mode='w', encoding='utf-8') as f:
                await f.write(json.dumps(data, indent=2))
        except Exception as e:
            raise InternalServerErrorException(f"Failed to write JSON file to {resolvedPath}: {str(e)}")


    async def fileExists(self, path: str) -> bool:
        resolvedPath = self._resolvePath(path)
        try:
            return await aios.path.exists(resolvedPath)
        except Exception as e:
            raise InternalServerErrorException(f"Failed to check existence of file {resolvedPath}: {str(e)}")

    async def deleteFile(self, path: str):
        resolvedPath = self._resolvePath(path)
        try:
            if await self.fileExists(resolvedPath):
                await aios.remove(resolvedPath)
        except Exception as e:
            raise InternalServerErrorException(f"Failed to delete file {resolvedPath}: {str(e)}")

storageAccessor = StorageAccessor(baseWarehousePath=settings.icebergWarehousePath)