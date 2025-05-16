from fastapi import HTTPException
from fastapi import status as httpStatus
from typing import List, Optional, Dict, Any
from pydantic import BaseModel

class IcebergErrorModel(BaseModel):
    message: str
    type: str
    code: int
    stack: Optional[List[str]] = None

class ErrorResponse(BaseModel):
    error: IcebergErrorModel

class BaseIcebergException(HTTPException):
    def __init__(self, statusCode: int, message: str, errorType: str, stack: Optional[List[str]] = None):
        self.errorType = errorType
        self.message = message
        self.stack = stack
        super().__init__(status_code=statusCode, detail={"error": {
            "message": message,
            "type": errorType,
            "code": statusCode,
            "stack": stack
        }})

class BadRequestException(BaseIcebergException):
    def __init__(self, message: str = "The request was malformed or contained invalid parameters."):
        super().__init__(
            statusCode=httpStatus.HTTP_400_BAD_REQUEST,
            message=message,
            errorType="BadRequestException"
        )

class ValidationException(BadRequestException):
    def __init__(self, message: str):
        super().__init__(
            message=message
        )
        self.errorType = "ValidationException"

class AuthenticationFailedException(BaseIcebergException):
    def __init__(self, message: str = "Authentication failed. Missing or invalid credentials."):
        super().__init__(
            statusCode=httpStatus.HTTP_401_UNAUTHORIZED,
            message=message,
            errorType="AuthenticationFailedException"
        )

class PermissionDeniedException(BaseIcebergException):
    def __init__(self, message: str = "Permission denied. The authenticated user does not have the necessary permissions."):
        super().__init__(
            statusCode=httpStatus.HTTP_403_FORBIDDEN,
            message=message,
            errorType="PermissionDeniedException"
        )

class NotFoundException(BaseIcebergException):
    def __init__(self, resourceType: str, identifier: Any):
        super().__init__(
            statusCode=httpStatus.HTTP_404_NOT_FOUND,
            message=f"{resourceType} with identifier '{identifier}' not found.",
            errorType="NotFoundException"
        )

class NoSuchNamespaceException(NotFoundException):
    def __init__(self, namespace: List[str]):
        super().__init__(
            resourceType="Namespace",
            identifier='.'.join(namespace)
        )
        self.errorType = "NoSuchNamespaceException"

class NoSuchTableException(NotFoundException):
    def __init__(self, tableIdentifier: List[str]):
        super().__init__(
            resourceType="Table",
            identifier='.'.join(tableIdentifier)
        )
        self.errorType = "NoSuchTableException"

class MethodNotAllowedException(BaseIcebergException):
    def __init__(self, method: str, allowedMethods: List[str]):
        super().__init__(
            statusCode=httpStatus.HTTP_405_METHOD_NOT_ALLOWED,
            message=f"Method '{method}' not allowed for this resource. Allowed methods: {', '.join(allowedMethods)}.",
            errorType="MethodNotAllowedException"
        )

class NotAcceptableException(BaseIcebergException):
    def __init__(self, message: str = "The requested representation is not acceptable."):
        super().__init__(
            statusCode=httpStatus.HTTP_406_NOT_ACCEPTABLE,
            message=message,
            errorType="NotAcceptableException"
        )

class ConflictException(BaseIcebergException):
    def __init__(self, message: str, errorType: str = "ConflictException"):
        super().__init__(
            statusCode=httpStatus.HTTP_409_CONFLICT,
            message=message,
            errorType=errorType
        )

class NamespaceAlreadyExistsException(ConflictException):
    def __init__(self, namespace: List[str]):
        super().__init__(
            message=f"Namespace already exists: {'.'.join(namespace)}",
            errorType="NamespaceAlreadyExistsException"
        )

class TableAlreadyExistsException(ConflictException):
    def __init__(self, tableIdentifier: List[str]):
        super().__init__(
            message=f"Table already exists: {'.'.join(tableIdentifier)}",
            errorType="TableAlreadyExistsException"
        )

class CommitFailedException(ConflictException):
    def __init__(self, message: str, reason: Optional[str] = None):
        fullMessage = f"Commit failed: {message}" + (f" (Reason: {reason})" if reason else "")
        super().__init__(
            message=fullMessage,
            errorType="CommitFailedException"
        )

class UnsupportedMediaTypeException(BaseIcebergException):
    def __init__(self, mediaType: str, supportedMediaTypes: List[str]):
        super().__init__(
            statusCode=httpStatus.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            message=f"Media type '{mediaType}' is not supported. Supported types: {', '.join(supportedMediaTypes)}.",
            errorType="UnsupportedMediaTypeException"
        )

class InternalServerErrorException(BaseIcebergException):
    def __init__(self, message: str = "An unexpected internal server error occurred."):
        super().__init__(
            statusCode=httpStatus.HTTP_500_INTERNAL_SERVER_ERROR,
            message=message,
            errorType="InternalServerErrorException"
        )

class ServiceUnavailableException(BaseIcebergException):
    def __init__(self, message: str = "The service is temporarily unavailable. Please try again later."):
        super().__init__(
            statusCode=httpStatus.HTTP_503_SERVICE_UNAVAILABLE,
            message=message,
            errorType="ServiceUnavailableException"
        )

class GatewayTimeoutException(BaseIcebergException):
    def __init__(self, message: str = "The upstream server did not respond in time."):
        super().__init__(
            statusCode=httpStatus.HTTP_504_GATEWAY_TIMEOUT,
            message=message,
            errorType="GatewayTimeoutException"
        )