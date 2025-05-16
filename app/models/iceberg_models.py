from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Union, Literal
import time
import uuid

class PrimitiveType:
    pass

class StructType(BaseModel):
    type: Literal["struct"] = "struct"
    fields: List["StructField"]

class ListType(BaseModel):
    type: Literal["list"] = "list"
    elementId: int = Field(alias="element-id")
    element: "IcebergType"
    elementRequired: bool = Field(alias="element-required")

class MapType(BaseModel):
    type: Literal["map"] = "map"
    keyId: int = Field(alias="key-id")
    key: "IcebergType"
    valueId: int = Field(alias="value-id")
    value: "IcebergType"
    valueRequired: bool = Field(alias="value-required")

IcebergType = Union[Literal["boolean", "int", "long", "float", "double", "decimal", "date", "time", "timestamp", "timestamptz", "string", "uuid", "fixed", "binary"], StructType, ListType, MapType]

class StructField(BaseModel):
    id: int
    name: str
    type: IcebergType
    required: bool
    doc: Optional[str] = None

class Schema(BaseModel):
    type: Literal["struct"] = "struct"
    schemaId: Optional[int] = Field(None, alias="schema-id")
    fields: List[StructField]
    identifierFieldIds: Optional[List[int]] = Field(None, alias="identifier-field-ids")

class Transform(BaseModel):
    type: str

class PartitionField(BaseModel):
    sourceId: int = Field(alias="source-id")
    fieldId: int = Field(alias="field-id")
    name: str
    transform: str # Change to Transform model if more details are needed

class PartitionSpec(BaseModel):
    specId: int = Field(alias="spec-id", default=0)
    fields: List[PartitionField]

class SortField(BaseModel):
    sourceId: int = Field(alias="source-id")
    transform: str # Change to Transform model if more details are needed
    direction: Literal["asc", "desc"] = "asc"
    nullOrder: Literal["nulls-first", "nulls-last"] = Field("nulls-first", alias="null-order")

class SortOrder(BaseModel):
    orderId: int = Field(alias="order-id", default=0)
    fields: List[SortField]

class SnapshotSummary(BaseModel):
    operation: str
    additionalProperties: Dict[str, str] = Field(default_factory=dict)

    class Config:
        extra = 'allow'


class Snapshot(BaseModel):
    snapshotId: int = Field(alias="snapshot-id")
    parentId: Optional[int] = Field(None, alias="parent-id")
    timestampMs: int = Field(alias="timestamp-ms", default_factory=lambda: int(time.time() * 1000))
    summary: Optional[Dict[str, str]] = None # Or SnapshotSummary, but spec often shows string dict
    manifestList: str = Field(alias="manifest-list")
    schemaId: Optional[int] = Field(None, alias="schema-id")

class SnapshotReference(BaseModel):
    snapshotId: int = Field(alias="snapshot-id")
    type: Literal["tag", "branch"]
    minSnapshotsToKeep: Optional[int] = Field(None, alias="min-snapshots-to-keep")
    maxSnapshotAgeMs: Optional[int] = Field(None, alias="max-snapshot-age-ms")
    maxRefAgeMs: Optional[int] = Field(None, alias="max-ref-age-ms")

class TableMetadata(BaseModel):
    formatVersion: int = Field(alias="format-version", default=1)
    tableUuid: str = Field(alias="table-uuid", default_factory=lambda: str(uuid.uuid4()))
    location: str
    lastUpdatedMs: int = Field(alias="last-updated-ms", default_factory=lambda: int(time.time() * 1000))
    lastColumnId: int = Field(alias="last-column-id")
    schemas: List[Schema] = Field(default_factory=list)
    currentSchemaId: int = Field(alias="current-schema-id", default=0)
    partitionSpecs: List[PartitionSpec] = Field(alias="partition-specs", default_factory=list)
    defaultSpecId: int = Field(alias="default-spec-id", default=0)
    lastPartitionId: int = Field(alias="last-partition-id", default=0)
    properties: Optional[Dict[str, str]] = None
    currentSnapshotId: Optional[int] = Field(None, alias="current-snapshot-id")
    snapshots: Optional[List[Snapshot]] = Field(None, default_factory=list)
    snapshotLog: Optional[List[Dict[str, Any]]] = Field(None, alias="snapshot-log", default_factory=list)
    metadataLog: Optional[List[Dict[str, Any]]] = Field(None, alias="metadata-log", default_factory=list)
    sortOrders: List[SortOrder] = Field(alias="sort-orders", default_factory=list)
    defaultSortOrderId: int = Field(alias="default-sort-order-id", default=0)
    refs: Optional[Dict[str, SnapshotReference]] = Field(None, default_factory=dict)

    def getCurrentSchema(self) -> Optional[Schema]:
        if not self.schemas:
            return None
        for sInList in self.schemas:
            if sInList.schemaId == self.currentSchemaId:
                return sInList
        # Fallback if currentSchemaId is not in schemas list (should not happen in valid metadata)
        # or if currentSchemaId is default 0 but schemaId 0 isn't explicitly set on the first schema
        if self.schemas and self.currentSchemaId == 0 and self.schemas[0].schemaId is None:
            return self.schemas[0]
        if self.schemas and self.schemas[0].schemaId == 0 and self.currentSchemaId == 0:
            return self.schemas[0]
        return self.schemas[0] if self.schemas else None


    def getCurrentPartitionSpec(self) -> Optional[PartitionSpec]:
        if not self.partitionSpecs:
            return None
        for spec in self.partitionSpecs:
            if spec.specId == self.defaultSpecId:
                return spec
        return self.partitionSpecs[0] if self.partitionSpecs else None

class Namespace(BaseModel):
    namespace: List[str]
    properties: Optional[Dict[str, str]] = None

class CreateNamespaceRequest(BaseModel):
    namespace: List[str]
    properties: Optional[Dict[str, str]] = None

class UpdateNamespacePropertiesRequest(BaseModel):
    removals: Optional[List[str]] = None
    updates: Optional[Dict[str, str]] = None

class UpdateNamespacePropertiesResponse(BaseModel):
    updated: List[str]
    removed: List[str]
    missing: Optional[List[str]] = None

class TableIdentifier(BaseModel):
    namespace: List[str]
    name: str

class CreateTableRequest(BaseModel):
    name: str
    location: Optional[str] = None
    schemaModel: Schema = Field(alias="schema")
    partitionSpec: Optional[PartitionSpec] = Field(None, alias="partition-spec")
    writeOrder: Optional[SortOrder] = Field(None, alias="write-order")
    stageCreate: Optional[bool] = Field(None, alias="stage-create", default=False)
    properties: Optional[Dict[str, str]] = None

class RegisterTableRequest(BaseModel):
    name: Optional[str] = None
    metadataLocation: str = Field(alias="metadata-location")

class LoadTableResult(BaseModel):
    metadataLocation: Optional[str] = Field(None, alias="metadata-location")
    metadata: TableMetadata
    config: Optional[Dict[str, str]] = None

class AssertCreate(BaseModel):
    type: Literal["assert-create"] = "assert-create"

class AssertTableUUID(BaseModel):
    type: Literal["assert-table-uuid"] = "assert-table-uuid"
    uuid: str

class AssertDefaultSpecID(BaseModel):
    type: Literal["assert-default-spec-id"] = "assert-default-spec-id"
    defaultSpecId: int = Field(alias="default-spec-id")

class AssertDefaultSortOrderID(BaseModel):
    type: Literal["assert-default-sort-order-id"] = "assert-default-sort-order-id"
    defaultSortOrderId: int = Field(alias="default-sort-order-id")

class AssertCurrentSchemaID(BaseModel):
    type: Literal["assert-current-schema-id"] = "assert-current-schema-id"
    currentSchemaId: int = Field(alias="current-schema-id")

class AssertLastAssignedFieldID(BaseModel):
    type: Literal["assert-last-assigned-field-id"] = "assert-last-assigned-field-id"
    lastAssignedFieldId: int = Field(alias="last-assigned-field-id")

class AssertRefSnapshotID(BaseModel):
    type: Literal["assert-ref-snapshot-id"] = "assert-ref-snapshot-id"
    ref: str
    snapshotId: Optional[int] = Field(None, alias="snapshot-id") # None if ref should not exist

TableRequirement = Union[
    AssertCreate,
    AssertTableUUID,
    AssertDefaultSpecID,
    AssertDefaultSortOrderID,
    AssertCurrentSchemaID,
    AssertLastAssignedFieldID,
    AssertRefSnapshotID
]


class AssignUUIDUpdate(BaseModel):
    action: Literal["assign-uuid"] = "assign-uuid"
    uuid: str

class UpgradeFormatVersionUpdate(BaseModel):
    action: Literal["upgrade-format-version"] = "upgrade-format-version"
    formatVersion: int = Field(alias="format-version")

class AddSchemaUpdate(BaseModel):
    action: Literal["add-schema"] = "add-schema"
    schemaModel: Schema = Field(alias="schema")
    lastAssignedFieldId: Optional[int] = Field(None, alias="last-assigned-field-id") # Deprecated in v2

class SetCurrentSchemaUpdate(BaseModel):
    action: Literal["set-current-schema"] = "set-current-schema"
    schemaId: int = Field(alias="schema-id")

class AddPartitionSpecUpdate(BaseModel):
    action: Literal["add-spec"] = "add-spec"
    spec: PartitionSpec

class SetDefaultSpecUpdate(BaseModel):
    action: Literal["set-default-spec"] = "set-default-spec"
    specId: int = Field(alias="spec-id")

class AddSortOrderUpdate(BaseModel):
    action: Literal["add-sort-order"] = "add-sort-order"
    sortOrder: SortOrder = Field(alias="sort-order")

class SetDefaultSortOrderUpdate(BaseModel):
    action: Literal["set-default-sort-order"] = "set-default-sort-order"
    sortOrderId: int = Field(alias="sort-order-id")

class AddSnapshotUpdate(BaseModel):
    action: Literal["add-snapshot"] = "add-snapshot"
    snapshot: Snapshot

class RemoveSnapshotsUpdate(BaseModel):
    action: Literal["remove-snapshots"] = "remove-snapshots"
    snapshotIds: List[int] = Field(alias="snapshot-ids")

class SetSnapshotRefUpdate(BaseModel): # For managing branches and tags
    action: Literal["set-snapshot-ref"] = "set-snapshot-ref"
    refName: str = Field(alias="ref-name")
    type: Literal["tag", "branch"]
    snapshotId: int = Field(alias="snapshot-id")
    maxRefAgeMs: Optional[int] = Field(None, alias="max-ref-age-ms")
    maxSnapshotAgeMs: Optional[int] = Field(None, alias="max-snapshot-age-ms")
    minSnapshotsToKeep: Optional[int] = Field(None, alias="min-snapshots-to-keep")

class RemoveSnapshotRefUpdate(BaseModel):
    action: Literal["remove-snapshot-ref"] = "remove-snapshot-ref"
    refName: str = Field(alias="ref-name")


class SetPropertiesUpdate(BaseModel):
    action: Literal["set-properties"] = "set-properties"
    updates: Dict[str, str]

class RemovePropertiesUpdate(BaseModel):
    action: Literal["remove-properties"] = "remove-properties"
    removals: List[str]

class SetLocationUpdate(BaseModel):
    action: Literal["set-location"] = "set-location"
    location: str

TableUpdate = Union[
    AssignUUIDUpdate, UpgradeFormatVersionUpdate, AddSchemaUpdate, SetCurrentSchemaUpdate,
    AddPartitionSpecUpdate, SetDefaultSpecUpdate, AddSortOrderUpdate, SetDefaultSortOrderUpdate,
    AddSnapshotUpdate, RemoveSnapshotsUpdate, SetSnapshotRefUpdate, RemoveSnapshotRefUpdate,
    SetPropertiesUpdate, RemovePropertiesUpdate, SetLocationUpdate
]

class CommitTableRequest(BaseModel):
    identifier: Optional[TableIdentifier] = None # Optional because it might be in path
    requirements: List[TableRequirement] = Field(default_factory=list)
    updates: List[TableUpdate]

class CommitTableResponse(BaseModel):
    metadataLocation: str = Field(alias="metadata-location")
    metadata: TableMetadata

class RenameTableRequest(BaseModel):
    source: TableIdentifier
    destination: TableIdentifier

class CatalogConfig(BaseModel):
    override: Optional[Dict[str, str]] = Field(None, alias="override")
    default: Optional[Dict[str, str]] = Field(None, alias="default")

class TokenResponse(BaseModel):
    accessToken: str = Field(alias="access_token")
    tokenType: str = Field(alias="token_type", default="bearer")
    expiresIn: Optional[int] = Field(None, alias="expires_in")
    issuedTokenType: Optional[str] = Field(None, alias="issued_token_type")
    refreshToken: Optional[str] = Field(None, alias="refresh_token")
    scope: Optional[str] = None

StructType.model_rebuild()
ListType.model_rebuild()
MapType.model_rebuild()
Schema.model_rebuild()