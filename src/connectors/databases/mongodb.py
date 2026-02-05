"""
MongoDB Connector

Connect to MongoDB and MongoDB Atlas for document operations.
"""

from typing import Any
from ..base import BaseConnector, ConnectorResult


class MongoDBConnector(BaseConnector):
    """Connector for MongoDB databases."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.connection_string = credentials.get("connection_string")
        self.database = credentials.get("database")
        self._client = None
        self._db = None

    async def _get_db(self):
        """Get database connection."""
        if self._client is None:
            from motor.motor_asyncio import AsyncIOMotorClient
            self._client = AsyncIOMotorClient(self.connection_string)
            self._db = self._client[self.database]
        return self._db

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "insert_one": {
                "description": "Insert a single document",
                "parameters": {
                    "collection": {"type": "string", "description": "Collection name", "required": True},
                    "document": {"type": "object", "description": "Document to insert", "required": True},
                },
            },
            "insert_many": {
                "description": "Insert multiple documents",
                "parameters": {
                    "collection": {"type": "string", "description": "Collection name", "required": True},
                    "documents": {"type": "array", "description": "Documents to insert", "required": True},
                },
            },
            "find": {
                "description": "Find documents matching a query",
                "parameters": {
                    "collection": {"type": "string", "description": "Collection name", "required": True},
                    "filter": {"type": "object", "description": "Query filter", "required": False},
                    "projection": {"type": "object", "description": "Fields to include/exclude", "required": False},
                    "sort": {"type": "object", "description": "Sort order", "required": False},
                    "limit": {"type": "integer", "description": "Max documents to return", "required": False},
                    "skip": {"type": "integer", "description": "Documents to skip", "required": False},
                },
            },
            "find_one": {
                "description": "Find a single document",
                "parameters": {
                    "collection": {"type": "string", "description": "Collection name", "required": True},
                    "filter": {"type": "object", "description": "Query filter", "required": True},
                },
            },
            "update_one": {
                "description": "Update a single document",
                "parameters": {
                    "collection": {"type": "string", "description": "Collection name", "required": True},
                    "filter": {"type": "object", "description": "Query filter", "required": True},
                    "update": {"type": "object", "description": "Update operations", "required": True},
                    "upsert": {"type": "boolean", "description": "Create if not exists", "required": False},
                },
            },
            "update_many": {
                "description": "Update multiple documents",
                "parameters": {
                    "collection": {"type": "string", "description": "Collection name", "required": True},
                    "filter": {"type": "object", "description": "Query filter", "required": True},
                    "update": {"type": "object", "description": "Update operations", "required": True},
                },
            },
            "delete_one": {
                "description": "Delete a single document",
                "parameters": {
                    "collection": {"type": "string", "description": "Collection name", "required": True},
                    "filter": {"type": "object", "description": "Query filter", "required": True},
                },
            },
            "delete_many": {
                "description": "Delete multiple documents",
                "parameters": {
                    "collection": {"type": "string", "description": "Collection name", "required": True},
                    "filter": {"type": "object", "description": "Query filter", "required": True},
                },
            },
            "aggregate": {
                "description": "Run an aggregation pipeline",
                "parameters": {
                    "collection": {"type": "string", "description": "Collection name", "required": True},
                    "pipeline": {"type": "array", "description": "Aggregation pipeline stages", "required": True},
                },
            },
            "count": {
                "description": "Count documents matching a filter",
                "parameters": {
                    "collection": {"type": "string", "description": "Collection name", "required": True},
                    "filter": {"type": "object", "description": "Query filter", "required": False},
                },
            },
            "distinct": {
                "description": "Get distinct values for a field",
                "parameters": {
                    "collection": {"type": "string", "description": "Collection name", "required": True},
                    "field": {"type": "string", "description": "Field name", "required": True},
                    "filter": {"type": "object", "description": "Query filter", "required": False},
                },
            },
            "list_collections": {
                "description": "List all collections",
                "parameters": {},
            },
            "create_index": {
                "description": "Create an index on a collection",
                "parameters": {
                    "collection": {"type": "string", "description": "Collection name", "required": True},
                    "keys": {"type": "object", "description": "Index keys and directions", "required": True},
                    "unique": {"type": "boolean", "description": "Unique index", "required": False},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            db = await self._get_db()

            if action == "insert_one":
                return await self._insert_one(db, params["collection"], params["document"])
            elif action == "insert_many":
                return await self._insert_many(db, params["collection"], params["documents"])
            elif action == "find":
                return await self._find(db, params["collection"], params)
            elif action == "find_one":
                return await self._find_one(db, params["collection"], params["filter"])
            elif action == "update_one":
                return await self._update_one(db, params["collection"], params["filter"],
                                             params["update"], params.get("upsert", False))
            elif action == "update_many":
                return await self._update_many(db, params["collection"], params["filter"], params["update"])
            elif action == "delete_one":
                return await self._delete_one(db, params["collection"], params["filter"])
            elif action == "delete_many":
                return await self._delete_many(db, params["collection"], params["filter"])
            elif action == "aggregate":
                return await self._aggregate(db, params["collection"], params["pipeline"])
            elif action == "count":
                return await self._count(db, params["collection"], params.get("filter", {}))
            elif action == "distinct":
                return await self._distinct(db, params["collection"], params["field"], params.get("filter", {}))
            elif action == "list_collections":
                return await self._list_collections(db)
            elif action == "create_index":
                return await self._create_index(db, params["collection"], params["keys"], params.get("unique", False))
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    def _serialize_doc(self, doc: dict) -> dict:
        """Convert MongoDB document to JSON-serializable format."""
        if doc is None:
            return None
        result = {}
        for k, v in doc.items():
            if k == "_id":
                result[k] = str(v)
            elif hasattr(v, "isoformat"):
                result[k] = v.isoformat()
            elif isinstance(v, dict):
                result[k] = self._serialize_doc(v)
            elif isinstance(v, list):
                result[k] = [self._serialize_doc(i) if isinstance(i, dict) else i for i in v]
            else:
                result[k] = v
        return result

    async def _insert_one(self, db, collection: str, document: dict) -> ConnectorResult:
        result = await db[collection].insert_one(document)
        return ConnectorResult(
            success=True,
            data={"inserted_id": str(result.inserted_id)}
        )

    async def _insert_many(self, db, collection: str, documents: list[dict]) -> ConnectorResult:
        result = await db[collection].insert_many(documents)
        return ConnectorResult(
            success=True,
            data={"inserted_ids": [str(id) for id in result.inserted_ids], "count": len(result.inserted_ids)}
        )

    async def _find(self, db, collection: str, params: dict) -> ConnectorResult:
        cursor = db[collection].find(
            params.get("filter", {}),
            params.get("projection")
        )

        if params.get("sort"):
            cursor = cursor.sort(list(params["sort"].items()))
        if params.get("skip"):
            cursor = cursor.skip(params["skip"])
        if params.get("limit"):
            cursor = cursor.limit(params["limit"])

        documents = await cursor.to_list(length=params.get("limit", 1000))
        return ConnectorResult(
            success=True,
            data={"documents": [self._serialize_doc(d) for d in documents], "count": len(documents)}
        )

    async def _find_one(self, db, collection: str, filter: dict) -> ConnectorResult:
        doc = await db[collection].find_one(filter)
        return ConnectorResult(
            success=True,
            data={"document": self._serialize_doc(doc)}
        )

    async def _update_one(self, db, collection: str, filter: dict, update: dict, upsert: bool) -> ConnectorResult:
        result = await db[collection].update_one(filter, update, upsert=upsert)
        return ConnectorResult(
            success=True,
            data={
                "matched_count": result.matched_count,
                "modified_count": result.modified_count,
                "upserted_id": str(result.upserted_id) if result.upserted_id else None
            }
        )

    async def _update_many(self, db, collection: str, filter: dict, update: dict) -> ConnectorResult:
        result = await db[collection].update_many(filter, update)
        return ConnectorResult(
            success=True,
            data={"matched_count": result.matched_count, "modified_count": result.modified_count}
        )

    async def _delete_one(self, db, collection: str, filter: dict) -> ConnectorResult:
        result = await db[collection].delete_one(filter)
        return ConnectorResult(success=True, data={"deleted_count": result.deleted_count})

    async def _delete_many(self, db, collection: str, filter: dict) -> ConnectorResult:
        result = await db[collection].delete_many(filter)
        return ConnectorResult(success=True, data={"deleted_count": result.deleted_count})

    async def _aggregate(self, db, collection: str, pipeline: list) -> ConnectorResult:
        cursor = db[collection].aggregate(pipeline)
        results = await cursor.to_list(length=1000)
        return ConnectorResult(
            success=True,
            data={"results": [self._serialize_doc(d) for d in results], "count": len(results)}
        )

    async def _count(self, db, collection: str, filter: dict) -> ConnectorResult:
        count = await db[collection].count_documents(filter)
        return ConnectorResult(success=True, data={"count": count})

    async def _distinct(self, db, collection: str, field: str, filter: dict) -> ConnectorResult:
        values = await db[collection].distinct(field, filter)
        return ConnectorResult(success=True, data={"values": values, "count": len(values)})

    async def _list_collections(self, db) -> ConnectorResult:
        collections = await db.list_collection_names()
        return ConnectorResult(success=True, data={"collections": collections})

    async def _create_index(self, db, collection: str, keys: dict, unique: bool) -> ConnectorResult:
        index_name = await db[collection].create_index(list(keys.items()), unique=unique)
        return ConnectorResult(success=True, data={"index_name": index_name})

    async def close(self):
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
