"""
Firebase/Firestore Connector

Connect to Firebase Realtime Database and Cloud Firestore.
"""

from typing import Any
from ..base import BaseConnector, ConnectorResult


class FirebaseConnector(BaseConnector):
    """Connector for Firebase/Firestore."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.credentials_json = credentials.get("credentials_json")
        self.project_id = credentials.get("project_id")
        self.database_url = credentials.get("database_url")  # For Realtime DB
        self._firestore = None
        self._realtime = None

    async def _get_firestore(self):
        """Get Firestore client."""
        if self._firestore is None:
            import firebase_admin
            from firebase_admin import credentials, firestore
            import json

            if isinstance(self.credentials_json, str):
                creds_dict = json.loads(self.credentials_json)
            else:
                creds_dict = self.credentials_json

            cred = credentials.Certificate(creds_dict)

            try:
                firebase_admin.get_app()
            except ValueError:
                firebase_admin.initialize_app(cred, {
                    "projectId": self.project_id,
                    "databaseURL": self.database_url,
                })

            self._firestore = firestore.client()
        return self._firestore

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "get_document": {
                "description": "Get a Firestore document",
                "parameters": {
                    "collection": {"type": "string", "description": "Collection name", "required": True},
                    "document_id": {"type": "string", "description": "Document ID", "required": True},
                },
            },
            "set_document": {
                "description": "Set/create a Firestore document",
                "parameters": {
                    "collection": {"type": "string", "description": "Collection name", "required": True},
                    "document_id": {"type": "string", "description": "Document ID", "required": True},
                    "data": {"type": "object", "description": "Document data", "required": True},
                    "merge": {"type": "boolean", "description": "Merge with existing", "required": False},
                },
            },
            "add_document": {
                "description": "Add a new document (auto-ID)",
                "parameters": {
                    "collection": {"type": "string", "description": "Collection name", "required": True},
                    "data": {"type": "object", "description": "Document data", "required": True},
                },
            },
            "update_document": {
                "description": "Update a Firestore document",
                "parameters": {
                    "collection": {"type": "string", "description": "Collection name", "required": True},
                    "document_id": {"type": "string", "description": "Document ID", "required": True},
                    "data": {"type": "object", "description": "Fields to update", "required": True},
                },
            },
            "delete_document": {
                "description": "Delete a Firestore document",
                "parameters": {
                    "collection": {"type": "string", "description": "Collection name", "required": True},
                    "document_id": {"type": "string", "description": "Document ID", "required": True},
                },
            },
            "query": {
                "description": "Query a Firestore collection",
                "parameters": {
                    "collection": {"type": "string", "description": "Collection name", "required": True},
                    "filters": {"type": "array", "description": "Filter conditions [field, op, value]", "required": False},
                    "order_by": {"type": "string", "description": "Field to order by", "required": False},
                    "order_direction": {"type": "string", "description": "asc or desc", "required": False},
                    "limit": {"type": "integer", "description": "Max documents", "required": False},
                },
            },
            "batch_write": {
                "description": "Batch write operations",
                "parameters": {
                    "operations": {"type": "array", "description": "Array of {type, collection, id, data}", "required": True},
                },
            },
            "list_collections": {
                "description": "List root collections",
                "parameters": {},
            },
            "get_subcollection": {
                "description": "Query a subcollection",
                "parameters": {
                    "parent_collection": {"type": "string", "description": "Parent collection", "required": True},
                    "parent_id": {"type": "string", "description": "Parent document ID", "required": True},
                    "subcollection": {"type": "string", "description": "Subcollection name", "required": True},
                    "limit": {"type": "integer", "description": "Max documents", "required": False},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            db = await self._get_firestore()

            if action == "get_document":
                return await self._get_document(db, params["collection"], params["document_id"])
            elif action == "set_document":
                return await self._set_document(
                    db, params["collection"], params["document_id"],
                    params["data"], params.get("merge", False)
                )
            elif action == "add_document":
                return await self._add_document(db, params["collection"], params["data"])
            elif action == "update_document":
                return await self._update_document(
                    db, params["collection"], params["document_id"], params["data"]
                )
            elif action == "delete_document":
                return await self._delete_document(db, params["collection"], params["document_id"])
            elif action == "query":
                return await self._query(db, params)
            elif action == "batch_write":
                return await self._batch_write(db, params["operations"])
            elif action == "list_collections":
                return await self._list_collections(db)
            elif action == "get_subcollection":
                return await self._get_subcollection(db, params)
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    def _serialize_doc(self, doc) -> dict:
        """Convert Firestore document to dict."""
        data = doc.to_dict() if doc.exists else None
        if data:
            for k, v in data.items():
                if hasattr(v, "isoformat"):
                    data[k] = v.isoformat()
        return {"id": doc.id, "data": data, "exists": doc.exists}

    async def _get_document(self, db, collection: str, doc_id: str) -> ConnectorResult:
        doc = db.collection(collection).document(doc_id).get()
        return ConnectorResult(success=True, data=self._serialize_doc(doc))

    async def _set_document(self, db, collection: str, doc_id: str, data: dict, merge: bool) -> ConnectorResult:
        db.collection(collection).document(doc_id).set(data, merge=merge)
        return ConnectorResult(success=True, data={"id": doc_id, "set": True})

    async def _add_document(self, db, collection: str, data: dict) -> ConnectorResult:
        _, doc_ref = db.collection(collection).add(data)
        return ConnectorResult(success=True, data={"id": doc_ref.id})

    async def _update_document(self, db, collection: str, doc_id: str, data: dict) -> ConnectorResult:
        db.collection(collection).document(doc_id).update(data)
        return ConnectorResult(success=True, data={"id": doc_id, "updated": True})

    async def _delete_document(self, db, collection: str, doc_id: str) -> ConnectorResult:
        db.collection(collection).document(doc_id).delete()
        return ConnectorResult(success=True, data={"id": doc_id, "deleted": True})

    async def _query(self, db, params: dict) -> ConnectorResult:
        from google.cloud.firestore_v1.base_query import FieldFilter

        query = db.collection(params["collection"])

        if params.get("filters"):
            for f in params["filters"]:
                if len(f) == 3:
                    query = query.where(filter=FieldFilter(f[0], f[1], f[2]))

        if params.get("order_by"):
            from google.cloud import firestore
            direction = firestore.Query.DESCENDING if params.get("order_direction") == "desc" else firestore.Query.ASCENDING
            query = query.order_by(params["order_by"], direction=direction)

        if params.get("limit"):
            query = query.limit(params["limit"])

        docs = query.stream()
        documents = [self._serialize_doc(doc) for doc in docs]

        return ConnectorResult(success=True, data={"documents": documents, "count": len(documents)})

    async def _batch_write(self, db, operations: list) -> ConnectorResult:
        batch = db.batch()

        for op in operations:
            ref = db.collection(op["collection"]).document(op.get("id"))

            if op["type"] == "set":
                batch.set(ref, op["data"], merge=op.get("merge", False))
            elif op["type"] == "update":
                batch.update(ref, op["data"])
            elif op["type"] == "delete":
                batch.delete(ref)

        batch.commit()
        return ConnectorResult(success=True, data={"committed": len(operations)})

    async def _list_collections(self, db) -> ConnectorResult:
        collections = [c.id for c in db.collections()]
        return ConnectorResult(success=True, data={"collections": collections})

    async def _get_subcollection(self, db, params: dict) -> ConnectorResult:
        query = (
            db.collection(params["parent_collection"])
            .document(params["parent_id"])
            .collection(params["subcollection"])
        )

        if params.get("limit"):
            query = query.limit(params["limit"])

        docs = query.stream()
        documents = [self._serialize_doc(doc) for doc in docs]

        return ConnectorResult(success=True, data={"documents": documents, "count": len(documents)})

    async def close(self):
        # Firebase Admin SDK doesn't require explicit cleanup
        pass
