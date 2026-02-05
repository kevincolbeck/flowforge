"""
Elasticsearch Connector

Connect to Elasticsearch for search and analytics.
"""

from typing import Any
from ..base import BaseConnector, ConnectorResult


class ElasticsearchConnector(BaseConnector):
    """Connector for Elasticsearch."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.hosts = credentials.get("hosts", ["localhost:9200"])
        self.cloud_id = credentials.get("cloud_id")  # For Elastic Cloud
        self.api_key = credentials.get("api_key")
        self.username = credentials.get("username")
        self.password = credentials.get("password")
        self._client = None

    async def _get_client(self):
        """Get Elasticsearch client."""
        if self._client is None:
            from elasticsearch import AsyncElasticsearch

            if self.cloud_id:
                self._client = AsyncElasticsearch(
                    cloud_id=self.cloud_id,
                    api_key=self.api_key,
                )
            elif self.api_key:
                self._client = AsyncElasticsearch(
                    hosts=self.hosts,
                    api_key=self.api_key,
                )
            else:
                self._client = AsyncElasticsearch(
                    hosts=self.hosts,
                    basic_auth=(self.username, self.password) if self.username else None,
                )
        return self._client

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "index": {
                "description": "Index a document",
                "parameters": {
                    "index": {"type": "string", "description": "Index name", "required": True},
                    "document": {"type": "object", "description": "Document to index", "required": True},
                    "id": {"type": "string", "description": "Document ID", "required": False},
                },
            },
            "bulk": {
                "description": "Bulk index documents",
                "parameters": {
                    "index": {"type": "string", "description": "Index name", "required": True},
                    "documents": {"type": "array", "description": "Documents to index", "required": True},
                },
            },
            "get": {
                "description": "Get a document by ID",
                "parameters": {
                    "index": {"type": "string", "description": "Index name", "required": True},
                    "id": {"type": "string", "description": "Document ID", "required": True},
                },
            },
            "search": {
                "description": "Search for documents",
                "parameters": {
                    "index": {"type": "string", "description": "Index name", "required": True},
                    "query": {"type": "object", "description": "Search query", "required": True},
                    "size": {"type": "integer", "description": "Max results", "required": False},
                    "from": {"type": "integer", "description": "Offset", "required": False},
                    "sort": {"type": "array", "description": "Sort order", "required": False},
                },
            },
            "update": {
                "description": "Update a document",
                "parameters": {
                    "index": {"type": "string", "description": "Index name", "required": True},
                    "id": {"type": "string", "description": "Document ID", "required": True},
                    "doc": {"type": "object", "description": "Fields to update", "required": True},
                },
            },
            "delete": {
                "description": "Delete a document",
                "parameters": {
                    "index": {"type": "string", "description": "Index name", "required": True},
                    "id": {"type": "string", "description": "Document ID", "required": True},
                },
            },
            "delete_by_query": {
                "description": "Delete documents matching a query",
                "parameters": {
                    "index": {"type": "string", "description": "Index name", "required": True},
                    "query": {"type": "object", "description": "Query to match", "required": True},
                },
            },
            "count": {
                "description": "Count documents",
                "parameters": {
                    "index": {"type": "string", "description": "Index name", "required": True},
                    "query": {"type": "object", "description": "Query to match", "required": False},
                },
            },
            "aggregate": {
                "description": "Run aggregations",
                "parameters": {
                    "index": {"type": "string", "description": "Index name", "required": True},
                    "aggs": {"type": "object", "description": "Aggregations", "required": True},
                    "query": {"type": "object", "description": "Filter query", "required": False},
                },
            },
            "create_index": {
                "description": "Create an index",
                "parameters": {
                    "index": {"type": "string", "description": "Index name", "required": True},
                    "mappings": {"type": "object", "description": "Index mappings", "required": False},
                    "settings": {"type": "object", "description": "Index settings", "required": False},
                },
            },
            "delete_index": {
                "description": "Delete an index",
                "parameters": {
                    "index": {"type": "string", "description": "Index name", "required": True},
                },
            },
            "list_indices": {
                "description": "List all indices",
                "parameters": {},
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            client = await self._get_client()

            if action == "index":
                return await self._index(client, params["index"], params["document"], params.get("id"))
            elif action == "bulk":
                return await self._bulk(client, params["index"], params["documents"])
            elif action == "get":
                return await self._get(client, params["index"], params["id"])
            elif action == "search":
                return await self._search(client, params)
            elif action == "update":
                return await self._update(client, params["index"], params["id"], params["doc"])
            elif action == "delete":
                return await self._delete(client, params["index"], params["id"])
            elif action == "delete_by_query":
                return await self._delete_by_query(client, params["index"], params["query"])
            elif action == "count":
                return await self._count(client, params["index"], params.get("query"))
            elif action == "aggregate":
                return await self._aggregate(client, params["index"], params["aggs"], params.get("query"))
            elif action == "create_index":
                return await self._create_index(client, params["index"], params.get("mappings"), params.get("settings"))
            elif action == "delete_index":
                return await self._delete_index(client, params["index"])
            elif action == "list_indices":
                return await self._list_indices(client)
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _index(self, client, index: str, document: dict, id: str | None) -> ConnectorResult:
        kwargs = {"index": index, "document": document}
        if id:
            kwargs["id"] = id

        result = await client.index(**kwargs)
        return ConnectorResult(
            success=True,
            data={"id": result["_id"], "result": result["result"], "version": result["_version"]}
        )

    async def _bulk(self, client, index: str, documents: list[dict]) -> ConnectorResult:
        from elasticsearch.helpers import async_bulk

        actions = [
            {"_index": index, "_source": doc}
            for doc in documents
        ]

        success, failed = await async_bulk(client, actions, stats_only=True)
        return ConnectorResult(
            success=True,
            data={"indexed": success, "failed": failed}
        )

    async def _get(self, client, index: str, id: str) -> ConnectorResult:
        result = await client.get(index=index, id=id)
        return ConnectorResult(
            success=True,
            data={"id": result["_id"], "document": result["_source"], "found": result["found"]}
        )

    async def _search(self, client, params: dict) -> ConnectorResult:
        kwargs = {
            "index": params["index"],
            "query": params["query"],
        }
        if params.get("size"):
            kwargs["size"] = params["size"]
        if params.get("from"):
            kwargs["from_"] = params["from"]
        if params.get("sort"):
            kwargs["sort"] = params["sort"]

        result = await client.search(**kwargs)

        hits = result["hits"]
        documents = [
            {"id": hit["_id"], "score": hit.get("_score"), **hit["_source"]}
            for hit in hits["hits"]
        ]

        return ConnectorResult(
            success=True,
            data={
                "documents": documents,
                "total": hits["total"]["value"],
                "max_score": hits.get("max_score"),
            }
        )

    async def _update(self, client, index: str, id: str, doc: dict) -> ConnectorResult:
        result = await client.update(index=index, id=id, doc=doc)
        return ConnectorResult(
            success=True,
            data={"id": result["_id"], "result": result["result"], "version": result["_version"]}
        )

    async def _delete(self, client, index: str, id: str) -> ConnectorResult:
        result = await client.delete(index=index, id=id)
        return ConnectorResult(success=True, data={"id": result["_id"], "result": result["result"]})

    async def _delete_by_query(self, client, index: str, query: dict) -> ConnectorResult:
        result = await client.delete_by_query(index=index, query=query)
        return ConnectorResult(
            success=True,
            data={"deleted": result["deleted"], "total": result["total"]}
        )

    async def _count(self, client, index: str, query: dict | None) -> ConnectorResult:
        kwargs = {"index": index}
        if query:
            kwargs["query"] = query

        result = await client.count(**kwargs)
        return ConnectorResult(success=True, data={"count": result["count"]})

    async def _aggregate(self, client, index: str, aggs: dict, query: dict | None) -> ConnectorResult:
        kwargs = {"index": index, "aggs": aggs, "size": 0}
        if query:
            kwargs["query"] = query

        result = await client.search(**kwargs)
        return ConnectorResult(success=True, data={"aggregations": result.get("aggregations", {})})

    async def _create_index(self, client, index: str, mappings: dict | None, settings: dict | None) -> ConnectorResult:
        body = {}
        if mappings:
            body["mappings"] = mappings
        if settings:
            body["settings"] = settings

        await client.indices.create(index=index, body=body if body else None)
        return ConnectorResult(success=True, data={"index": index, "created": True})

    async def _delete_index(self, client, index: str) -> ConnectorResult:
        await client.indices.delete(index=index)
        return ConnectorResult(success=True, data={"index": index, "deleted": True})

    async def _list_indices(self, client) -> ConnectorResult:
        result = await client.cat.indices(format="json")
        indices = [{"name": idx["index"], "docs": idx.get("docs.count"), "size": idx.get("store.size")} for idx in result]
        return ConnectorResult(success=True, data={"indices": indices})

    async def close(self):
        if self._client:
            await self._client.close()
            self._client = None
