"""
Amazon DynamoDB Connector

Connect to Amazon DynamoDB for NoSQL operations.
"""

from typing import Any
from ..base import BaseConnector, ConnectorResult


class DynamoDBConnector(BaseConnector):
    """Connector for Amazon DynamoDB."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.region = credentials.get("region", "us-east-1")
        self.access_key_id = credentials.get("access_key_id")
        self.secret_access_key = credentials.get("secret_access_key")
        self.endpoint_url = credentials.get("endpoint_url")  # For local DynamoDB
        self._client = None
        self._resource = None

    async def _get_client(self):
        """Get DynamoDB client."""
        if self._client is None:
            import boto3
            session = boto3.Session(
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                region_name=self.region,
            )
            kwargs = {"endpoint_url": self.endpoint_url} if self.endpoint_url else {}
            self._client = session.client("dynamodb", **kwargs)
            self._resource = session.resource("dynamodb", **kwargs)
        return self._client, self._resource

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "put_item": {
                "description": "Insert or replace an item",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "item": {"type": "object", "description": "Item to insert", "required": True},
                },
            },
            "get_item": {
                "description": "Get an item by key",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "key": {"type": "object", "description": "Primary key", "required": True},
                },
            },
            "update_item": {
                "description": "Update an existing item",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "key": {"type": "object", "description": "Primary key", "required": True},
                    "updates": {"type": "object", "description": "Attributes to update", "required": True},
                },
            },
            "delete_item": {
                "description": "Delete an item by key",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "key": {"type": "object", "description": "Primary key", "required": True},
                },
            },
            "query": {
                "description": "Query items by partition key",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "key_condition": {"type": "string", "description": "Key condition expression", "required": True},
                    "expression_values": {"type": "object", "description": "Expression attribute values", "required": True},
                    "filter_expression": {"type": "string", "description": "Filter expression", "required": False},
                    "limit": {"type": "integer", "description": "Max items to return", "required": False},
                },
            },
            "scan": {
                "description": "Scan entire table",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "filter_expression": {"type": "string", "description": "Filter expression", "required": False},
                    "expression_values": {"type": "object", "description": "Expression attribute values", "required": False},
                    "limit": {"type": "integer", "description": "Max items to return", "required": False},
                },
            },
            "batch_write": {
                "description": "Batch write (put/delete) items",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "items": {"type": "array", "description": "Items to put", "required": False},
                    "delete_keys": {"type": "array", "description": "Keys to delete", "required": False},
                },
            },
            "batch_get": {
                "description": "Batch get items by keys",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "keys": {"type": "array", "description": "Keys to fetch", "required": True},
                },
            },
            "list_tables": {
                "description": "List all tables",
                "parameters": {},
            },
            "describe_table": {
                "description": "Get table details",
                "parameters": {
                    "table": {"type": "string", "description": "Table name", "required": True},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            client, resource = await self._get_client()

            if action == "put_item":
                return await self._put_item(resource, params["table"], params["item"])
            elif action == "get_item":
                return await self._get_item(resource, params["table"], params["key"])
            elif action == "update_item":
                return await self._update_item(resource, params["table"], params["key"], params["updates"])
            elif action == "delete_item":
                return await self._delete_item(resource, params["table"], params["key"])
            elif action == "query":
                return await self._query(resource, params)
            elif action == "scan":
                return await self._scan(resource, params)
            elif action == "batch_write":
                return await self._batch_write(resource, params["table"], params.get("items", []), params.get("delete_keys", []))
            elif action == "batch_get":
                return await self._batch_get(resource, params["table"], params["keys"])
            elif action == "list_tables":
                return await self._list_tables(client)
            elif action == "describe_table":
                return await self._describe_table(client, params["table"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _put_item(self, resource, table: str, item: dict) -> ConnectorResult:
        tbl = resource.Table(table)
        tbl.put_item(Item=item)
        return ConnectorResult(success=True, data={"inserted": 1})

    async def _get_item(self, resource, table: str, key: dict) -> ConnectorResult:
        tbl = resource.Table(table)
        response = tbl.get_item(Key=key)
        item = response.get("Item")
        return ConnectorResult(success=True, data={"item": item})

    async def _update_item(self, resource, table: str, key: dict, updates: dict) -> ConnectorResult:
        tbl = resource.Table(table)

        update_expr_parts = []
        expr_attr_values = {}
        expr_attr_names = {}

        for i, (k, v) in enumerate(updates.items()):
            attr_name = f"#attr{i}"
            attr_value = f":val{i}"
            update_expr_parts.append(f"{attr_name} = {attr_value}")
            expr_attr_names[attr_name] = k
            expr_attr_values[attr_value] = v

        response = tbl.update_item(
            Key=key,
            UpdateExpression="SET " + ", ".join(update_expr_parts),
            ExpressionAttributeNames=expr_attr_names,
            ExpressionAttributeValues=expr_attr_values,
            ReturnValues="ALL_NEW",
        )

        return ConnectorResult(success=True, data={"updated": response.get("Attributes")})

    async def _delete_item(self, resource, table: str, key: dict) -> ConnectorResult:
        tbl = resource.Table(table)
        tbl.delete_item(Key=key)
        return ConnectorResult(success=True, data={"deleted": 1})

    async def _query(self, resource, params: dict) -> ConnectorResult:
        tbl = resource.Table(params["table"])

        kwargs = {
            "KeyConditionExpression": params["key_condition"],
            "ExpressionAttributeValues": params["expression_values"],
        }

        if params.get("filter_expression"):
            kwargs["FilterExpression"] = params["filter_expression"]
        if params.get("limit"):
            kwargs["Limit"] = params["limit"]

        response = tbl.query(**kwargs)
        return ConnectorResult(
            success=True,
            data={"items": response.get("Items", []), "count": response.get("Count", 0)}
        )

    async def _scan(self, resource, params: dict) -> ConnectorResult:
        tbl = resource.Table(params["table"])

        kwargs = {}
        if params.get("filter_expression"):
            kwargs["FilterExpression"] = params["filter_expression"]
        if params.get("expression_values"):
            kwargs["ExpressionAttributeValues"] = params["expression_values"]
        if params.get("limit"):
            kwargs["Limit"] = params["limit"]

        response = tbl.scan(**kwargs)
        return ConnectorResult(
            success=True,
            data={"items": response.get("Items", []), "count": response.get("Count", 0)}
        )

    async def _batch_write(self, resource, table: str, items: list, delete_keys: list) -> ConnectorResult:
        tbl = resource.Table(table)

        with tbl.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
            for key in delete_keys:
                batch.delete_item(Key=key)

        return ConnectorResult(
            success=True,
            data={"put": len(items), "deleted": len(delete_keys)}
        )

    async def _batch_get(self, resource, table: str, keys: list) -> ConnectorResult:
        response = resource.batch_get_item(
            RequestItems={table: {"Keys": keys}}
        )
        items = response.get("Responses", {}).get(table, [])
        return ConnectorResult(success=True, data={"items": items, "count": len(items)})

    async def _list_tables(self, client) -> ConnectorResult:
        response = client.list_tables()
        return ConnectorResult(success=True, data={"tables": response.get("TableNames", [])})

    async def _describe_table(self, client, table: str) -> ConnectorResult:
        response = client.describe_table(TableName=table)
        table_desc = response.get("Table", {})
        return ConnectorResult(
            success=True,
            data={
                "name": table_desc.get("TableName"),
                "status": table_desc.get("TableStatus"),
                "item_count": table_desc.get("ItemCount"),
                "size_bytes": table_desc.get("TableSizeBytes"),
                "key_schema": table_desc.get("KeySchema"),
                "attributes": table_desc.get("AttributeDefinitions"),
            }
        )

    async def close(self):
        self._client = None
        self._resource = None
