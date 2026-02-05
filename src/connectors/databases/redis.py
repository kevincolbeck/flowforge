"""
Redis Connector

Connect to Redis for caching, pub/sub, and data storage.
"""

from typing import Any
from ..base import BaseConnector, ConnectorResult


class RedisConnector(BaseConnector):
    """Connector for Redis."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.host = credentials.get("host", "localhost")
        self.port = credentials.get("port", 6379)
        self.password = credentials.get("password")
        self.db = credentials.get("db", 0)
        self.url = credentials.get("url")  # redis://... or rediss://...
        self._client = None

    async def _get_client(self):
        """Get Redis client."""
        if self._client is None:
            import redis.asyncio as redis

            if self.url:
                self._client = redis.from_url(self.url)
            else:
                self._client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    password=self.password,
                    db=self.db,
                    decode_responses=True,
                )
        return self._client

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "get": {
                "description": "Get a value by key",
                "parameters": {
                    "key": {"type": "string", "description": "Key name", "required": True},
                },
            },
            "set": {
                "description": "Set a key-value pair",
                "parameters": {
                    "key": {"type": "string", "description": "Key name", "required": True},
                    "value": {"type": "string", "description": "Value to set", "required": True},
                    "ex": {"type": "integer", "description": "Expiry in seconds", "required": False},
                    "px": {"type": "integer", "description": "Expiry in milliseconds", "required": False},
                },
            },
            "delete": {
                "description": "Delete keys",
                "parameters": {
                    "keys": {"type": "array", "description": "Keys to delete", "required": True},
                },
            },
            "mget": {
                "description": "Get multiple values",
                "parameters": {
                    "keys": {"type": "array", "description": "Keys to get", "required": True},
                },
            },
            "mset": {
                "description": "Set multiple key-value pairs",
                "parameters": {
                    "mapping": {"type": "object", "description": "Key-value pairs", "required": True},
                },
            },
            "hget": {
                "description": "Get a hash field",
                "parameters": {
                    "name": {"type": "string", "description": "Hash name", "required": True},
                    "key": {"type": "string", "description": "Field name", "required": True},
                },
            },
            "hset": {
                "description": "Set hash fields",
                "parameters": {
                    "name": {"type": "string", "description": "Hash name", "required": True},
                    "mapping": {"type": "object", "description": "Field-value pairs", "required": True},
                },
            },
            "hgetall": {
                "description": "Get all hash fields",
                "parameters": {
                    "name": {"type": "string", "description": "Hash name", "required": True},
                },
            },
            "lpush": {
                "description": "Push to list (left)",
                "parameters": {
                    "name": {"type": "string", "description": "List name", "required": True},
                    "values": {"type": "array", "description": "Values to push", "required": True},
                },
            },
            "rpush": {
                "description": "Push to list (right)",
                "parameters": {
                    "name": {"type": "string", "description": "List name", "required": True},
                    "values": {"type": "array", "description": "Values to push", "required": True},
                },
            },
            "lrange": {
                "description": "Get list range",
                "parameters": {
                    "name": {"type": "string", "description": "List name", "required": True},
                    "start": {"type": "integer", "description": "Start index", "required": True},
                    "end": {"type": "integer", "description": "End index", "required": True},
                },
            },
            "sadd": {
                "description": "Add to set",
                "parameters": {
                    "name": {"type": "string", "description": "Set name", "required": True},
                    "values": {"type": "array", "description": "Values to add", "required": True},
                },
            },
            "smembers": {
                "description": "Get all set members",
                "parameters": {
                    "name": {"type": "string", "description": "Set name", "required": True},
                },
            },
            "zadd": {
                "description": "Add to sorted set",
                "parameters": {
                    "name": {"type": "string", "description": "Sorted set name", "required": True},
                    "mapping": {"type": "object", "description": "Member-score pairs", "required": True},
                },
            },
            "zrange": {
                "description": "Get sorted set range",
                "parameters": {
                    "name": {"type": "string", "description": "Sorted set name", "required": True},
                    "start": {"type": "integer", "description": "Start index", "required": True},
                    "end": {"type": "integer", "description": "End index", "required": True},
                    "withscores": {"type": "boolean", "description": "Include scores", "required": False},
                },
            },
            "publish": {
                "description": "Publish to channel",
                "parameters": {
                    "channel": {"type": "string", "description": "Channel name", "required": True},
                    "message": {"type": "string", "description": "Message to publish", "required": True},
                },
            },
            "expire": {
                "description": "Set key expiry",
                "parameters": {
                    "key": {"type": "string", "description": "Key name", "required": True},
                    "seconds": {"type": "integer", "description": "TTL in seconds", "required": True},
                },
            },
            "keys": {
                "description": "Find keys matching pattern",
                "parameters": {
                    "pattern": {"type": "string", "description": "Pattern (e.g., user:*)", "required": True},
                },
            },
            "incr": {
                "description": "Increment a key",
                "parameters": {
                    "key": {"type": "string", "description": "Key name", "required": True},
                    "amount": {"type": "integer", "description": "Amount to increment", "required": False},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            client = await self._get_client()

            if action == "get":
                value = await client.get(params["key"])
                return ConnectorResult(success=True, data={"value": value})

            elif action == "set":
                kwargs = {}
                if params.get("ex"):
                    kwargs["ex"] = params["ex"]
                if params.get("px"):
                    kwargs["px"] = params["px"]
                await client.set(params["key"], params["value"], **kwargs)
                return ConnectorResult(success=True, data={"set": True})

            elif action == "delete":
                deleted = await client.delete(*params["keys"])
                return ConnectorResult(success=True, data={"deleted": deleted})

            elif action == "mget":
                values = await client.mget(params["keys"])
                return ConnectorResult(success=True, data={"values": dict(zip(params["keys"], values))})

            elif action == "mset":
                await client.mset(params["mapping"])
                return ConnectorResult(success=True, data={"set": True})

            elif action == "hget":
                value = await client.hget(params["name"], params["key"])
                return ConnectorResult(success=True, data={"value": value})

            elif action == "hset":
                await client.hset(params["name"], mapping=params["mapping"])
                return ConnectorResult(success=True, data={"set": True})

            elif action == "hgetall":
                data = await client.hgetall(params["name"])
                return ConnectorResult(success=True, data={"fields": data})

            elif action == "lpush":
                length = await client.lpush(params["name"], *params["values"])
                return ConnectorResult(success=True, data={"length": length})

            elif action == "rpush":
                length = await client.rpush(params["name"], *params["values"])
                return ConnectorResult(success=True, data={"length": length})

            elif action == "lrange":
                values = await client.lrange(params["name"], params["start"], params["end"])
                return ConnectorResult(success=True, data={"values": values})

            elif action == "sadd":
                added = await client.sadd(params["name"], *params["values"])
                return ConnectorResult(success=True, data={"added": added})

            elif action == "smembers":
                members = await client.smembers(params["name"])
                return ConnectorResult(success=True, data={"members": list(members)})

            elif action == "zadd":
                added = await client.zadd(params["name"], params["mapping"])
                return ConnectorResult(success=True, data={"added": added})

            elif action == "zrange":
                values = await client.zrange(
                    params["name"], params["start"], params["end"],
                    withscores=params.get("withscores", False)
                )
                return ConnectorResult(success=True, data={"values": values})

            elif action == "publish":
                receivers = await client.publish(params["channel"], params["message"])
                return ConnectorResult(success=True, data={"receivers": receivers})

            elif action == "expire":
                result = await client.expire(params["key"], params["seconds"])
                return ConnectorResult(success=True, data={"set": result})

            elif action == "keys":
                keys = await client.keys(params["pattern"])
                return ConnectorResult(success=True, data={"keys": keys})

            elif action == "incr":
                amount = params.get("amount", 1)
                if amount == 1:
                    value = await client.incr(params["key"])
                else:
                    value = await client.incrby(params["key"], amount)
                return ConnectorResult(success=True, data={"value": value})

            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")

        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def close(self):
        if self._client:
            await self._client.close()
            self._client = None
