"""
AWS S3 Connector

Connect to Amazon S3 for object storage operations.
"""

from typing import Any
import base64
from ..base import BaseConnector, ConnectorResult


class AWSS3Connector(BaseConnector):
    """Connector for AWS S3."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.access_key_id = credentials.get("access_key_id")
        self.secret_access_key = credentials.get("secret_access_key")
        self.region = credentials.get("region", "us-east-1")
        self.endpoint_url = credentials.get("endpoint_url")  # For S3-compatible storage
        self._client = None

    async def _get_client(self):
        """Get S3 client."""
        if self._client is None:
            import boto3
            session = boto3.Session(
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                region_name=self.region,
            )
            kwargs = {"endpoint_url": self.endpoint_url} if self.endpoint_url else {}
            self._client = session.client("s3", **kwargs)
        return self._client

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "upload": {
                "description": "Upload a file to S3",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                    "key": {"type": "string", "description": "Object key (path)", "required": True},
                    "content": {"type": "string", "description": "Base64-encoded content", "required": True},
                    "content_type": {"type": "string", "description": "MIME type", "required": False},
                },
            },
            "download": {
                "description": "Download a file from S3",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                    "key": {"type": "string", "description": "Object key", "required": True},
                },
            },
            "delete": {
                "description": "Delete an object",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                    "key": {"type": "string", "description": "Object key", "required": True},
                },
            },
            "list_objects": {
                "description": "List objects in a bucket",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                    "prefix": {"type": "string", "description": "Key prefix", "required": False},
                    "max_keys": {"type": "integer", "description": "Max objects to return", "required": False},
                },
            },
            "copy": {
                "description": "Copy an object",
                "parameters": {
                    "source_bucket": {"type": "string", "description": "Source bucket", "required": True},
                    "source_key": {"type": "string", "description": "Source key", "required": True},
                    "dest_bucket": {"type": "string", "description": "Destination bucket", "required": True},
                    "dest_key": {"type": "string", "description": "Destination key", "required": True},
                },
            },
            "move": {
                "description": "Move an object (copy + delete)",
                "parameters": {
                    "source_bucket": {"type": "string", "description": "Source bucket", "required": True},
                    "source_key": {"type": "string", "description": "Source key", "required": True},
                    "dest_bucket": {"type": "string", "description": "Destination bucket", "required": True},
                    "dest_key": {"type": "string", "description": "Destination key", "required": True},
                },
            },
            "get_presigned_url": {
                "description": "Generate a presigned URL",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                    "key": {"type": "string", "description": "Object key", "required": True},
                    "operation": {"type": "string", "description": "get_object or put_object", "required": False},
                    "expires_in": {"type": "integer", "description": "URL expiry in seconds", "required": False},
                },
            },
            "list_buckets": {
                "description": "List all buckets",
                "parameters": {},
            },
            "create_bucket": {
                "description": "Create a new bucket",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                },
            },
            "delete_bucket": {
                "description": "Delete a bucket",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                },
            },
            "head_object": {
                "description": "Get object metadata",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                    "key": {"type": "string", "description": "Object key", "required": True},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            client = await self._get_client()

            if action == "upload":
                return await self._upload(client, params)
            elif action == "download":
                return await self._download(client, params["bucket"], params["key"])
            elif action == "delete":
                return await self._delete(client, params["bucket"], params["key"])
            elif action == "list_objects":
                return await self._list_objects(client, params)
            elif action == "copy":
                return await self._copy(client, params)
            elif action == "move":
                return await self._move(client, params)
            elif action == "get_presigned_url":
                return await self._get_presigned_url(client, params)
            elif action == "list_buckets":
                return await self._list_buckets(client)
            elif action == "create_bucket":
                return await self._create_bucket(client, params["bucket"])
            elif action == "delete_bucket":
                return await self._delete_bucket(client, params["bucket"])
            elif action == "head_object":
                return await self._head_object(client, params["bucket"], params["key"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _upload(self, client, params: dict) -> ConnectorResult:
        content = base64.b64decode(params["content"])
        extra_args = {}
        if params.get("content_type"):
            extra_args["ContentType"] = params["content_type"]

        client.put_object(
            Bucket=params["bucket"],
            Key=params["key"],
            Body=content,
            **extra_args
        )
        return ConnectorResult(success=True, data={"key": params["key"], "bucket": params["bucket"]})

    async def _download(self, client, bucket: str, key: str) -> ConnectorResult:
        response = client.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read()
        return ConnectorResult(
            success=True,
            data={
                "content": base64.b64encode(content).decode(),
                "content_type": response.get("ContentType"),
                "content_length": response.get("ContentLength"),
            }
        )

    async def _delete(self, client, bucket: str, key: str) -> ConnectorResult:
        client.delete_object(Bucket=bucket, Key=key)
        return ConnectorResult(success=True, data={"deleted": key})

    async def _list_objects(self, client, params: dict) -> ConnectorResult:
        kwargs = {"Bucket": params["bucket"]}
        if params.get("prefix"):
            kwargs["Prefix"] = params["prefix"]
        if params.get("max_keys"):
            kwargs["MaxKeys"] = params["max_keys"]

        response = client.list_objects_v2(**kwargs)
        objects = [
            {
                "key": obj["Key"],
                "size": obj["Size"],
                "last_modified": obj["LastModified"].isoformat(),
            }
            for obj in response.get("Contents", [])
        ]
        return ConnectorResult(success=True, data={"objects": objects, "count": len(objects)})

    async def _copy(self, client, params: dict) -> ConnectorResult:
        client.copy_object(
            Bucket=params["dest_bucket"],
            Key=params["dest_key"],
            CopySource={"Bucket": params["source_bucket"], "Key": params["source_key"]}
        )
        return ConnectorResult(success=True, data={"copied": params["dest_key"]})

    async def _move(self, client, params: dict) -> ConnectorResult:
        await self._copy(client, params)
        await self._delete(client, params["source_bucket"], params["source_key"])
        return ConnectorResult(success=True, data={"moved": params["dest_key"]})

    async def _get_presigned_url(self, client, params: dict) -> ConnectorResult:
        url = client.generate_presigned_url(
            params.get("operation", "get_object"),
            Params={"Bucket": params["bucket"], "Key": params["key"]},
            ExpiresIn=params.get("expires_in", 3600)
        )
        return ConnectorResult(success=True, data={"url": url})

    async def _list_buckets(self, client) -> ConnectorResult:
        response = client.list_buckets()
        buckets = [
            {"name": b["Name"], "created": b["CreationDate"].isoformat()}
            for b in response.get("Buckets", [])
        ]
        return ConnectorResult(success=True, data={"buckets": buckets})

    async def _create_bucket(self, client, bucket: str) -> ConnectorResult:
        kwargs = {"Bucket": bucket}
        if self.region != "us-east-1":
            kwargs["CreateBucketConfiguration"] = {"LocationConstraint": self.region}
        client.create_bucket(**kwargs)
        return ConnectorResult(success=True, data={"created": bucket})

    async def _delete_bucket(self, client, bucket: str) -> ConnectorResult:
        client.delete_bucket(Bucket=bucket)
        return ConnectorResult(success=True, data={"deleted": bucket})

    async def _head_object(self, client, bucket: str, key: str) -> ConnectorResult:
        response = client.head_object(Bucket=bucket, Key=key)
        return ConnectorResult(
            success=True,
            data={
                "content_type": response.get("ContentType"),
                "content_length": response.get("ContentLength"),
                "last_modified": response.get("LastModified").isoformat() if response.get("LastModified") else None,
                "etag": response.get("ETag"),
            }
        )

    async def close(self):
        self._client = None
