"""
Google Cloud Storage Connector

Connect to Google Cloud Storage for object storage operations.
"""

from typing import Any
import base64
from ..base import BaseConnector, ConnectorResult


class GCSConnector(BaseConnector):
    """Connector for Google Cloud Storage."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.project_id = credentials.get("project_id")
        self.credentials_json = credentials.get("credentials_json")
        self._client = None

    async def _get_client(self):
        """Get GCS client."""
        if self._client is None:
            from google.cloud import storage
            from google.oauth2 import service_account
            import json

            if isinstance(self.credentials_json, str):
                creds_dict = json.loads(self.credentials_json)
            else:
                creds_dict = self.credentials_json

            credentials = service_account.Credentials.from_service_account_info(creds_dict)
            self._client = storage.Client(project=self.project_id, credentials=credentials)
        return self._client

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "upload": {
                "description": "Upload a file to GCS",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                    "blob_name": {"type": "string", "description": "Object name (path)", "required": True},
                    "content": {"type": "string", "description": "Base64-encoded content", "required": True},
                    "content_type": {"type": "string", "description": "MIME type", "required": False},
                },
            },
            "download": {
                "description": "Download a file from GCS",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                    "blob_name": {"type": "string", "description": "Object name", "required": True},
                },
            },
            "delete": {
                "description": "Delete an object",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                    "blob_name": {"type": "string", "description": "Object name", "required": True},
                },
            },
            "list_blobs": {
                "description": "List objects in a bucket",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                    "prefix": {"type": "string", "description": "Name prefix", "required": False},
                    "max_results": {"type": "integer", "description": "Max objects", "required": False},
                },
            },
            "copy": {
                "description": "Copy an object",
                "parameters": {
                    "source_bucket": {"type": "string", "description": "Source bucket", "required": True},
                    "source_blob": {"type": "string", "description": "Source object", "required": True},
                    "dest_bucket": {"type": "string", "description": "Destination bucket", "required": True},
                    "dest_blob": {"type": "string", "description": "Destination object", "required": True},
                },
            },
            "get_signed_url": {
                "description": "Generate a signed URL",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                    "blob_name": {"type": "string", "description": "Object name", "required": True},
                    "method": {"type": "string", "description": "GET or PUT", "required": False},
                    "expires_in": {"type": "integer", "description": "Expiry in minutes", "required": False},
                },
            },
            "list_buckets": {
                "description": "List all buckets",
                "parameters": {},
            },
            "create_bucket": {
                "description": "Create a bucket",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                    "location": {"type": "string", "description": "Location (e.g., US)", "required": False},
                },
            },
            "delete_bucket": {
                "description": "Delete a bucket",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                },
            },
            "get_blob_metadata": {
                "description": "Get object metadata",
                "parameters": {
                    "bucket": {"type": "string", "description": "Bucket name", "required": True},
                    "blob_name": {"type": "string", "description": "Object name", "required": True},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            client = await self._get_client()

            if action == "upload":
                return await self._upload(client, params)
            elif action == "download":
                return await self._download(client, params["bucket"], params["blob_name"])
            elif action == "delete":
                return await self._delete(client, params["bucket"], params["blob_name"])
            elif action == "list_blobs":
                return await self._list_blobs(client, params)
            elif action == "copy":
                return await self._copy(client, params)
            elif action == "get_signed_url":
                return await self._get_signed_url(client, params)
            elif action == "list_buckets":
                return await self._list_buckets(client)
            elif action == "create_bucket":
                return await self._create_bucket(client, params["bucket"], params.get("location", "US"))
            elif action == "delete_bucket":
                return await self._delete_bucket(client, params["bucket"])
            elif action == "get_blob_metadata":
                return await self._get_blob_metadata(client, params["bucket"], params["blob_name"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _upload(self, client, params: dict) -> ConnectorResult:
        content = base64.b64decode(params["content"])
        bucket = client.bucket(params["bucket"])
        blob = bucket.blob(params["blob_name"])

        blob.upload_from_string(content, content_type=params.get("content_type"))
        return ConnectorResult(success=True, data={"blob": params["blob_name"], "bucket": params["bucket"]})

    async def _download(self, client, bucket_name: str, blob_name: str) -> ConnectorResult:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        content = blob.download_as_bytes()

        return ConnectorResult(
            success=True,
            data={
                "content": base64.b64encode(content).decode(),
                "content_type": blob.content_type,
                "size": blob.size,
            }
        )

    async def _delete(self, client, bucket_name: str, blob_name: str) -> ConnectorResult:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        return ConnectorResult(success=True, data={"deleted": blob_name})

    async def _list_blobs(self, client, params: dict) -> ConnectorResult:
        bucket = client.bucket(params["bucket"])
        blobs = bucket.list_blobs(
            prefix=params.get("prefix"),
            max_results=params.get("max_results")
        )

        blob_list = [
            {
                "name": blob.name,
                "size": blob.size,
                "updated": blob.updated.isoformat() if blob.updated else None,
                "content_type": blob.content_type,
            }
            for blob in blobs
        ]
        return ConnectorResult(success=True, data={"blobs": blob_list, "count": len(blob_list)})

    async def _copy(self, client, params: dict) -> ConnectorResult:
        source_bucket = client.bucket(params["source_bucket"])
        source_blob = source_bucket.blob(params["source_blob"])
        dest_bucket = client.bucket(params["dest_bucket"])

        source_bucket.copy_blob(source_blob, dest_bucket, params["dest_blob"])
        return ConnectorResult(success=True, data={"copied": params["dest_blob"]})

    async def _get_signed_url(self, client, params: dict) -> ConnectorResult:
        from datetime import timedelta

        bucket = client.bucket(params["bucket"])
        blob = bucket.blob(params["blob_name"])

        url = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=params.get("expires_in", 60)),
            method=params.get("method", "GET"),
        )
        return ConnectorResult(success=True, data={"url": url})

    async def _list_buckets(self, client) -> ConnectorResult:
        buckets = list(client.list_buckets())
        bucket_list = [
            {"name": b.name, "created": b.time_created.isoformat() if b.time_created else None}
            for b in buckets
        ]
        return ConnectorResult(success=True, data={"buckets": bucket_list})

    async def _create_bucket(self, client, bucket_name: str, location: str) -> ConnectorResult:
        bucket = client.bucket(bucket_name)
        bucket.location = location
        client.create_bucket(bucket)
        return ConnectorResult(success=True, data={"created": bucket_name})

    async def _delete_bucket(self, client, bucket_name: str) -> ConnectorResult:
        bucket = client.bucket(bucket_name)
        bucket.delete()
        return ConnectorResult(success=True, data={"deleted": bucket_name})

    async def _get_blob_metadata(self, client, bucket_name: str, blob_name: str) -> ConnectorResult:
        bucket = client.bucket(bucket_name)
        blob = bucket.get_blob(blob_name)

        if not blob:
            return ConnectorResult(success=False, error="Blob not found")

        return ConnectorResult(
            success=True,
            data={
                "name": blob.name,
                "size": blob.size,
                "content_type": blob.content_type,
                "updated": blob.updated.isoformat() if blob.updated else None,
                "md5_hash": blob.md5_hash,
                "etag": blob.etag,
            }
        )

    async def close(self):
        self._client = None
