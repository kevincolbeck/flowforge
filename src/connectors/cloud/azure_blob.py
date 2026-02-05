"""
Azure Blob Storage Connector

Connect to Azure Blob Storage for object storage operations.
"""

from typing import Any
import base64
from ..base import BaseConnector, ConnectorResult


class AzureBlobConnector(BaseConnector):
    """Connector for Azure Blob Storage."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.connection_string = credentials.get("connection_string")
        self.account_name = credentials.get("account_name")
        self.account_key = credentials.get("account_key")
        self.sas_token = credentials.get("sas_token")
        self._service_client = None

    async def _get_client(self):
        """Get Blob service client."""
        if self._service_client is None:
            from azure.storage.blob import BlobServiceClient

            if self.connection_string:
                self._service_client = BlobServiceClient.from_connection_string(self.connection_string)
            elif self.sas_token:
                account_url = f"https://{self.account_name}.blob.core.windows.net"
                self._service_client = BlobServiceClient(account_url=account_url, credential=self.sas_token)
            else:
                account_url = f"https://{self.account_name}.blob.core.windows.net"
                self._service_client = BlobServiceClient(account_url=account_url, credential=self.account_key)
        return self._service_client

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "upload": {
                "description": "Upload a blob",
                "parameters": {
                    "container": {"type": "string", "description": "Container name", "required": True},
                    "blob_name": {"type": "string", "description": "Blob name (path)", "required": True},
                    "content": {"type": "string", "description": "Base64-encoded content", "required": True},
                    "content_type": {"type": "string", "description": "MIME type", "required": False},
                },
            },
            "download": {
                "description": "Download a blob",
                "parameters": {
                    "container": {"type": "string", "description": "Container name", "required": True},
                    "blob_name": {"type": "string", "description": "Blob name", "required": True},
                },
            },
            "delete": {
                "description": "Delete a blob",
                "parameters": {
                    "container": {"type": "string", "description": "Container name", "required": True},
                    "blob_name": {"type": "string", "description": "Blob name", "required": True},
                },
            },
            "list_blobs": {
                "description": "List blobs in a container",
                "parameters": {
                    "container": {"type": "string", "description": "Container name", "required": True},
                    "prefix": {"type": "string", "description": "Name prefix", "required": False},
                },
            },
            "copy": {
                "description": "Copy a blob",
                "parameters": {
                    "source_container": {"type": "string", "description": "Source container", "required": True},
                    "source_blob": {"type": "string", "description": "Source blob", "required": True},
                    "dest_container": {"type": "string", "description": "Destination container", "required": True},
                    "dest_blob": {"type": "string", "description": "Destination blob", "required": True},
                },
            },
            "get_sas_url": {
                "description": "Generate a SAS URL",
                "parameters": {
                    "container": {"type": "string", "description": "Container name", "required": True},
                    "blob_name": {"type": "string", "description": "Blob name", "required": True},
                    "permission": {"type": "string", "description": "r, w, d, or combination", "required": False},
                    "expires_in": {"type": "integer", "description": "Expiry in hours", "required": False},
                },
            },
            "list_containers": {
                "description": "List all containers",
                "parameters": {},
            },
            "create_container": {
                "description": "Create a container",
                "parameters": {
                    "container": {"type": "string", "description": "Container name", "required": True},
                },
            },
            "delete_container": {
                "description": "Delete a container",
                "parameters": {
                    "container": {"type": "string", "description": "Container name", "required": True},
                },
            },
            "get_blob_properties": {
                "description": "Get blob metadata",
                "parameters": {
                    "container": {"type": "string", "description": "Container name", "required": True},
                    "blob_name": {"type": "string", "description": "Blob name", "required": True},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            client = await self._get_client()

            if action == "upload":
                return await self._upload(client, params)
            elif action == "download":
                return await self._download(client, params["container"], params["blob_name"])
            elif action == "delete":
                return await self._delete(client, params["container"], params["blob_name"])
            elif action == "list_blobs":
                return await self._list_blobs(client, params["container"], params.get("prefix"))
            elif action == "copy":
                return await self._copy(client, params)
            elif action == "get_sas_url":
                return await self._get_sas_url(client, params)
            elif action == "list_containers":
                return await self._list_containers(client)
            elif action == "create_container":
                return await self._create_container(client, params["container"])
            elif action == "delete_container":
                return await self._delete_container(client, params["container"])
            elif action == "get_blob_properties":
                return await self._get_blob_properties(client, params["container"], params["blob_name"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _upload(self, client, params: dict) -> ConnectorResult:
        content = base64.b64decode(params["content"])
        blob_client = client.get_blob_client(params["container"], params["blob_name"])

        kwargs = {}
        if params.get("content_type"):
            from azure.storage.blob import ContentSettings
            kwargs["content_settings"] = ContentSettings(content_type=params["content_type"])

        blob_client.upload_blob(content, overwrite=True, **kwargs)
        return ConnectorResult(success=True, data={"blob": params["blob_name"], "container": params["container"]})

    async def _download(self, client, container: str, blob_name: str) -> ConnectorResult:
        blob_client = client.get_blob_client(container, blob_name)
        download = blob_client.download_blob()
        content = download.readall()
        properties = blob_client.get_blob_properties()

        return ConnectorResult(
            success=True,
            data={
                "content": base64.b64encode(content).decode(),
                "content_type": properties.content_settings.content_type,
                "content_length": properties.size,
            }
        )

    async def _delete(self, client, container: str, blob_name: str) -> ConnectorResult:
        blob_client = client.get_blob_client(container, blob_name)
        blob_client.delete_blob()
        return ConnectorResult(success=True, data={"deleted": blob_name})

    async def _list_blobs(self, client, container: str, prefix: str | None) -> ConnectorResult:
        container_client = client.get_container_client(container)
        blobs = container_client.list_blobs(name_starts_with=prefix)

        blob_list = [
            {
                "name": blob.name,
                "size": blob.size,
                "last_modified": blob.last_modified.isoformat() if blob.last_modified else None,
                "content_type": blob.content_settings.content_type if blob.content_settings else None,
            }
            for blob in blobs
        ]
        return ConnectorResult(success=True, data={"blobs": blob_list, "count": len(blob_list)})

    async def _copy(self, client, params: dict) -> ConnectorResult:
        source_blob = client.get_blob_client(params["source_container"], params["source_blob"])
        dest_blob = client.get_blob_client(params["dest_container"], params["dest_blob"])

        dest_blob.start_copy_from_url(source_blob.url)
        return ConnectorResult(success=True, data={"copied": params["dest_blob"]})

    async def _get_sas_url(self, client, params: dict) -> ConnectorResult:
        from azure.storage.blob import generate_blob_sas, BlobSasPermissions
        from datetime import datetime, timedelta

        permissions = BlobSasPermissions(
            read="r" in params.get("permission", "r"),
            write="w" in params.get("permission", ""),
            delete="d" in params.get("permission", ""),
        )

        expiry = datetime.utcnow() + timedelta(hours=params.get("expires_in", 1))

        sas_token = generate_blob_sas(
            account_name=self.account_name,
            container_name=params["container"],
            blob_name=params["blob_name"],
            account_key=self.account_key,
            permission=permissions,
            expiry=expiry,
        )

        url = f"https://{self.account_name}.blob.core.windows.net/{params['container']}/{params['blob_name']}?{sas_token}"
        return ConnectorResult(success=True, data={"url": url})

    async def _list_containers(self, client) -> ConnectorResult:
        containers = client.list_containers()
        container_list = [
            {"name": c.name, "last_modified": c.last_modified.isoformat() if c.last_modified else None}
            for c in containers
        ]
        return ConnectorResult(success=True, data={"containers": container_list})

    async def _create_container(self, client, container: str) -> ConnectorResult:
        client.create_container(container)
        return ConnectorResult(success=True, data={"created": container})

    async def _delete_container(self, client, container: str) -> ConnectorResult:
        client.delete_container(container)
        return ConnectorResult(success=True, data={"deleted": container})

    async def _get_blob_properties(self, client, container: str, blob_name: str) -> ConnectorResult:
        blob_client = client.get_blob_client(container, blob_name)
        properties = blob_client.get_blob_properties()

        return ConnectorResult(
            success=True,
            data={
                "name": properties.name,
                "size": properties.size,
                "content_type": properties.content_settings.content_type if properties.content_settings else None,
                "last_modified": properties.last_modified.isoformat() if properties.last_modified else None,
                "etag": properties.etag,
            }
        )

    async def close(self):
        self._service_client = None
