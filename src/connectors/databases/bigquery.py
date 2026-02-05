"""
BigQuery Connector

Connect to Google BigQuery for analytics and data warehouse operations.
"""

from typing import Any
from ..base import BaseConnector, ConnectorResult


class BigQueryConnector(BaseConnector):
    """Connector for Google BigQuery."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.project_id = credentials.get("project_id")
        self.credentials_json = credentials.get("credentials_json")
        self.location = credentials.get("location", "US")
        self._client = None

    async def _get_client(self):
        """Get BigQuery client."""
        if self._client is None:
            from google.cloud import bigquery
            from google.oauth2 import service_account
            import json

            if isinstance(self.credentials_json, str):
                creds_dict = json.loads(self.credentials_json)
            else:
                creds_dict = self.credentials_json

            credentials = service_account.Credentials.from_service_account_info(creds_dict)
            self._client = bigquery.Client(
                project=self.project_id,
                credentials=credentials,
                location=self.location,
            )
        return self._client

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "query": {
                "description": "Execute a SQL query",
                "parameters": {
                    "sql": {"type": "string", "description": "SQL query", "required": True},
                    "params": {"type": "array", "description": "Query parameters", "required": False},
                },
            },
            "insert_rows": {
                "description": "Insert rows into a table",
                "parameters": {
                    "dataset": {"type": "string", "description": "Dataset name", "required": True},
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "rows": {"type": "array", "description": "Rows to insert", "required": True},
                },
            },
            "create_table": {
                "description": "Create a new table",
                "parameters": {
                    "dataset": {"type": "string", "description": "Dataset name", "required": True},
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "schema": {"type": "array", "description": "Table schema", "required": True},
                },
            },
            "load_from_gcs": {
                "description": "Load data from Google Cloud Storage",
                "parameters": {
                    "dataset": {"type": "string", "description": "Dataset name", "required": True},
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "gcs_uri": {"type": "string", "description": "GCS URI (gs://...)", "required": True},
                    "source_format": {"type": "string", "description": "CSV, JSON, PARQUET, etc.", "required": False},
                },
            },
            "export_to_gcs": {
                "description": "Export table to Google Cloud Storage",
                "parameters": {
                    "dataset": {"type": "string", "description": "Dataset name", "required": True},
                    "table": {"type": "string", "description": "Table name", "required": True},
                    "gcs_uri": {"type": "string", "description": "GCS URI destination", "required": True},
                    "destination_format": {"type": "string", "description": "Export format", "required": False},
                },
            },
            "list_datasets": {
                "description": "List all datasets in the project",
                "parameters": {},
            },
            "list_tables": {
                "description": "List tables in a dataset",
                "parameters": {
                    "dataset": {"type": "string", "description": "Dataset name", "required": True},
                },
            },
            "get_table_schema": {
                "description": "Get table schema",
                "parameters": {
                    "dataset": {"type": "string", "description": "Dataset name", "required": True},
                    "table": {"type": "string", "description": "Table name", "required": True},
                },
            },
            "delete_table": {
                "description": "Delete a table",
                "parameters": {
                    "dataset": {"type": "string", "description": "Dataset name", "required": True},
                    "table": {"type": "string", "description": "Table name", "required": True},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            client = await self._get_client()

            if action == "query":
                return await self._query(client, params["sql"], params.get("params", []))
            elif action == "insert_rows":
                return await self._insert_rows(client, params["dataset"], params["table"], params["rows"])
            elif action == "create_table":
                return await self._create_table(client, params["dataset"], params["table"], params["schema"])
            elif action == "load_from_gcs":
                return await self._load_from_gcs(
                    client, params["dataset"], params["table"],
                    params["gcs_uri"], params.get("source_format", "CSV")
                )
            elif action == "export_to_gcs":
                return await self._export_to_gcs(
                    client, params["dataset"], params["table"],
                    params["gcs_uri"], params.get("destination_format", "CSV")
                )
            elif action == "list_datasets":
                return await self._list_datasets(client)
            elif action == "list_tables":
                return await self._list_tables(client, params["dataset"])
            elif action == "get_table_schema":
                return await self._get_table_schema(client, params["dataset"], params["table"])
            elif action == "delete_table":
                return await self._delete_table(client, params["dataset"], params["table"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _query(self, client, sql: str, params: list) -> ConnectorResult:
        from google.cloud import bigquery

        job_config = bigquery.QueryJobConfig()
        if params:
            job_config.query_parameters = [
                bigquery.ScalarQueryParameter(None, "STRING", p) for p in params
            ]

        query_job = client.query(sql, job_config=job_config)
        results = query_job.result()

        rows = [dict(row) for row in results]
        return ConnectorResult(
            success=True,
            data={
                "rows": rows,
                "count": len(rows),
                "total_bytes_processed": query_job.total_bytes_processed,
            }
        )

    async def _insert_rows(self, client, dataset: str, table: str, rows: list) -> ConnectorResult:
        table_ref = client.dataset(dataset).table(table)
        errors = client.insert_rows_json(table_ref, rows)

        if errors:
            return ConnectorResult(success=False, error=str(errors))
        return ConnectorResult(success=True, data={"inserted": len(rows)})

    async def _create_table(self, client, dataset: str, table: str, schema: list) -> ConnectorResult:
        from google.cloud import bigquery

        bq_schema = [
            bigquery.SchemaField(
                name=field["name"],
                field_type=field.get("type", "STRING"),
                mode=field.get("mode", "NULLABLE"),
                description=field.get("description", ""),
            )
            for field in schema
        ]

        table_ref = client.dataset(dataset).table(table)
        table_obj = bigquery.Table(table_ref, schema=bq_schema)
        client.create_table(table_obj)

        return ConnectorResult(success=True, data={"table": f"{dataset}.{table}"})

    async def _load_from_gcs(self, client, dataset: str, table: str, gcs_uri: str, source_format: str) -> ConnectorResult:
        from google.cloud import bigquery

        table_ref = client.dataset(dataset).table(table)

        format_map = {
            "CSV": bigquery.SourceFormat.CSV,
            "JSON": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            "PARQUET": bigquery.SourceFormat.PARQUET,
            "AVRO": bigquery.SourceFormat.AVRO,
        }

        job_config = bigquery.LoadJobConfig(
            source_format=format_map.get(source_format.upper(), bigquery.SourceFormat.CSV),
            autodetect=True,
        )

        load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        load_job.result()

        return ConnectorResult(
            success=True,
            data={"rows_loaded": load_job.output_rows, "table": f"{dataset}.{table}"}
        )

    async def _export_to_gcs(self, client, dataset: str, table: str, gcs_uri: str, destination_format: str) -> ConnectorResult:
        from google.cloud import bigquery

        table_ref = client.dataset(dataset).table(table)

        format_map = {
            "CSV": bigquery.DestinationFormat.CSV,
            "JSON": bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON,
            "AVRO": bigquery.DestinationFormat.AVRO,
        }

        job_config = bigquery.ExtractJobConfig(
            destination_format=format_map.get(destination_format.upper(), bigquery.DestinationFormat.CSV)
        )

        extract_job = client.extract_table(table_ref, gcs_uri, job_config=job_config)
        extract_job.result()

        return ConnectorResult(success=True, data={"destination": gcs_uri})

    async def _list_datasets(self, client) -> ConnectorResult:
        datasets = list(client.list_datasets())
        return ConnectorResult(
            success=True,
            data={"datasets": [{"id": d.dataset_id, "project": d.project} for d in datasets]}
        )

    async def _list_tables(self, client, dataset: str) -> ConnectorResult:
        tables = list(client.list_tables(dataset))
        return ConnectorResult(
            success=True,
            data={"tables": [{"id": t.table_id, "type": t.table_type} for t in tables]}
        )

    async def _get_table_schema(self, client, dataset: str, table: str) -> ConnectorResult:
        table_ref = client.dataset(dataset).table(table)
        table_obj = client.get_table(table_ref)

        schema = [
            {
                "name": field.name,
                "type": field.field_type,
                "mode": field.mode,
                "description": field.description,
            }
            for field in table_obj.schema
        ]

        return ConnectorResult(
            success=True,
            data={
                "schema": schema,
                "num_rows": table_obj.num_rows,
                "num_bytes": table_obj.num_bytes,
            }
        )

    async def _delete_table(self, client, dataset: str, table: str) -> ConnectorResult:
        table_ref = client.dataset(dataset).table(table)
        client.delete_table(table_ref)
        return ConnectorResult(success=True, data={"deleted": f"{dataset}.{table}"})

    async def close(self):
        if self._client:
            self._client.close()
            self._client = None
