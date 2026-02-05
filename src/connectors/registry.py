"""
Connector Registry

Central registry for all connectors. Provides discovery and instantiation.
"""

from typing import Any, Type
from .base import BaseConnector, ConnectorResult

# Original connectors
from .slack import SlackConnector
from .discord import DiscordConnector
from .github import GitHubConnector
from .notion import NotionConnector
from .airtable import AirtableConnector
from .google_sheets import GoogleSheetsConnector
from .email import EmailConnector
from .http import HTTPConnector
from .webhook import WebhookConnector
from .auth_http import AuthenticatedHTTPConnector

# Database connectors
from .databases.postgresql import PostgreSQLConnector
from .databases.mysql import MySQLConnector
from .databases.mongodb import MongoDBConnector
from .databases.azure_sql import AzureSQLConnector
from .databases.snowflake import SnowflakeConnector
from .databases.bigquery import BigQueryConnector
from .databases.redshift import RedshiftConnector
from .databases.dynamodb import DynamoDBConnector
from .databases.supabase import SupabaseConnector
from .databases.planetscale import PlanetScaleConnector
from .databases.cockroachdb import CockroachDBConnector
from .databases.elasticsearch import ElasticsearchConnector
from .databases.redis import RedisConnector
from .databases.firebase import FirebaseConnector
from .databases.sqlite import SQLiteConnector
from .databases.oracle import OracleConnector
from .databases.sqlserver import SQLServerConnector
from .databases.mariadb import MariaDBConnector
from .databases.cassandra import CassandraConnector
from .databases.clickhouse import ClickHouseConnector

# Cloud storage connectors
from .cloud.aws_s3 import AWSS3Connector
from .cloud.azure_blob import AzureBlobConnector
from .cloud.gcs import GCSConnector
from .cloud.dropbox import DropboxConnector
from .cloud.box import BoxConnector
from .cloud.onedrive import OneDriveConnector

# CRM connectors
from .crm.salesforce import SalesforceConnector
from .crm.hubspot import HubSpotConnector
from .crm.zoho import ZohoCRMConnector as ZohoConnector
from .crm.pipedrive import PipedriveConnector
from .crm.freshsales import FreshsalesConnector

# Payment connectors
from .payments.stripe import StripeConnector
from .payments.paypal import PayPalConnector
from .payments.square import SquareConnector

# Productivity connectors
from .productivity.jira import JiraConnector
from .productivity.asana import AsanaConnector
from .productivity.monday import MondayConnector
from .productivity.trello import TrelloConnector
from .productivity.linear import LinearConnector
from .productivity.clickup import ClickUpConnector


class ConnectorRegistry:
    """Registry of all available connectors."""

    _connectors: dict[str, Type[BaseConnector]] = {
        # Communication
        "slack": SlackConnector,
        "discord": DiscordConnector,
        "email": EmailConnector,

        # Development
        "github": GitHubConnector,

        # Productivity (Original)
        "notion": NotionConnector,
        "airtable": AirtableConnector,
        "google_sheets": GoogleSheetsConnector,

        # Utility
        "http": HTTPConnector,
        "auth_http": AuthenticatedHTTPConnector,
        "webhook": WebhookConnector,

        # Databases (20)
        "postgresql": PostgreSQLConnector,
        "mysql": MySQLConnector,
        "mongodb": MongoDBConnector,
        "azure_sql": AzureSQLConnector,
        "snowflake": SnowflakeConnector,
        "bigquery": BigQueryConnector,
        "redshift": RedshiftConnector,
        "dynamodb": DynamoDBConnector,
        "supabase": SupabaseConnector,
        "planetscale": PlanetScaleConnector,
        "cockroachdb": CockroachDBConnector,
        "elasticsearch": ElasticsearchConnector,
        "redis": RedisConnector,
        "firebase": FirebaseConnector,
        "sqlite": SQLiteConnector,
        "oracle": OracleConnector,
        "sqlserver": SQLServerConnector,
        "mariadb": MariaDBConnector,
        "cassandra": CassandraConnector,
        "clickhouse": ClickHouseConnector,

        # Cloud Storage (6)
        "aws_s3": AWSS3Connector,
        "azure_blob": AzureBlobConnector,
        "gcs": GCSConnector,
        "dropbox": DropboxConnector,
        "box": BoxConnector,
        "onedrive": OneDriveConnector,

        # CRM (5)
        "salesforce": SalesforceConnector,
        "hubspot": HubSpotConnector,
        "zoho": ZohoConnector,
        "pipedrive": PipedriveConnector,
        "freshsales": FreshsalesConnector,

        # Payments (3)
        "stripe": StripeConnector,
        "paypal": PayPalConnector,
        "square": SquareConnector,

        # Productivity (6)
        "jira": JiraConnector,
        "asana": AsanaConnector,
        "monday": MondayConnector,
        "trello": TrelloConnector,
        "linear": LinearConnector,
        "clickup": ClickUpConnector,
    }

    @classmethod
    def list_connectors(cls) -> list[dict[str, Any]]:
        """List all available connectors with their metadata."""
        connectors = []
        for name, connector_class in cls._connectors.items():
            instance = connector_class({})
            connectors.append({
                "service": name,
                "display_name": instance.display_name,
                "actions": instance.get_actions(),
            })
        return connectors

    @classmethod
    def get_connector_class(cls, service: str) -> Type[BaseConnector] | None:
        """Get a connector class by service name."""
        return cls._connectors.get(service.lower())

    @classmethod
    def get_connector(cls, service: str, credentials: dict[str, Any]) -> BaseConnector | None:
        """Get an instantiated connector with credentials."""
        connector_class = cls.get_connector_class(service)
        if connector_class:
            return connector_class(credentials)
        return None

    @classmethod
    def register_connector(cls, service: str, connector_class: Type[BaseConnector]):
        """Register a new connector."""
        cls._connectors[service.lower()] = connector_class

    @classmethod
    def get_actions(cls, service: str) -> list[dict[str, str]]:
        """Get available actions for a service."""
        connector_class = cls.get_connector_class(service)
        if connector_class:
            instance = connector_class({})
            return instance.get_actions()
        return []

    @classmethod
    def service_exists(cls, service: str) -> bool:
        """Check if a service connector exists."""
        return service.lower() in cls._connectors

    @classmethod
    def list_by_category(cls) -> dict[str, list[str]]:
        """List all connectors grouped by category."""
        categories = {}
        for service_id, info in SERVICE_INFO.items():
            category = info.get("category", "other")
            if category not in categories:
                categories[category] = []
            categories[category].append(service_id)
        return categories

    @classmethod
    def count_connectors(cls) -> int:
        """Get total number of connectors."""
        return len(cls._connectors)


async def execute_connector(
    service: str,
    action: str,
    inputs: dict[str, Any],
    credentials: dict[str, Any],
) -> ConnectorResult:
    """Execute a connector action. Convenience function."""
    connector = ConnectorRegistry.get_connector(service, credentials)
    if not connector:
        return ConnectorResult(
            success=False,
            error=f"Unknown service: {service}",
        )

    return await connector.execute(action, inputs)


def get_connector(service: str, credentials: dict[str, Any]) -> BaseConnector | None:
    """Get an instantiated connector. Convenience function."""
    return ConnectorRegistry.get_connector(service, credentials)


# Service metadata for UI
SERVICE_INFO = {
    # ==================== COMMUNICATION ====================
    "slack": {
        "name": "Slack",
        "icon": "slack",
        "description": "Send messages, manage channels, and interact with Slack workspaces",
        "auth_fields": [
            {"name": "access_token", "label": "Bot Token", "type": "password", "required": True},
        ],
        "category": "communication",
    },
    "discord": {
        "name": "Discord",
        "icon": "discord",
        "description": "Send messages, manage channels, and interact with Discord servers",
        "auth_fields": [
            {"name": "bot_token", "label": "Bot Token", "type": "password", "required": True},
        ],
        "category": "communication",
    },
    "email": {
        "name": "Email",
        "icon": "mail",
        "description": "Send emails via SMTP or email service APIs",
        "auth_fields": [
            {"name": "smtp_host", "label": "SMTP Host", "type": "text", "required": False},
            {"name": "smtp_port", "label": "SMTP Port", "type": "text", "required": False},
            {"name": "smtp_user", "label": "SMTP Username", "type": "text", "required": False},
            {"name": "smtp_pass", "label": "SMTP Password", "type": "password", "required": False},
            {"name": "sendgrid_key", "label": "SendGrid API Key (alternative)", "type": "password", "required": False},
        ],
        "category": "communication",
    },

    # ==================== DEVELOPMENT ====================
    "github": {
        "name": "GitHub",
        "icon": "github",
        "description": "Manage repositories, issues, pull requests, and more",
        "auth_fields": [
            {"name": "access_token", "label": "Personal Access Token", "type": "password", "required": True},
        ],
        "category": "development",
    },

    # ==================== PRODUCTIVITY (ORIGINAL) ====================
    "notion": {
        "name": "Notion",
        "icon": "notion",
        "description": "Create pages, manage databases, and organize your workspace",
        "auth_fields": [
            {"name": "api_key", "label": "Integration Token", "type": "password", "required": True},
        ],
        "category": "productivity",
    },
    "airtable": {
        "name": "Airtable",
        "icon": "airtable",
        "description": "Manage bases, tables, and records in Airtable",
        "auth_fields": [
            {"name": "api_key", "label": "API Key", "type": "password", "required": True},
        ],
        "category": "productivity",
    },
    "google_sheets": {
        "name": "Google Sheets",
        "icon": "google-sheets",
        "description": "Read and write data to Google Sheets",
        "auth_fields": [
            {"name": "access_token", "label": "OAuth Access Token", "type": "password", "required": True},
        ],
        "category": "productivity",
    },

    # ==================== UTILITY ====================
    "http": {
        "name": "HTTP Request",
        "icon": "globe",
        "description": "Make HTTP requests to any API endpoint",
        "auth_fields": [
            {"name": "auth_type", "label": "Auth Type", "type": "select", "options": ["none", "bearer", "api_key", "basic"], "required": False},
            {"name": "token", "label": "Bearer Token", "type": "password", "required": False},
            {"name": "api_key", "label": "API Key", "type": "password", "required": False},
            {"name": "username", "label": "Username (Basic Auth)", "type": "text", "required": False},
            {"name": "password", "label": "Password (Basic Auth)", "type": "password", "required": False},
        ],
        "category": "utility",
    },
    "webhook": {
        "name": "Webhook",
        "icon": "webhook",
        "description": "Send webhooks to any URL",
        "auth_fields": [],
        "category": "utility",
    },
    "auth_http": {
        "name": "Authenticated HTTP",
        "icon": "lock",
        "description": "Make HTTP requests with automatic token-based authentication (OAuth2, custom login, API key exchange)",
        "auth_fields": [
            {"name": "auth_type", "label": "Auth Type", "type": "select", "options": ["oauth2_client", "oauth2_password", "login", "api_key_exchange", "custom"], "required": True},
            {"name": "token_url", "label": "Token URL (OAuth2)", "type": "text", "required": False},
            {"name": "auth_url", "label": "Auth URL (Custom)", "type": "text", "required": False},
            {"name": "client_id", "label": "Client ID", "type": "text", "required": False},
            {"name": "client_secret", "label": "Client Secret", "type": "password", "required": False},
            {"name": "username", "label": "Username", "type": "text", "required": False},
            {"name": "password", "label": "Password", "type": "password", "required": False},
            {"name": "api_key", "label": "API Key", "type": "password", "required": False},
            {"name": "scope", "label": "Scope (OAuth2)", "type": "text", "required": False},
            {"name": "token_path", "label": "Token Path in Response", "type": "text", "required": False, "default": "access_token"},
            {"name": "base_url", "label": "Base URL for API calls", "type": "text", "required": False},
            {"name": "inject_prefix", "label": "Token Prefix", "type": "text", "required": False, "default": "Bearer "},
        ],
        "category": "utility",
    },

    # ==================== DATABASES ====================
    "postgresql": {
        "name": "PostgreSQL",
        "icon": "postgresql",
        "description": "Connect to PostgreSQL databases for data operations",
        "auth_fields": [
            {"name": "host", "label": "Host", "type": "text", "required": True},
            {"name": "port", "label": "Port", "type": "text", "required": False, "default": "5432"},
            {"name": "database", "label": "Database", "type": "text", "required": True},
            {"name": "user", "label": "Username", "type": "text", "required": True},
            {"name": "password", "label": "Password", "type": "password", "required": True},
        ],
        "category": "database",
    },
    "mysql": {
        "name": "MySQL",
        "icon": "mysql",
        "description": "Connect to MySQL databases for data operations",
        "auth_fields": [
            {"name": "host", "label": "Host", "type": "text", "required": True},
            {"name": "port", "label": "Port", "type": "text", "required": False, "default": "3306"},
            {"name": "database", "label": "Database", "type": "text", "required": True},
            {"name": "user", "label": "Username", "type": "text", "required": True},
            {"name": "password", "label": "Password", "type": "password", "required": True},
        ],
        "category": "database",
    },
    "mongodb": {
        "name": "MongoDB",
        "icon": "mongodb",
        "description": "Connect to MongoDB for document database operations",
        "auth_fields": [
            {"name": "connection_string", "label": "Connection String", "type": "password", "required": True},
            {"name": "database", "label": "Database", "type": "text", "required": True},
        ],
        "category": "database",
    },
    "azure_sql": {
        "name": "Azure SQL Database",
        "icon": "azure",
        "description": "Connect to Azure SQL Database for enterprise data operations",
        "auth_fields": [
            {"name": "server", "label": "Server", "type": "text", "required": True},
            {"name": "database", "label": "Database", "type": "text", "required": True},
            {"name": "username", "label": "Username", "type": "text", "required": True},
            {"name": "password", "label": "Password", "type": "password", "required": True},
        ],
        "category": "database",
    },
    "snowflake": {
        "name": "Snowflake",
        "icon": "snowflake",
        "description": "Connect to Snowflake data warehouse for analytics",
        "auth_fields": [
            {"name": "account", "label": "Account Identifier", "type": "text", "required": True},
            {"name": "user", "label": "Username", "type": "text", "required": True},
            {"name": "password", "label": "Password", "type": "password", "required": True},
            {"name": "warehouse", "label": "Warehouse", "type": "text", "required": True},
            {"name": "database", "label": "Database", "type": "text", "required": True},
            {"name": "schema", "label": "Schema", "type": "text", "required": False, "default": "PUBLIC"},
        ],
        "category": "database",
    },
    "bigquery": {
        "name": "Google BigQuery",
        "icon": "google-cloud",
        "description": "Connect to BigQuery for analytics and data warehousing",
        "auth_fields": [
            {"name": "project_id", "label": "Project ID", "type": "text", "required": True},
            {"name": "credentials_json", "label": "Service Account JSON", "type": "textarea", "required": True},
        ],
        "category": "database",
    },
    "redshift": {
        "name": "Amazon Redshift",
        "icon": "aws",
        "description": "Connect to Amazon Redshift data warehouse",
        "auth_fields": [
            {"name": "host", "label": "Host", "type": "text", "required": True},
            {"name": "port", "label": "Port", "type": "text", "required": False, "default": "5439"},
            {"name": "database", "label": "Database", "type": "text", "required": True},
            {"name": "user", "label": "Username", "type": "text", "required": True},
            {"name": "password", "label": "Password", "type": "password", "required": True},
        ],
        "category": "database",
    },
    "dynamodb": {
        "name": "Amazon DynamoDB",
        "icon": "aws",
        "description": "Connect to DynamoDB for NoSQL data operations",
        "auth_fields": [
            {"name": "aws_access_key_id", "label": "AWS Access Key ID", "type": "text", "required": True},
            {"name": "aws_secret_access_key", "label": "AWS Secret Access Key", "type": "password", "required": True},
            {"name": "region", "label": "Region", "type": "text", "required": True},
        ],
        "category": "database",
    },
    "supabase": {
        "name": "Supabase",
        "icon": "supabase",
        "description": "Connect to Supabase for PostgreSQL and realtime data",
        "auth_fields": [
            {"name": "url", "label": "Project URL", "type": "text", "required": True},
            {"name": "api_key", "label": "API Key (anon or service)", "type": "password", "required": True},
        ],
        "category": "database",
    },
    "planetscale": {
        "name": "PlanetScale",
        "icon": "planetscale",
        "description": "Connect to PlanetScale serverless MySQL",
        "auth_fields": [
            {"name": "host", "label": "Host", "type": "text", "required": True},
            {"name": "username", "label": "Username", "type": "text", "required": True},
            {"name": "password", "label": "Password", "type": "password", "required": True},
            {"name": "database", "label": "Database", "type": "text", "required": True},
        ],
        "category": "database",
    },
    "cockroachdb": {
        "name": "CockroachDB",
        "icon": "cockroachdb",
        "description": "Connect to CockroachDB distributed SQL database",
        "auth_fields": [
            {"name": "connection_string", "label": "Connection String", "type": "password", "required": True},
        ],
        "category": "database",
    },
    "elasticsearch": {
        "name": "Elasticsearch",
        "icon": "elasticsearch",
        "description": "Connect to Elasticsearch for search and analytics",
        "auth_fields": [
            {"name": "hosts", "label": "Hosts (comma-separated)", "type": "text", "required": True},
            {"name": "api_key", "label": "API Key", "type": "password", "required": False},
            {"name": "username", "label": "Username", "type": "text", "required": False},
            {"name": "password", "label": "Password", "type": "password", "required": False},
        ],
        "category": "database",
    },
    "redis": {
        "name": "Redis",
        "icon": "redis",
        "description": "Connect to Redis for caching and data structures",
        "auth_fields": [
            {"name": "host", "label": "Host", "type": "text", "required": True},
            {"name": "port", "label": "Port", "type": "text", "required": False, "default": "6379"},
            {"name": "password", "label": "Password", "type": "password", "required": False},
            {"name": "db", "label": "Database Number", "type": "text", "required": False, "default": "0"},
        ],
        "category": "database",
    },
    "firebase": {
        "name": "Firebase/Firestore",
        "icon": "firebase",
        "description": "Connect to Firebase Firestore for document database",
        "auth_fields": [
            {"name": "project_id", "label": "Project ID", "type": "text", "required": True},
            {"name": "credentials_json", "label": "Service Account JSON", "type": "textarea", "required": True},
        ],
        "category": "database",
    },
    "sqlite": {
        "name": "SQLite",
        "icon": "sqlite",
        "description": "Connect to SQLite databases for local data storage",
        "auth_fields": [
            {"name": "database_path", "label": "Database Path", "type": "text", "required": True},
        ],
        "category": "database",
    },
    "oracle": {
        "name": "Oracle Database",
        "icon": "oracle",
        "description": "Connect to Oracle Database for enterprise data",
        "auth_fields": [
            {"name": "host", "label": "Host", "type": "text", "required": True},
            {"name": "port", "label": "Port", "type": "text", "required": False, "default": "1521"},
            {"name": "service_name", "label": "Service Name", "type": "text", "required": True},
            {"name": "user", "label": "Username", "type": "text", "required": True},
            {"name": "password", "label": "Password", "type": "password", "required": True},
        ],
        "category": "database",
    },
    "sqlserver": {
        "name": "SQL Server",
        "icon": "microsoft",
        "description": "Connect to Microsoft SQL Server",
        "auth_fields": [
            {"name": "server", "label": "Server", "type": "text", "required": True},
            {"name": "database", "label": "Database", "type": "text", "required": True},
            {"name": "username", "label": "Username", "type": "text", "required": True},
            {"name": "password", "label": "Password", "type": "password", "required": True},
        ],
        "category": "database",
    },
    "mariadb": {
        "name": "MariaDB",
        "icon": "mariadb",
        "description": "Connect to MariaDB for MySQL-compatible operations",
        "auth_fields": [
            {"name": "host", "label": "Host", "type": "text", "required": True},
            {"name": "port", "label": "Port", "type": "text", "required": False, "default": "3306"},
            {"name": "database", "label": "Database", "type": "text", "required": True},
            {"name": "user", "label": "Username", "type": "text", "required": True},
            {"name": "password", "label": "Password", "type": "password", "required": True},
        ],
        "category": "database",
    },
    "cassandra": {
        "name": "Apache Cassandra",
        "icon": "cassandra",
        "description": "Connect to Cassandra/DataStax Astra for distributed data",
        "auth_fields": [
            {"name": "hosts", "label": "Contact Points (comma-separated)", "type": "text", "required": False},
            {"name": "keyspace", "label": "Keyspace", "type": "text", "required": True},
            {"name": "username", "label": "Username", "type": "text", "required": False},
            {"name": "password", "label": "Password", "type": "password", "required": False},
            {"name": "secure_connect_bundle", "label": "Secure Connect Bundle Path (Astra)", "type": "text", "required": False},
            {"name": "client_id", "label": "Client ID (Astra)", "type": "text", "required": False},
            {"name": "client_secret", "label": "Client Secret (Astra)", "type": "password", "required": False},
        ],
        "category": "database",
    },
    "clickhouse": {
        "name": "ClickHouse",
        "icon": "clickhouse",
        "description": "Connect to ClickHouse for analytics database",
        "auth_fields": [
            {"name": "host", "label": "Host", "type": "text", "required": True},
            {"name": "port", "label": "Port", "type": "text", "required": False, "default": "8443"},
            {"name": "database", "label": "Database", "type": "text", "required": True},
            {"name": "username", "label": "Username", "type": "text", "required": True},
            {"name": "password", "label": "Password", "type": "password", "required": True},
        ],
        "category": "database",
    },

    # ==================== CLOUD STORAGE ====================
    "aws_s3": {
        "name": "Amazon S3",
        "icon": "aws",
        "description": "Store and retrieve files from Amazon S3",
        "auth_fields": [
            {"name": "aws_access_key_id", "label": "AWS Access Key ID", "type": "text", "required": True},
            {"name": "aws_secret_access_key", "label": "AWS Secret Access Key", "type": "password", "required": True},
            {"name": "region", "label": "Region", "type": "text", "required": True},
        ],
        "category": "cloud_storage",
    },
    "azure_blob": {
        "name": "Azure Blob Storage",
        "icon": "azure",
        "description": "Store and retrieve files from Azure Blob Storage",
        "auth_fields": [
            {"name": "connection_string", "label": "Connection String", "type": "password", "required": True},
        ],
        "category": "cloud_storage",
    },
    "gcs": {
        "name": "Google Cloud Storage",
        "icon": "google-cloud",
        "description": "Store and retrieve files from Google Cloud Storage",
        "auth_fields": [
            {"name": "project_id", "label": "Project ID", "type": "text", "required": True},
            {"name": "credentials_json", "label": "Service Account JSON", "type": "textarea", "required": True},
        ],
        "category": "cloud_storage",
    },
    "dropbox": {
        "name": "Dropbox",
        "icon": "dropbox",
        "description": "Store and share files with Dropbox",
        "auth_fields": [
            {"name": "access_token", "label": "Access Token", "type": "password", "required": True},
        ],
        "category": "cloud_storage",
    },
    "box": {
        "name": "Box",
        "icon": "box",
        "description": "Secure file sharing and storage with Box",
        "auth_fields": [
            {"name": "access_token", "label": "Access Token", "type": "password", "required": True},
        ],
        "category": "cloud_storage",
    },
    "onedrive": {
        "name": "OneDrive",
        "icon": "microsoft",
        "description": "Store and share files with Microsoft OneDrive",
        "auth_fields": [
            {"name": "access_token", "label": "Access Token", "type": "password", "required": True},
        ],
        "category": "cloud_storage",
    },

    # ==================== CRM ====================
    "salesforce": {
        "name": "Salesforce",
        "icon": "salesforce",
        "description": "Manage leads, contacts, accounts, and opportunities",
        "auth_fields": [
            {"name": "instance_url", "label": "Instance URL", "type": "text", "required": True},
            {"name": "access_token", "label": "Access Token", "type": "password", "required": True},
        ],
        "category": "crm",
    },
    "hubspot": {
        "name": "HubSpot",
        "icon": "hubspot",
        "description": "Manage contacts, companies, deals, and marketing",
        "auth_fields": [
            {"name": "access_token", "label": "Access Token", "type": "password", "required": True},
        ],
        "category": "crm",
    },
    "zoho": {
        "name": "Zoho CRM",
        "icon": "zoho",
        "description": "Manage leads, contacts, and sales pipeline",
        "auth_fields": [
            {"name": "access_token", "label": "Access Token", "type": "password", "required": True},
            {"name": "api_domain", "label": "API Domain", "type": "text", "required": False, "default": "https://www.zohoapis.com"},
        ],
        "category": "crm",
    },
    "pipedrive": {
        "name": "Pipedrive",
        "icon": "pipedrive",
        "description": "Manage deals, contacts, and sales activities",
        "auth_fields": [
            {"name": "api_token", "label": "API Token", "type": "password", "required": True},
        ],
        "category": "crm",
    },
    "freshsales": {
        "name": "Freshsales",
        "icon": "freshworks",
        "description": "Manage leads, contacts, accounts, and deals",
        "auth_fields": [
            {"name": "domain", "label": "Domain (e.g., yourcompany.freshsales.io)", "type": "text", "required": True},
            {"name": "api_key", "label": "API Key", "type": "password", "required": True},
        ],
        "category": "crm",
    },

    # ==================== PAYMENTS ====================
    "stripe": {
        "name": "Stripe",
        "icon": "stripe",
        "description": "Process payments, manage subscriptions and invoices",
        "auth_fields": [
            {"name": "api_key", "label": "Secret Key", "type": "password", "required": True},
        ],
        "category": "payments",
    },
    "paypal": {
        "name": "PayPal",
        "icon": "paypal",
        "description": "Process payments, payouts, and invoices",
        "auth_fields": [
            {"name": "client_id", "label": "Client ID", "type": "text", "required": True},
            {"name": "client_secret", "label": "Client Secret", "type": "password", "required": True},
            {"name": "mode", "label": "Mode", "type": "select", "options": ["sandbox", "live"], "required": True},
        ],
        "category": "payments",
    },
    "square": {
        "name": "Square",
        "icon": "square",
        "description": "Process payments, manage customers and catalog",
        "auth_fields": [
            {"name": "access_token", "label": "Access Token", "type": "password", "required": True},
            {"name": "environment", "label": "Environment", "type": "select", "options": ["sandbox", "production"], "required": True},
        ],
        "category": "payments",
    },

    # ==================== PRODUCTIVITY (PROJECT MANAGEMENT) ====================
    "jira": {
        "name": "Jira",
        "icon": "jira",
        "description": "Manage issues, projects, and agile workflows",
        "auth_fields": [
            {"name": "domain", "label": "Domain (e.g., yourcompany.atlassian.net)", "type": "text", "required": True},
            {"name": "email", "label": "Email", "type": "text", "required": True},
            {"name": "api_token", "label": "API Token", "type": "password", "required": True},
        ],
        "category": "productivity",
    },
    "asana": {
        "name": "Asana",
        "icon": "asana",
        "description": "Manage tasks, projects, and team workflows",
        "auth_fields": [
            {"name": "access_token", "label": "Access Token", "type": "password", "required": True},
        ],
        "category": "productivity",
    },
    "monday": {
        "name": "Monday.com",
        "icon": "monday",
        "description": "Manage boards, items, and work management",
        "auth_fields": [
            {"name": "api_token", "label": "API Token", "type": "password", "required": True},
        ],
        "category": "productivity",
    },
    "trello": {
        "name": "Trello",
        "icon": "trello",
        "description": "Manage boards, lists, and cards",
        "auth_fields": [
            {"name": "api_key", "label": "API Key", "type": "text", "required": True},
            {"name": "token", "label": "Token", "type": "password", "required": True},
        ],
        "category": "productivity",
    },
    "linear": {
        "name": "Linear",
        "icon": "linear",
        "description": "Manage issues, projects, and engineering workflows",
        "auth_fields": [
            {"name": "api_key", "label": "API Key", "type": "password", "required": True},
        ],
        "category": "productivity",
    },
    "clickup": {
        "name": "ClickUp",
        "icon": "clickup",
        "description": "Manage tasks, spaces, and productivity workflows",
        "auth_fields": [
            {"name": "api_token", "label": "API Token", "type": "password", "required": True},
        ],
        "category": "productivity",
    },
}


def get_service_info(service: str) -> dict[str, Any] | None:
    """Get service metadata for UI display."""
    return SERVICE_INFO.get(service.lower())


def list_services_by_category() -> dict[str, list[dict[str, Any]]]:
    """List all services grouped by category."""
    categories: dict[str, list[dict[str, Any]]] = {}

    for service_id, info in SERVICE_INFO.items():
        category = info.get("category", "other")
        if category not in categories:
            categories[category] = []

        categories[category].append({
            "id": service_id,
            **info,
        })

    return categories


def search_services(query: str) -> list[dict[str, Any]]:
    """Search services by name or description."""
    query = query.lower()
    results = []

    for service_id, info in SERVICE_INFO.items():
        if (query in service_id.lower() or
            query in info["name"].lower() or
            query in info["description"].lower()):
            results.append({
                "id": service_id,
                **info,
            })

    return results
