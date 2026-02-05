"""
Service Connectors

Actual implementations for connecting to external services.
Each connector handles authentication, API calls, and data transformation.

Available connector categories:
- Communication: Slack, Discord, Email
- Development: GitHub
- Productivity: Notion, Airtable, Google Sheets, Jira, Asana, Monday, Trello, Linear, ClickUp
- Utility: HTTP, Authenticated HTTP, Webhook
- Databases: PostgreSQL, MySQL, MongoDB, Azure SQL, Snowflake, BigQuery, Redshift,
            DynamoDB, Supabase, PlanetScale, CockroachDB, Elasticsearch, Redis,
            Firebase, SQLite, Oracle, SQL Server, MariaDB, Cassandra, ClickHouse
- Cloud Storage: AWS S3, Azure Blob, Google Cloud Storage, Dropbox, Box, OneDrive
- CRM: Salesforce, HubSpot, Zoho, Pipedrive, Freshsales
- Payments: Stripe, PayPal, Square

Authentication Module:
- OAuth2 (client credentials, password grant, authorization code, PKCE)
- Custom token-based authentication for any API
- Automatic token caching and refresh
"""

from .base import BaseConnector, ConnectorResult

# Original connectors
from .slack import SlackConnector
from .http import HTTPConnector
from .auth_http import AuthenticatedHTTPConnector
from .webhook import WebhookConnector
from .discord import DiscordConnector
from .github import GitHubConnector
from .notion import NotionConnector
from .airtable import AirtableConnector
from .google_sheets import GoogleSheetsConnector
from .email import EmailConnector

# Registry and utilities
from .registry import (
    ConnectorRegistry,
    get_connector,
    execute_connector,
    get_service_info,
    list_services_by_category,
    search_services,
    SERVICE_INFO,
)

# Submodules - import for namespace access
from . import databases
from . import cloud
from . import crm
from . import payments
from . import productivity
from . import auth

__all__ = [
    # Base classes
    "BaseConnector",
    "ConnectorResult",

    # Original connectors
    "SlackConnector",
    "HTTPConnector",
    "AuthenticatedHTTPConnector",
    "WebhookConnector",
    "DiscordConnector",
    "GitHubConnector",
    "NotionConnector",
    "AirtableConnector",
    "GoogleSheetsConnector",
    "EmailConnector",

    # Registry
    "ConnectorRegistry",
    "get_connector",
    "execute_connector",
    "get_service_info",
    "list_services_by_category",
    "search_services",
    "SERVICE_INFO",

    # Submodules
    "databases",
    "cloud",
    "crm",
    "payments",
    "productivity",
    "auth",
]
