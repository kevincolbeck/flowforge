"""
Service Registry

Maps service names to their API specifications, auth requirements,
and common actions. Users can just type "Shopify" instead of URLs.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ServiceConfig:
    """Configuration for a known service."""

    name: str
    display_name: str
    description: str
    openapi_url: str | None = None
    docs_url: str | None = None
    base_url: str | None = None
    auth_type: str = "api_key"  # api_key, oauth2, bearer, basic
    auth_config: dict[str, Any] = field(default_factory=dict)
    common_triggers: list[dict[str, str]] = field(default_factory=list)
    common_actions: list[dict[str, str]] = field(default_factory=list)
    category: str = "other"
    icon: str = ""


# Pre-configured popular services
SERVICES: dict[str, ServiceConfig] = {
    # E-commerce
    "shopify": ServiceConfig(
        name="shopify",
        display_name="Shopify",
        description="E-commerce platform for online stores",
        docs_url="https://shopify.dev/api",
        base_url="https://{store}.myshopify.com/admin/api/2024-01",
        auth_type="api_key",
        auth_config={"header": "X-Shopify-Access-Token"},
        common_triggers=[
            {"event": "orders/create", "description": "When a new order is placed"},
            {"event": "products/create", "description": "When a new product is added"},
            {"event": "customers/create", "description": "When a new customer signs up"},
        ],
        common_actions=[
            {"action": "create_product", "description": "Create a new product"},
            {"action": "update_inventory", "description": "Update inventory levels"},
            {"action": "create_order", "description": "Create an order"},
        ],
        category="ecommerce",
    ),
    "stripe": ServiceConfig(
        name="stripe",
        display_name="Stripe",
        description="Payment processing platform",
        openapi_url="https://raw.githubusercontent.com/stripe/openapi/master/openapi/spec3.json",
        base_url="https://api.stripe.com/v1",
        auth_type="bearer",
        common_triggers=[
            {"event": "payment_intent.succeeded", "description": "When a payment succeeds"},
            {"event": "customer.created", "description": "When a new customer is created"},
            {"event": "invoice.paid", "description": "When an invoice is paid"},
        ],
        common_actions=[
            {"action": "create_customer", "description": "Create a new customer"},
            {"action": "create_payment_intent", "description": "Create a payment"},
            {"action": "create_invoice", "description": "Create an invoice"},
        ],
        category="payments",
    ),
    "woocommerce": ServiceConfig(
        name="woocommerce",
        display_name="WooCommerce",
        description="WordPress e-commerce plugin",
        base_url="https://{site}/wp-json/wc/v3",
        auth_type="basic",
        common_triggers=[
            {"event": "order.created", "description": "When a new order is placed"},
            {"event": "product.created", "description": "When a new product is added"},
        ],
        common_actions=[
            {"action": "create_product", "description": "Create a new product"},
            {"action": "update_order", "description": "Update an order"},
        ],
        category="ecommerce",
    ),

    # Communication
    "slack": ServiceConfig(
        name="slack",
        display_name="Slack",
        description="Team communication platform",
        openapi_url="https://raw.githubusercontent.com/slackapi/slack-api-specs/master/web-api/slack_web_openapi_v2.json",
        base_url="https://slack.com/api",
        auth_type="bearer",
        common_triggers=[
            {"event": "message", "description": "When a message is posted"},
            {"event": "reaction_added", "description": "When a reaction is added"},
            {"event": "channel_created", "description": "When a channel is created"},
        ],
        common_actions=[
            {"action": "chat.postMessage", "description": "Send a message to a channel"},
            {"action": "files.upload", "description": "Upload a file"},
            {"action": "users.info", "description": "Get user information"},
        ],
        category="communication",
    ),
    "discord": ServiceConfig(
        name="discord",
        display_name="Discord",
        description="Community chat platform",
        openapi_url="https://raw.githubusercontent.com/discord/discord-api-spec/main/specs/openapi.json",
        base_url="https://discord.com/api/v10",
        auth_type="bearer",
        auth_config={"prefix": "Bot"},
        common_triggers=[
            {"event": "MESSAGE_CREATE", "description": "When a message is sent"},
            {"event": "GUILD_MEMBER_ADD", "description": "When someone joins a server"},
        ],
        common_actions=[
            {"action": "create_message", "description": "Send a message"},
            {"action": "create_reaction", "description": "Add a reaction"},
        ],
        category="communication",
    ),
    "twilio": ServiceConfig(
        name="twilio",
        display_name="Twilio",
        description="SMS and voice communication API",
        base_url="https://api.twilio.com/2010-04-01",
        auth_type="basic",
        common_triggers=[
            {"event": "incoming_sms", "description": "When an SMS is received"},
            {"event": "incoming_call", "description": "When a call is received"},
        ],
        common_actions=[
            {"action": "send_sms", "description": "Send an SMS message"},
            {"action": "make_call", "description": "Make a phone call"},
        ],
        category="communication",
    ),
    "sendgrid": ServiceConfig(
        name="sendgrid",
        display_name="SendGrid",
        description="Email delivery service",
        base_url="https://api.sendgrid.com/v3",
        auth_type="bearer",
        common_actions=[
            {"action": "send_email", "description": "Send an email"},
            {"action": "add_contact", "description": "Add a contact to a list"},
        ],
        category="email",
    ),

    # Productivity
    "notion": ServiceConfig(
        name="notion",
        display_name="Notion",
        description="All-in-one workspace for notes and databases",
        base_url="https://api.notion.com/v1",
        auth_type="bearer",
        auth_config={"header": "Authorization", "prefix": "Bearer", "version_header": "Notion-Version"},
        common_triggers=[
            {"event": "page_created", "description": "When a page is created"},
            {"event": "database_updated", "description": "When a database is updated"},
        ],
        common_actions=[
            {"action": "create_page", "description": "Create a new page"},
            {"action": "update_page", "description": "Update a page"},
            {"action": "query_database", "description": "Query a database"},
        ],
        category="productivity",
    ),
    "airtable": ServiceConfig(
        name="airtable",
        display_name="Airtable",
        description="Spreadsheet-database hybrid",
        base_url="https://api.airtable.com/v0",
        auth_type="bearer",
        common_triggers=[
            {"event": "record_created", "description": "When a record is created"},
            {"event": "record_updated", "description": "When a record is updated"},
        ],
        common_actions=[
            {"action": "create_record", "description": "Create a new record"},
            {"action": "update_record", "description": "Update a record"},
            {"action": "list_records", "description": "List records in a table"},
        ],
        category="productivity",
    ),
    "google_sheets": ServiceConfig(
        name="google_sheets",
        display_name="Google Sheets",
        description="Spreadsheet application",
        base_url="https://sheets.googleapis.com/v4",
        auth_type="oauth2",
        auth_config={
            "auth_url": "https://accounts.google.com/o/oauth2/v2/auth",
            "token_url": "https://oauth2.googleapis.com/token",
            "scopes": ["https://www.googleapis.com/auth/spreadsheets"],
        },
        common_triggers=[
            {"event": "row_added", "description": "When a new row is added"},
        ],
        common_actions=[
            {"action": "append_row", "description": "Add a row to a sheet"},
            {"action": "update_cell", "description": "Update a cell"},
            {"action": "get_values", "description": "Get values from a range"},
        ],
        category="productivity",
    ),
    "trello": ServiceConfig(
        name="trello",
        display_name="Trello",
        description="Kanban-style project management",
        base_url="https://api.trello.com/1",
        auth_type="api_key",
        auth_config={"key_param": "key", "token_param": "token"},
        common_triggers=[
            {"event": "card_created", "description": "When a card is created"},
            {"event": "card_moved", "description": "When a card is moved"},
        ],
        common_actions=[
            {"action": "create_card", "description": "Create a new card"},
            {"action": "move_card", "description": "Move a card to another list"},
            {"action": "add_comment", "description": "Add a comment to a card"},
        ],
        category="productivity",
    ),
    "asana": ServiceConfig(
        name="asana",
        display_name="Asana",
        description="Project management platform",
        base_url="https://app.asana.com/api/1.0",
        auth_type="bearer",
        common_triggers=[
            {"event": "task_created", "description": "When a task is created"},
            {"event": "task_completed", "description": "When a task is completed"},
        ],
        common_actions=[
            {"action": "create_task", "description": "Create a new task"},
            {"action": "update_task", "description": "Update a task"},
            {"action": "add_comment", "description": "Add a comment to a task"},
        ],
        category="productivity",
    ),

    # Developer Tools
    "github": ServiceConfig(
        name="github",
        display_name="GitHub",
        description="Code hosting and collaboration",
        openapi_url="https://raw.githubusercontent.com/github/rest-api-description/main/descriptions/api.github.com/api.github.com.json",
        base_url="https://api.github.com",
        auth_type="bearer",
        common_triggers=[
            {"event": "push", "description": "When code is pushed"},
            {"event": "pull_request", "description": "When a PR is opened"},
            {"event": "issues", "description": "When an issue is created"},
        ],
        common_actions=[
            {"action": "create_issue", "description": "Create an issue"},
            {"action": "create_comment", "description": "Comment on an issue/PR"},
            {"action": "create_release", "description": "Create a release"},
        ],
        category="developer",
    ),
    "gitlab": ServiceConfig(
        name="gitlab",
        display_name="GitLab",
        description="DevOps platform",
        base_url="https://gitlab.com/api/v4",
        auth_type="bearer",
        common_triggers=[
            {"event": "push", "description": "When code is pushed"},
            {"event": "merge_request", "description": "When an MR is opened"},
        ],
        common_actions=[
            {"action": "create_issue", "description": "Create an issue"},
            {"action": "create_mr", "description": "Create a merge request"},
        ],
        category="developer",
    ),
    "jira": ServiceConfig(
        name="jira",
        display_name="Jira",
        description="Issue tracking and project management",
        base_url="https://{domain}.atlassian.net/rest/api/3",
        auth_type="basic",
        auth_config={"email_as_username": True},
        common_triggers=[
            {"event": "issue_created", "description": "When an issue is created"},
            {"event": "issue_updated", "description": "When an issue is updated"},
        ],
        common_actions=[
            {"action": "create_issue", "description": "Create an issue"},
            {"action": "update_issue", "description": "Update an issue"},
            {"action": "add_comment", "description": "Add a comment"},
        ],
        category="developer",
    ),

    # CRM
    "hubspot": ServiceConfig(
        name="hubspot",
        display_name="HubSpot",
        description="CRM and marketing platform",
        base_url="https://api.hubapi.com",
        auth_type="bearer",
        common_triggers=[
            {"event": "contact.created", "description": "When a contact is created"},
            {"event": "deal.created", "description": "When a deal is created"},
        ],
        common_actions=[
            {"action": "create_contact", "description": "Create a contact"},
            {"action": "create_deal", "description": "Create a deal"},
            {"action": "update_contact", "description": "Update a contact"},
        ],
        category="crm",
    ),
    "salesforce": ServiceConfig(
        name="salesforce",
        display_name="Salesforce",
        description="CRM platform",
        base_url="https://{instance}.salesforce.com/services/data/v58.0",
        auth_type="oauth2",
        common_triggers=[
            {"event": "lead_created", "description": "When a lead is created"},
            {"event": "opportunity_updated", "description": "When an opportunity is updated"},
        ],
        common_actions=[
            {"action": "create_lead", "description": "Create a lead"},
            {"action": "create_opportunity", "description": "Create an opportunity"},
            {"action": "update_record", "description": "Update a record"},
        ],
        category="crm",
    ),

    # AI
    "openai": ServiceConfig(
        name="openai",
        display_name="OpenAI",
        description="AI models API (GPT, DALL-E, etc.)",
        openapi_url="https://raw.githubusercontent.com/openai/openai-openapi/master/openapi.yaml",
        base_url="https://api.openai.com/v1",
        auth_type="bearer",
        common_actions=[
            {"action": "chat_completion", "description": "Generate text with GPT"},
            {"action": "create_image", "description": "Generate an image with DALL-E"},
            {"action": "create_embedding", "description": "Generate embeddings"},
        ],
        category="ai",
    ),
    "anthropic": ServiceConfig(
        name="anthropic",
        display_name="Anthropic",
        description="Claude AI API",
        base_url="https://api.anthropic.com/v1",
        auth_type="api_key",
        auth_config={"header": "x-api-key"},
        common_actions=[
            {"action": "messages", "description": "Generate text with Claude"},
        ],
        category="ai",
    ),

    # Storage
    "dropbox": ServiceConfig(
        name="dropbox",
        display_name="Dropbox",
        description="Cloud file storage",
        base_url="https://api.dropboxapi.com/2",
        auth_type="bearer",
        common_triggers=[
            {"event": "file_added", "description": "When a file is added"},
        ],
        common_actions=[
            {"action": "upload_file", "description": "Upload a file"},
            {"action": "download_file", "description": "Download a file"},
            {"action": "create_folder", "description": "Create a folder"},
        ],
        category="storage",
    ),
    "aws_s3": ServiceConfig(
        name="aws_s3",
        display_name="Amazon S3",
        description="Cloud object storage",
        base_url="https://s3.{region}.amazonaws.com",
        auth_type="aws",
        common_triggers=[
            {"event": "object_created", "description": "When an object is created"},
        ],
        common_actions=[
            {"action": "put_object", "description": "Upload an object"},
            {"action": "get_object", "description": "Download an object"},
            {"action": "delete_object", "description": "Delete an object"},
        ],
        category="storage",
    ),

    # Utilities - Generic HTTP/Webhooks
    "http": ServiceConfig(
        name="http",
        display_name="HTTP Request",
        description="Make custom HTTP requests to any API with JSON",
        base_url="",  # User provides URL
        auth_type="custom",
        auth_config={"supports": ["none", "api_key", "bearer", "basic"]},
        common_actions=[
            {"action": "get", "description": "Make a GET request"},
            {"action": "post", "description": "Make a POST request with JSON body"},
            {"action": "put", "description": "Make a PUT request with JSON body"},
            {"action": "patch", "description": "Make a PATCH request with JSON body"},
            {"action": "delete", "description": "Make a DELETE request"},
        ],
        category="utilities",
    ),
    "webhook": ServiceConfig(
        name="webhook",
        display_name="Webhooks",
        description="Send and receive webhooks with custom JSON payloads",
        base_url="",  # User provides URL
        auth_type="custom",
        common_triggers=[
            {"event": "receive", "description": "When a webhook is received"},
        ],
        common_actions=[
            {"action": "send", "description": "Send a webhook with JSON payload"},
            {"action": "send_form", "description": "Send a webhook with form data"},
        ],
        category="utilities",
    ),
    "json_transform": ServiceConfig(
        name="json_transform",
        display_name="JSON Transform",
        description="Transform, filter, and manipulate JSON data",
        auth_type="none",
        common_actions=[
            {"action": "map", "description": "Map/transform JSON fields"},
            {"action": "filter", "description": "Filter JSON array by condition"},
            {"action": "merge", "description": "Merge multiple JSON objects"},
            {"action": "extract", "description": "Extract nested field from JSON"},
            {"action": "format", "description": "Format JSON to string template"},
        ],
        category="utilities",
    ),
    "code": ServiceConfig(
        name="code",
        display_name="Run Code",
        description="Execute custom Python/JavaScript code for transformations",
        auth_type="none",
        common_actions=[
            {"action": "python", "description": "Run Python code"},
            {"action": "javascript", "description": "Run JavaScript code"},
            {"action": "jq", "description": "Run jq JSON query"},
        ],
        category="utilities",
    ),
    "delay": ServiceConfig(
        name="delay",
        display_name="Delay",
        description="Add delays and scheduling to workflows",
        auth_type="none",
        common_actions=[
            {"action": "wait", "description": "Wait for specified time"},
            {"action": "wait_until", "description": "Wait until specific time"},
            {"action": "rate_limit", "description": "Rate limit execution"},
        ],
        category="utilities",
    ),

    # Databases
    "postgresql": ServiceConfig(
        name="postgresql",
        display_name="PostgreSQL",
        description="PostgreSQL database",
        auth_type="connection_string",
        common_actions=[
            {"action": "query", "description": "Run a SQL query"},
            {"action": "insert", "description": "Insert a row"},
            {"action": "update", "description": "Update rows"},
            {"action": "delete", "description": "Delete rows"},
        ],
        category="database",
    ),
    "mysql": ServiceConfig(
        name="mysql",
        display_name="MySQL",
        description="MySQL database",
        auth_type="connection_string",
        common_actions=[
            {"action": "query", "description": "Run a SQL query"},
            {"action": "insert", "description": "Insert a row"},
            {"action": "update", "description": "Update rows"},
            {"action": "delete", "description": "Delete rows"},
        ],
        category="database",
    ),
    "mongodb": ServiceConfig(
        name="mongodb",
        display_name="MongoDB",
        description="MongoDB NoSQL database",
        auth_type="connection_string",
        common_actions=[
            {"action": "find", "description": "Find documents"},
            {"action": "insert", "description": "Insert a document"},
            {"action": "update", "description": "Update documents"},
            {"action": "delete", "description": "Delete documents"},
            {"action": "aggregate", "description": "Run aggregation pipeline"},
        ],
        category="database",
    ),
    "redis": ServiceConfig(
        name="redis",
        display_name="Redis",
        description="Redis key-value store",
        auth_type="connection_string",
        common_actions=[
            {"action": "get", "description": "Get a value"},
            {"action": "set", "description": "Set a value"},
            {"action": "delete", "description": "Delete a key"},
            {"action": "publish", "description": "Publish to channel"},
        ],
        category="database",
    ),
}


class ServiceRegistry:
    """Registry for looking up service configurations."""

    def __init__(self):
        self.services = SERVICES.copy()

    def get(self, name: str) -> ServiceConfig | None:
        """Get a service by name (case-insensitive)."""
        return self.services.get(name.lower())

    def search(self, query: str) -> list[ServiceConfig]:
        """Search for services matching a query."""
        query_lower = query.lower()
        results = []
        for service in self.services.values():
            if (query_lower in service.name.lower() or
                query_lower in service.display_name.lower() or
                query_lower in service.description.lower()):
                results.append(service)
        return results

    def list_by_category(self, category: str) -> list[ServiceConfig]:
        """List all services in a category."""
        return [s for s in self.services.values() if s.category == category]

    def list_all(self) -> list[ServiceConfig]:
        """List all services."""
        return list(self.services.values())

    def get_categories(self) -> list[str]:
        """Get all unique categories."""
        return list(set(s.category for s in self.services.values()))

    def add_service(self, service: ServiceConfig):
        """Add a custom service to the registry."""
        self.services[service.name.lower()] = service

    def find_by_intent(self, intent: str) -> list[tuple[ServiceConfig, str]]:
        """
        Find services mentioned in a natural language intent.

        Returns list of (service, matched_term) tuples.
        """
        intent_lower = intent.lower()
        matches = []

        for service in self.services.values():
            # Check name and display name
            if service.name in intent_lower or service.display_name.lower() in intent_lower:
                matches.append((service, service.display_name))

        return matches
