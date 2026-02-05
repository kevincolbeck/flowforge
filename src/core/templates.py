"""
Pre-built Workflow Templates

Ready-to-use workflow templates that users can activate with one click.
Designed for common use cases - no coding required.
"""

from dataclasses import dataclass, field
from typing import Any


# Template categories for organization
TEMPLATE_CATEGORIES = {
    "sales": "Sales & CRM",
    "marketing": "Marketing",
    "support": "Customer Support",
    "development": "Development & DevOps",
    "productivity": "Productivity",
    "ecommerce": "E-commerce",
    "social": "Social Media",
    "data": "Data & Analytics",
    "communication": "Communication",
}

# Icons for categories and services
CATEGORY_ICONS = {
    "sales": "💼",
    "marketing": "📣",
    "support": "🎫",
    "development": "🐙",
    "productivity": "✅",
    "ecommerce": "🛒",
    "social": "📱",
    "data": "📊",
    "communication": "💬",
}


@dataclass
class WorkflowTemplate:
    """A pre-built workflow template."""

    id: str
    name: str
    description: str
    category: str
    trigger_service: str
    action_service: str
    trigger: dict[str, Any]
    steps: list[dict[str, Any]]
    icon: str = "⚡"
    difficulty: str = "easy"
    services: list[str] = field(default_factory=list)
    config_fields: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "category": self.category,
            "trigger_service": self.trigger_service,
            "action_service": self.action_service,
            "trigger": self.trigger,
            "steps": self.steps,
            "icon": self.icon,
            "difficulty": self.difficulty,
            "services": self.services,
            "config_fields": self.config_fields,
        }


# Pre-built templates
WORKFLOW_TEMPLATES: list[WorkflowTemplate] = [
    # ============== Sales & CRM ==============
    WorkflowTemplate(
        id="template_new_lead_slack",
        name="New Lead to Slack",
        description="Get notified in Slack when a new lead is added to your CRM",
        category="sales",
        icon="💼",
        trigger_service="hubspot",
        action_service="slack",
        services=["hubspot", "slack"],
        trigger={
            "type": "webhook",
            "service": "hubspot",
            "event": "contact.created",
        },
        steps=[
            {
                "id": "notify",
                "name": "Send Slack notification",
                "service": "slack",
                "action": "send_message",
                "inputs": {
                    "channel": "#sales",
                    "message": "🎉 New lead: {{trigger.data.properties.firstname}} {{trigger.data.properties.lastname}}"
                }
            }
        ],
    ),
    WorkflowTemplate(
        id="template_deal_won",
        name="Celebrate Won Deals",
        description="Automatically celebrate in Slack when a deal is closed",
        category="sales",
        icon="🎊",
        trigger_service="hubspot",
        action_service="slack",
        services=["hubspot", "slack"],
        trigger={
            "type": "webhook",
            "service": "hubspot",
            "event": "deal.updated",
        },
        steps=[
            {
                "id": "celebrate",
                "name": "Post celebration",
                "service": "slack",
                "action": "send_message",
                "inputs": {
                    "channel": "#wins",
                    "message": "🎊 DEAL WON! {{trigger.data.properties.dealname}}"
                }
            }
        ],
    ),

    # ============== E-commerce ==============
    WorkflowTemplate(
        id="template_order_sheets",
        name="Orders to Google Sheets",
        description="Log every new order to a Google Sheet for tracking",
        category="ecommerce",
        icon="📗",
        trigger_service="shopify",
        action_service="google_sheets",
        services=["shopify", "google_sheets"],
        trigger={
            "type": "webhook",
            "service": "shopify",
            "event": "orders/create",
        },
        steps=[
            {
                "id": "log_order",
                "name": "Add row to sheet",
                "service": "google_sheets",
                "action": "append_row",
                "inputs": {
                    "spreadsheet_id": "{{config.spreadsheet_id}}",
                    "values": ["{{trigger.data.order_number}}", "{{trigger.data.total_price}}"]
                }
            }
        ],
        config_fields=[
            {"name": "spreadsheet_id", "label": "Google Sheet ID", "type": "text", "required": True}
        ]
    ),
    WorkflowTemplate(
        id="template_order_slack",
        name="Order Notifications",
        description="Get instant Slack notifications for new orders",
        category="ecommerce",
        icon="🛒",
        trigger_service="shopify",
        action_service="slack",
        services=["shopify", "slack"],
        trigger={
            "type": "webhook",
            "service": "shopify",
            "event": "orders/create",
        },
        steps=[
            {
                "id": "notify",
                "name": "Notify team",
                "service": "slack",
                "action": "send_message",
                "inputs": {
                    "channel": "#orders",
                    "message": "💰 New order received! ${{trigger.data.total_price}}"
                }
            }
        ],
    ),

    # ============== Development ==============
    WorkflowTemplate(
        id="template_github_slack",
        name="GitHub to Slack",
        description="Post GitHub events (pushes, PRs) to Slack",
        category="development",
        icon="🐙",
        trigger_service="github",
        action_service="slack",
        services=["github", "slack"],
        trigger={
            "type": "webhook",
            "service": "github",
            "event": "push",
        },
        steps=[
            {
                "id": "notify",
                "name": "Post to Slack",
                "service": "slack",
                "action": "send_message",
                "inputs": {
                    "channel": "#dev",
                    "message": "📦 New push to {{trigger.data.repository.name}}"
                }
            }
        ],
    ),
    WorkflowTemplate(
        id="template_github_discord",
        name="GitHub to Discord",
        description="Post GitHub events to Discord",
        category="development",
        icon="🎮",
        trigger_service="github",
        action_service="discord",
        services=["github", "discord"],
        trigger={
            "type": "webhook",
            "service": "github",
            "event": "push",
        },
        steps=[
            {
                "id": "notify",
                "name": "Post to Discord",
                "service": "discord",
                "action": "send_message",
                "inputs": {
                    "channel_id": "{{config.channel_id}}",
                    "content": "📦 New push to {{trigger.data.repository.name}}"
                }
            }
        ],
        config_fields=[
            {"name": "channel_id", "label": "Discord Channel ID", "type": "text", "required": True}
        ]
    ),
    WorkflowTemplate(
        id="template_issue_notion",
        name="GitHub Issues to Notion",
        description="Create Notion pages from GitHub issues",
        category="development",
        icon="📝",
        trigger_service="github",
        action_service="notion",
        services=["github", "notion"],
        trigger={
            "type": "webhook",
            "service": "github",
            "event": "issues.opened",
        },
        steps=[
            {
                "id": "create_page",
                "name": "Create Notion page",
                "service": "notion",
                "action": "create_page",
                "inputs": {
                    "parent_id": "{{config.database_id}}",
                    "title": "{{trigger.data.issue.title}}",
                    "content": "{{trigger.data.issue.body}}"
                }
            }
        ],
        config_fields=[
            {"name": "database_id", "label": "Notion Database ID", "type": "text", "required": True}
        ]
    ),

    # ============== Productivity ==============
    WorkflowTemplate(
        id="template_form_sheets",
        name="Form to Google Sheets",
        description="Save form submissions to a spreadsheet",
        category="productivity",
        icon="📊",
        trigger_service="webhook",
        action_service="google_sheets",
        services=["webhook", "google_sheets"],
        trigger={
            "type": "webhook",
            "service": "webhook",
            "event": "receive",
        },
        steps=[
            {
                "id": "save",
                "name": "Save to sheet",
                "service": "google_sheets",
                "action": "append_row",
                "inputs": {
                    "spreadsheet_id": "{{config.spreadsheet_id}}",
                    "values": "{{trigger.data}}"
                }
            }
        ],
        config_fields=[
            {"name": "spreadsheet_id", "label": "Google Sheet ID", "type": "text", "required": True}
        ]
    ),
    WorkflowTemplate(
        id="template_form_airtable",
        name="Form to Airtable",
        description="Save form submissions to Airtable",
        category="productivity",
        icon="📋",
        trigger_service="webhook",
        action_service="airtable",
        services=["webhook", "airtable"],
        trigger={
            "type": "webhook",
            "service": "webhook",
            "event": "receive",
        },
        steps=[
            {
                "id": "save",
                "name": "Create record",
                "service": "airtable",
                "action": "create_record",
                "inputs": {
                    "base_id": "{{config.base_id}}",
                    "table_name": "{{config.table_name}}",
                    "fields": "{{trigger.data}}"
                }
            }
        ],
        config_fields=[
            {"name": "base_id", "label": "Airtable Base ID", "type": "text", "required": True},
            {"name": "table_name", "label": "Table Name", "type": "text", "required": True}
        ]
    ),
    WorkflowTemplate(
        id="template_webhook_slack",
        name="Webhook to Slack",
        description="Forward any webhook to a Slack channel",
        category="productivity",
        icon="🔗",
        trigger_service="webhook",
        action_service="slack",
        services=["webhook", "slack"],
        trigger={
            "type": "webhook",
            "service": "webhook",
            "event": "receive",
        },
        steps=[
            {
                "id": "forward",
                "name": "Send to Slack",
                "service": "slack",
                "action": "send_message",
                "inputs": {
                    "channel": "{{config.channel}}",
                    "message": "📨 Webhook received"
                }
            }
        ],
        config_fields=[
            {"name": "channel", "label": "Slack Channel", "type": "text", "default": "#webhooks"}
        ]
    ),

    # ============== Communication ==============
    WorkflowTemplate(
        id="template_slack_email",
        name="Slack to Email",
        description="Forward Slack messages to email",
        category="communication",
        icon="📧",
        trigger_service="slack",
        action_service="email",
        services=["slack", "email"],
        trigger={
            "type": "webhook",
            "service": "slack",
            "event": "message",
        },
        steps=[
            {
                "id": "forward",
                "name": "Send email",
                "service": "email",
                "action": "send",
                "inputs": {
                    "to": "{{config.email}}",
                    "subject": "Slack message from {{trigger.data.user}}",
                    "body": "{{trigger.data.text}}"
                }
            }
        ],
        config_fields=[
            {"name": "email", "label": "Email Address", "type": "email", "required": True}
        ]
    ),
    WorkflowTemplate(
        id="template_discord_slack",
        name="Discord to Slack",
        description="Mirror Discord messages to Slack",
        category="communication",
        icon="💬",
        trigger_service="discord",
        action_service="slack",
        services=["discord", "slack"],
        trigger={
            "type": "webhook",
            "service": "discord",
            "event": "message",
        },
        steps=[
            {
                "id": "forward",
                "name": "Forward to Slack",
                "service": "slack",
                "action": "send_message",
                "inputs": {
                    "channel": "{{config.channel}}",
                    "message": "[Discord] {{trigger.data.author}}: {{trigger.data.content}}"
                }
            }
        ],
        config_fields=[
            {"name": "channel", "label": "Slack Channel", "type": "text", "required": True}
        ]
    ),

    # ============== Support ==============
    WorkflowTemplate(
        id="template_support_slack",
        name="Support Tickets to Slack",
        description="Get notified in Slack when new support tickets arrive",
        category="support",
        icon="🎫",
        trigger_service="zendesk",
        action_service="slack",
        services=["zendesk", "slack"],
        trigger={
            "type": "webhook",
            "service": "zendesk",
            "event": "ticket.created",
        },
        steps=[
            {
                "id": "notify",
                "name": "Notify team",
                "service": "slack",
                "action": "send_message",
                "inputs": {
                    "channel": "#support",
                    "message": "🎫 New ticket: {{trigger.data.ticket.subject}}"
                }
            }
        ],
    ),

    # ============== Marketing ==============
    WorkflowTemplate(
        id="template_welcome_email",
        name="Welcome New Subscribers",
        description="Send a welcome email to new newsletter subscribers",
        category="marketing",
        icon="✉️",
        trigger_service="mailchimp",
        action_service="email",
        services=["mailchimp", "email"],
        trigger={
            "type": "webhook",
            "service": "mailchimp",
            "event": "subscribe",
        },
        steps=[
            {
                "id": "welcome",
                "name": "Send welcome email",
                "service": "email",
                "action": "send",
                "inputs": {
                    "to": "{{trigger.data.email}}",
                    "subject": "Welcome! 🎉",
                    "body": "Thanks for subscribing!"
                }
            }
        ],
    ),
]


def get_templates() -> list[WorkflowTemplate]:
    """Get all workflow templates."""
    return WORKFLOW_TEMPLATES


def get_templates_by_category(category: str) -> list[WorkflowTemplate]:
    """Get templates in a specific category."""
    return [t for t in WORKFLOW_TEMPLATES if t.category == category]


def get_template(template_id: str) -> WorkflowTemplate | None:
    """Get a specific template by ID."""
    for template in WORKFLOW_TEMPLATES:
        if template.id == template_id:
            return template
    return None


def get_template_categories() -> dict[str, str]:
    """Get all template categories."""
    return TEMPLATE_CATEGORIES


def search_templates(query: str) -> list[WorkflowTemplate]:
    """Search templates by name, description, or services."""
    query = query.lower()
    results = []
    for template in WORKFLOW_TEMPLATES:
        if (query in template.name.lower() or
            query in template.description.lower() or
            any(query in s for s in template.services)):
            results.append(template)
    return results
