"""
Webhook Management

Handles incoming webhooks to trigger workflows.
"""

import hashlib
import hmac
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

logger = logging.getLogger(__name__)


@dataclass
class WebhookConfig:
    """Configuration for a webhook endpoint."""

    workflow_id: str
    path: str
    service: str | None = None
    event: str | None = None
    secret: str | None = None  # For signature verification
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class WebhookEvent:
    """Represents an incoming webhook event."""

    path: str
    method: str
    headers: dict[str, str]
    body: bytes
    query_params: dict[str, str] = field(default_factory=dict)
    received_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def json_body(self) -> dict[str, Any] | None:
        """Parse body as JSON."""
        try:
            import json
            return json.loads(self.body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None


class WebhookManager:
    """
    Manages webhook endpoints and routing.

    Supports:
    - Dynamic webhook creation per workflow
    - Signature verification for common providers
    - Event routing to workflow execution
    """

    def __init__(
        self,
        execute_callback: Callable[[str, dict[str, Any]], Any],
        base_path: str = "/webhooks",
    ):
        self.execute_callback = execute_callback
        self.base_path = base_path
        self._webhooks: dict[str, WebhookConfig] = {}  # path -> config
        self._workflow_webhooks: dict[str, list[str]] = {}  # workflow_id -> [paths]

    def register_webhook(
        self,
        workflow_id: str,
        path: str | None = None,
        service: str | None = None,
        event: str | None = None,
        secret: str | None = None,
    ) -> WebhookConfig:
        """Register a new webhook endpoint."""
        # Generate path if not provided
        if not path:
            if service and event:
                path = f"{self.base_path}/{service}/{event.replace('.', '/')}/{workflow_id[:8]}"
            else:
                path = f"{self.base_path}/{workflow_id[:8]}"

        # Ensure path starts with base
        if not path.startswith(self.base_path):
            path = f"{self.base_path}/{path.lstrip('/')}"

        config = WebhookConfig(
            workflow_id=workflow_id,
            path=path,
            service=service,
            event=event,
            secret=secret,
        )

        self._webhooks[path] = config

        # Track webhooks by workflow
        if workflow_id not in self._workflow_webhooks:
            self._workflow_webhooks[workflow_id] = []
        self._workflow_webhooks[workflow_id].append(path)

        logger.info(f"Registered webhook for workflow {workflow_id} at {path}")
        return config

    def unregister_webhook(self, path: str) -> bool:
        """Unregister a webhook endpoint."""
        if path not in self._webhooks:
            return False

        config = self._webhooks[path]
        del self._webhooks[path]

        # Remove from workflow tracking
        if config.workflow_id in self._workflow_webhooks:
            self._workflow_webhooks[config.workflow_id] = [
                p for p in self._workflow_webhooks[config.workflow_id] if p != path
            ]

        logger.info(f"Unregistered webhook at {path}")
        return True

    def unregister_workflow_webhooks(self, workflow_id: str) -> int:
        """Unregister all webhooks for a workflow."""
        if workflow_id not in self._workflow_webhooks:
            return 0

        count = 0
        for path in list(self._workflow_webhooks[workflow_id]):
            if self.unregister_webhook(path):
                count += 1

        del self._workflow_webhooks[workflow_id]
        return count

    def get_webhook_config(self, path: str) -> WebhookConfig | None:
        """Get webhook configuration by path."""
        return self._webhooks.get(path)

    def get_workflow_webhooks(self, workflow_id: str) -> list[WebhookConfig]:
        """Get all webhooks for a workflow."""
        paths = self._workflow_webhooks.get(workflow_id, [])
        return [self._webhooks[p] for p in paths if p in self._webhooks]

    async def handle_webhook(self, event: WebhookEvent) -> dict[str, Any]:
        """
        Handle an incoming webhook event.

        Returns result of workflow execution or error info.
        """
        # Find matching webhook
        config = self._find_matching_webhook(event.path)
        if not config:
            logger.warning(f"No webhook found for path: {event.path}")
            return {"status": "error", "error": "Webhook not found"}

        if not config.enabled:
            logger.info(f"Webhook at {event.path} is disabled")
            return {"status": "error", "error": "Webhook disabled"}

        # Verify signature if secret is configured
        if config.secret:
            if not self._verify_signature(event, config):
                logger.warning(f"Invalid signature for webhook at {event.path}")
                return {"status": "error", "error": "Invalid signature"}

        # Build trigger data
        trigger_data = {
            "webhook": {
                "path": event.path,
                "method": event.method,
                "headers": event.headers,
                "query_params": event.query_params,
                "received_at": event.received_at.isoformat(),
            },
            "data": event.json_body or {},
            "raw_body": event.body.decode("utf-8", errors="replace"),
        }

        # Add service-specific parsing
        if config.service:
            trigger_data["service"] = config.service
            trigger_data["event"] = config.event
            trigger_data = self._parse_service_payload(config.service, trigger_data, event)

        # Execute workflow
        logger.info(f"Triggering workflow {config.workflow_id} from webhook")
        try:
            result = await self.execute_callback(config.workflow_id, trigger_data)
            return {"status": "success", "result": result}
        except Exception as e:
            logger.error(f"Workflow execution failed: {e}")
            return {"status": "error", "error": str(e)}

    def _find_matching_webhook(self, path: str) -> WebhookConfig | None:
        """Find webhook config matching a path."""
        # Exact match
        if path in self._webhooks:
            return self._webhooks[path]

        # Try without trailing slash
        normalized = path.rstrip("/")
        if normalized in self._webhooks:
            return self._webhooks[normalized]

        # Try with trailing slash
        if f"{normalized}/" in self._webhooks:
            return self._webhooks[f"{normalized}/"]

        return None

    def _verify_signature(self, event: WebhookEvent, config: WebhookConfig) -> bool:
        """Verify webhook signature based on service type."""
        if not config.secret:
            return True

        service = config.service or ""

        # GitHub signature verification
        if service.lower() == "github":
            sig_header = event.headers.get("x-hub-signature-256", "")
            if sig_header.startswith("sha256="):
                expected = hmac.new(
                    config.secret.encode(),
                    event.body,
                    hashlib.sha256,
                ).hexdigest()
                return hmac.compare_digest(sig_header[7:], expected)

        # Stripe signature verification
        elif service.lower() == "stripe":
            sig_header = event.headers.get("stripe-signature", "")
            # Parse Stripe signature format: t=timestamp,v1=signature
            try:
                parts = dict(p.split("=") for p in sig_header.split(","))
                timestamp = parts.get("t", "")
                signature = parts.get("v1", "")
                payload = f"{timestamp}.{event.body.decode()}"
                expected = hmac.new(
                    config.secret.encode(),
                    payload.encode(),
                    hashlib.sha256,
                ).hexdigest()
                return hmac.compare_digest(signature, expected)
            except (ValueError, KeyError):
                return False

        # Shopify signature verification
        elif service.lower() == "shopify":
            sig_header = event.headers.get("x-shopify-hmac-sha256", "")
            import base64
            expected = base64.b64encode(
                hmac.new(config.secret.encode(), event.body, hashlib.sha256).digest()
            ).decode()
            return hmac.compare_digest(sig_header, expected)

        # Generic HMAC-SHA256 verification
        else:
            sig_header = (
                event.headers.get("x-signature-256", "") or
                event.headers.get("x-webhook-signature", "") or
                event.headers.get("signature", "")
            )
            if sig_header:
                expected = hmac.new(
                    config.secret.encode(),
                    event.body,
                    hashlib.sha256,
                ).hexdigest()
                # Handle "sha256=" prefix
                if sig_header.startswith("sha256="):
                    sig_header = sig_header[7:]
                return hmac.compare_digest(sig_header, expected)

        return True  # No signature to verify

    def _parse_service_payload(
        self,
        service: str,
        trigger_data: dict[str, Any],
        event: WebhookEvent,
    ) -> dict[str, Any]:
        """Parse service-specific webhook payloads."""
        data = trigger_data.get("data", {})

        if service.lower() == "github":
            trigger_data["github"] = {
                "event": event.headers.get("x-github-event"),
                "delivery": event.headers.get("x-github-delivery"),
                "action": data.get("action"),
                "repository": data.get("repository", {}).get("full_name"),
                "sender": data.get("sender", {}).get("login"),
            }

        elif service.lower() == "stripe":
            trigger_data["stripe"] = {
                "type": data.get("type"),
                "object": data.get("data", {}).get("object", {}),
                "livemode": data.get("livemode"),
            }

        elif service.lower() == "shopify":
            trigger_data["shopify"] = {
                "topic": event.headers.get("x-shopify-topic"),
                "shop_domain": event.headers.get("x-shopify-shop-domain"),
                "api_version": event.headers.get("x-shopify-api-version"),
            }

        elif service.lower() == "slack":
            trigger_data["slack"] = {
                "type": data.get("type"),
                "event": data.get("event", {}),
                "team_id": data.get("team_id"),
                "channel": data.get("event", {}).get("channel"),
            }

        return trigger_data

    def list_webhooks(self) -> list[dict[str, Any]]:
        """List all registered webhooks."""
        return [
            {
                "path": config.path,
                "workflow_id": config.workflow_id,
                "service": config.service,
                "event": config.event,
                "enabled": config.enabled,
                "created_at": config.created_at.isoformat(),
            }
            for config in self._webhooks.values()
        ]
