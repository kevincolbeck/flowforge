"""
PayPal Connector

Connect to PayPal for payment processing.
"""

from typing import Any
import httpx
from ..base import BaseConnector, ConnectorResult


class PayPalConnector(BaseConnector):
    """Connector for PayPal."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.client_id = credentials.get("client_id")
        self.client_secret = credentials.get("client_secret")
        self.sandbox = credentials.get("sandbox", False)
        self.base_url = "https://api-m.sandbox.paypal.com" if self.sandbox else "https://api-m.paypal.com"
        self._access_token = None

    async def _get_access_token(self) -> str:
        """Get OAuth access token."""
        if self._access_token:
            return self._access_token

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/v1/oauth2/token",
                auth=(self.client_id, self.client_secret),
                data={"grant_type": "client_credentials"},
            )
            response.raise_for_status()
            result = response.json()
            self._access_token = result["access_token"]
            return self._access_token

    async def _headers(self):
        token = await self._get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "create_order": {
                "description": "Create a payment order",
                "parameters": {
                    "amount": {"type": "number", "description": "Amount", "required": True},
                    "currency": {"type": "string", "description": "Currency code", "required": True},
                    "description": {"type": "string", "description": "Order description", "required": False},
                },
            },
            "capture_order": {
                "description": "Capture an approved order",
                "parameters": {
                    "order_id": {"type": "string", "description": "Order ID", "required": True},
                },
            },
            "get_order": {
                "description": "Get order details",
                "parameters": {
                    "order_id": {"type": "string", "description": "Order ID", "required": True},
                },
            },
            "refund_capture": {
                "description": "Refund a captured payment",
                "parameters": {
                    "capture_id": {"type": "string", "description": "Capture ID", "required": True},
                    "amount": {"type": "number", "description": "Refund amount (optional for full)", "required": False},
                    "currency": {"type": "string", "description": "Currency", "required": False},
                },
            },
            "create_payout": {
                "description": "Create a payout to multiple recipients",
                "parameters": {
                    "items": {"type": "array", "description": "Payout items", "required": True},
                    "sender_batch_id": {"type": "string", "description": "Unique batch ID", "required": True},
                },
            },
            "get_payout": {
                "description": "Get payout batch details",
                "parameters": {
                    "payout_batch_id": {"type": "string", "description": "Payout batch ID", "required": True},
                },
            },
            "create_subscription": {
                "description": "Create a subscription",
                "parameters": {
                    "plan_id": {"type": "string", "description": "Plan ID", "required": True},
                    "subscriber": {"type": "object", "description": "Subscriber info", "required": True},
                },
            },
            "get_subscription": {
                "description": "Get subscription details",
                "parameters": {
                    "subscription_id": {"type": "string", "description": "Subscription ID", "required": True},
                },
            },
            "cancel_subscription": {
                "description": "Cancel a subscription",
                "parameters": {
                    "subscription_id": {"type": "string", "description": "Subscription ID", "required": True},
                    "reason": {"type": "string", "description": "Cancellation reason", "required": False},
                },
            },
            "create_invoice": {
                "description": "Create an invoice",
                "parameters": {
                    "recipient_email": {"type": "string", "description": "Recipient email", "required": True},
                    "items": {"type": "array", "description": "Invoice items", "required": True},
                    "currency": {"type": "string", "description": "Currency", "required": True},
                },
            },
            "send_invoice": {
                "description": "Send an invoice",
                "parameters": {
                    "invoice_id": {"type": "string", "description": "Invoice ID", "required": True},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "create_order":
                return await self._create_order(params)
            elif action == "capture_order":
                return await self._capture_order(params["order_id"])
            elif action == "get_order":
                return await self._get_order(params["order_id"])
            elif action == "refund_capture":
                return await self._refund_capture(params)
            elif action == "create_payout":
                return await self._create_payout(params["items"], params["sender_batch_id"])
            elif action == "get_payout":
                return await self._get_payout(params["payout_batch_id"])
            elif action == "create_subscription":
                return await self._create_subscription(params["plan_id"], params["subscriber"])
            elif action == "get_subscription":
                return await self._get_subscription(params["subscription_id"])
            elif action == "cancel_subscription":
                return await self._cancel_subscription(params["subscription_id"], params.get("reason"))
            elif action == "create_invoice":
                return await self._create_invoice(params)
            elif action == "send_invoice":
                return await self._send_invoice(params["invoice_id"])
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _create_order(self, params: dict) -> ConnectorResult:
        body = {
            "intent": "CAPTURE",
            "purchase_units": [{
                "amount": {
                    "currency_code": params["currency"],
                    "value": str(params["amount"]),
                },
            }],
        }
        if params.get("description"):
            body["purchase_units"][0]["description"] = params["description"]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/v2/checkout/orders",
                headers=await self._headers(),
                json=body,
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["id"], "status": result["status"]})

    async def _capture_order(self, order_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/v2/checkout/orders/{order_id}/capture",
                headers=await self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["id"], "status": result["status"]})

    async def _get_order(self, order_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/v2/checkout/orders/{order_id}",
                headers=await self._headers(),
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data=response.json())

    async def _refund_capture(self, params: dict) -> ConnectorResult:
        body = {}
        if params.get("amount"):
            body["amount"] = {
                "value": str(params["amount"]),
                "currency_code": params.get("currency", "USD"),
            }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/v2/payments/captures/{params['capture_id']}/refund",
                headers=await self._headers(),
                json=body if body else None,
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["id"], "status": result["status"]})

    async def _create_payout(self, items: list, sender_batch_id: str) -> ConnectorResult:
        body = {
            "sender_batch_header": {
                "sender_batch_id": sender_batch_id,
                "email_subject": "You have a payout!",
            },
            "items": items,
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/v1/payments/payouts",
                headers=await self._headers(),
                json=body,
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={
                "payout_batch_id": result["batch_header"]["payout_batch_id"],
                "batch_status": result["batch_header"]["batch_status"],
            })

    async def _get_payout(self, payout_batch_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/v1/payments/payouts/{payout_batch_id}",
                headers=await self._headers(),
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data=response.json())

    async def _create_subscription(self, plan_id: str, subscriber: dict) -> ConnectorResult:
        body = {
            "plan_id": plan_id,
            "subscriber": subscriber,
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/v1/billing/subscriptions",
                headers=await self._headers(),
                json=body,
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["id"], "status": result["status"]})

    async def _get_subscription(self, subscription_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/v1/billing/subscriptions/{subscription_id}",
                headers=await self._headers(),
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data=response.json())

    async def _cancel_subscription(self, subscription_id: str, reason: str | None) -> ConnectorResult:
        body = {"reason": reason or "Customer requested cancellation"}

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/v1/billing/subscriptions/{subscription_id}/cancel",
                headers=await self._headers(),
                json=body,
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"id": subscription_id, "canceled": True})

    async def _create_invoice(self, params: dict) -> ConnectorResult:
        body = {
            "detail": {
                "currency_code": params["currency"],
            },
            "invoicer": {},
            "primary_recipients": [{
                "billing_info": {
                    "email_address": params["recipient_email"],
                }
            }],
            "items": params["items"],
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/v2/invoicing/invoices",
                headers=await self._headers(),
                json=body,
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result.get("id")})

    async def _send_invoice(self, invoice_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/v2/invoicing/invoices/{invoice_id}/send",
                headers=await self._headers(),
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"id": invoice_id, "sent": True})
