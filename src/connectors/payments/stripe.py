"""
Stripe Connector

Connect to Stripe for payment processing, subscriptions, and invoicing.
"""

from typing import Any
import httpx
from ..base import BaseConnector, ConnectorResult


class StripeConnector(BaseConnector):
    """Connector for Stripe."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.api_key = credentials.get("api_key")
        self.base_url = "https://api.stripe.com/v1"

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
        }

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "create_customer": {
                "description": "Create a new customer",
                "parameters": {
                    "email": {"type": "string", "description": "Customer email", "required": True},
                    "name": {"type": "string", "description": "Customer name", "required": False},
                    "phone": {"type": "string", "description": "Phone number", "required": False},
                    "metadata": {"type": "object", "description": "Custom metadata", "required": False},
                },
            },
            "get_customer": {
                "description": "Get a customer by ID",
                "parameters": {
                    "customer_id": {"type": "string", "description": "Customer ID", "required": True},
                },
            },
            "update_customer": {
                "description": "Update a customer",
                "parameters": {
                    "customer_id": {"type": "string", "description": "Customer ID", "required": True},
                    "data": {"type": "object", "description": "Fields to update", "required": True},
                },
            },
            "list_customers": {
                "description": "List customers",
                "parameters": {
                    "limit": {"type": "integer", "description": "Max customers to return", "required": False},
                    "email": {"type": "string", "description": "Filter by email", "required": False},
                },
            },
            "create_payment_intent": {
                "description": "Create a payment intent",
                "parameters": {
                    "amount": {"type": "integer", "description": "Amount in cents", "required": True},
                    "currency": {"type": "string", "description": "Currency code (usd, eur, etc.)", "required": True},
                    "customer_id": {"type": "string", "description": "Customer ID", "required": False},
                    "metadata": {"type": "object", "description": "Custom metadata", "required": False},
                },
            },
            "get_payment_intent": {
                "description": "Get a payment intent",
                "parameters": {
                    "payment_intent_id": {"type": "string", "description": "Payment Intent ID", "required": True},
                },
            },
            "create_invoice": {
                "description": "Create an invoice",
                "parameters": {
                    "customer_id": {"type": "string", "description": "Customer ID", "required": True},
                    "auto_advance": {"type": "boolean", "description": "Auto-finalize invoice", "required": False},
                },
            },
            "add_invoice_item": {
                "description": "Add an item to an invoice",
                "parameters": {
                    "customer_id": {"type": "string", "description": "Customer ID", "required": True},
                    "amount": {"type": "integer", "description": "Amount in cents", "required": True},
                    "currency": {"type": "string", "description": "Currency", "required": True},
                    "description": {"type": "string", "description": "Item description", "required": False},
                    "invoice_id": {"type": "string", "description": "Invoice ID (draft)", "required": False},
                },
            },
            "finalize_invoice": {
                "description": "Finalize a draft invoice",
                "parameters": {
                    "invoice_id": {"type": "string", "description": "Invoice ID", "required": True},
                },
            },
            "create_subscription": {
                "description": "Create a subscription",
                "parameters": {
                    "customer_id": {"type": "string", "description": "Customer ID", "required": True},
                    "price_id": {"type": "string", "description": "Price ID", "required": True},
                },
            },
            "cancel_subscription": {
                "description": "Cancel a subscription",
                "parameters": {
                    "subscription_id": {"type": "string", "description": "Subscription ID", "required": True},
                    "at_period_end": {"type": "boolean", "description": "Cancel at period end", "required": False},
                },
            },
            "list_charges": {
                "description": "List charges",
                "parameters": {
                    "customer_id": {"type": "string", "description": "Filter by customer", "required": False},
                    "limit": {"type": "integer", "description": "Max charges", "required": False},
                },
            },
            "create_refund": {
                "description": "Create a refund",
                "parameters": {
                    "charge_id": {"type": "string", "description": "Charge ID", "required": False},
                    "payment_intent_id": {"type": "string", "description": "Payment Intent ID", "required": False},
                    "amount": {"type": "integer", "description": "Amount in cents (partial refund)", "required": False},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "create_customer":
                return await self._create_customer(params)
            elif action == "get_customer":
                return await self._get_customer(params["customer_id"])
            elif action == "update_customer":
                return await self._update_customer(params["customer_id"], params["data"])
            elif action == "list_customers":
                return await self._list_customers(params.get("limit"), params.get("email"))
            elif action == "create_payment_intent":
                return await self._create_payment_intent(params)
            elif action == "get_payment_intent":
                return await self._get_payment_intent(params["payment_intent_id"])
            elif action == "create_invoice":
                return await self._create_invoice(params["customer_id"], params.get("auto_advance", True))
            elif action == "add_invoice_item":
                return await self._add_invoice_item(params)
            elif action == "finalize_invoice":
                return await self._finalize_invoice(params["invoice_id"])
            elif action == "create_subscription":
                return await self._create_subscription(params["customer_id"], params["price_id"])
            elif action == "cancel_subscription":
                return await self._cancel_subscription(params["subscription_id"], params.get("at_period_end", True))
            elif action == "list_charges":
                return await self._list_charges(params.get("customer_id"), params.get("limit"))
            elif action == "create_refund":
                return await self._create_refund(params)
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _create_customer(self, params: dict) -> ConnectorResult:
        data = {"email": params["email"]}
        if params.get("name"):
            data["name"] = params["name"]
        if params.get("phone"):
            data["phone"] = params["phone"]
        if params.get("metadata"):
            for k, v in params["metadata"].items():
                data[f"metadata[{k}]"] = v

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/customers",
                headers=self._headers(),
                data=data,
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["id"], "email": result["email"]})

    async def _get_customer(self, customer_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/customers/{customer_id}",
                headers=self._headers(),
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data=response.json())

    async def _update_customer(self, customer_id: str, data: dict) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/customers/{customer_id}",
                headers=self._headers(),
                data=data,
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data={"id": customer_id, "updated": True})

    async def _list_customers(self, limit: int | None, email: str | None) -> ConnectorResult:
        params = {}
        if limit:
            params["limit"] = limit
        if email:
            params["email"] = email

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/customers",
                headers=self._headers(),
                params=params,
            )
            response.raise_for_status()
            result = response.json()
            customers = [{"id": c["id"], "email": c.get("email"), "name": c.get("name")} for c in result["data"]]
            return ConnectorResult(success=True, data={"customers": customers})

    async def _create_payment_intent(self, params: dict) -> ConnectorResult:
        data = {
            "amount": params["amount"],
            "currency": params["currency"],
        }
        if params.get("customer_id"):
            data["customer"] = params["customer_id"]
        if params.get("metadata"):
            for k, v in params["metadata"].items():
                data[f"metadata[{k}]"] = v

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/payment_intents",
                headers=self._headers(),
                data=data,
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={
                "id": result["id"],
                "client_secret": result["client_secret"],
                "status": result["status"],
            })

    async def _get_payment_intent(self, payment_intent_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/payment_intents/{payment_intent_id}",
                headers=self._headers(),
            )
            response.raise_for_status()
            return ConnectorResult(success=True, data=response.json())

    async def _create_invoice(self, customer_id: str, auto_advance: bool) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/invoices",
                headers=self._headers(),
                data={"customer": customer_id, "auto_advance": str(auto_advance).lower()},
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["id"], "status": result["status"]})

    async def _add_invoice_item(self, params: dict) -> ConnectorResult:
        data = {
            "customer": params["customer_id"],
            "amount": params["amount"],
            "currency": params["currency"],
        }
        if params.get("description"):
            data["description"] = params["description"]
        if params.get("invoice_id"):
            data["invoice"] = params["invoice_id"]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/invoiceitems",
                headers=self._headers(),
                data=data,
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["id"]})

    async def _finalize_invoice(self, invoice_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/invoices/{invoice_id}/finalize",
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["id"], "status": result["status"]})

    async def _create_subscription(self, customer_id: str, price_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/subscriptions",
                headers=self._headers(),
                data={"customer": customer_id, "items[0][price]": price_id},
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["id"], "status": result["status"]})

    async def _cancel_subscription(self, subscription_id: str, at_period_end: bool) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            if at_period_end:
                response = await client.post(
                    f"{self.base_url}/subscriptions/{subscription_id}",
                    headers=self._headers(),
                    data={"cancel_at_period_end": "true"},
                )
            else:
                response = await client.delete(
                    f"{self.base_url}/subscriptions/{subscription_id}",
                    headers=self._headers(),
                )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["id"], "canceled": True})

    async def _list_charges(self, customer_id: str | None, limit: int | None) -> ConnectorResult:
        params = {}
        if customer_id:
            params["customer"] = customer_id
        if limit:
            params["limit"] = limit

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/charges",
                headers=self._headers(),
                params=params,
            )
            response.raise_for_status()
            result = response.json()
            charges = [{"id": c["id"], "amount": c["amount"], "status": c["status"]} for c in result["data"]]
            return ConnectorResult(success=True, data={"charges": charges})

    async def _create_refund(self, params: dict) -> ConnectorResult:
        data = {}
        if params.get("charge_id"):
            data["charge"] = params["charge_id"]
        if params.get("payment_intent_id"):
            data["payment_intent"] = params["payment_intent_id"]
        if params.get("amount"):
            data["amount"] = params["amount"]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/refunds",
                headers=self._headers(),
                data=data,
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data={"id": result["id"], "amount": result["amount"]})
