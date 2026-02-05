"""
Square Connector

Connect to Square for payment processing and commerce.
"""

from typing import Any
import httpx
from ..base import BaseConnector, ConnectorResult


class SquareConnector(BaseConnector):
    """Connector for Square."""

    def __init__(self, credentials: dict[str, Any]):
        super().__init__(credentials)
        self.access_token = credentials.get("access_token")
        self.sandbox = credentials.get("sandbox", False)
        self.base_url = "https://connect.squareupsandbox.com" if self.sandbox else "https://connect.squareup.com"
        self.location_id = credentials.get("location_id")

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Square-Version": "2024-01-18",
        }

    @classmethod
    def get_actions(cls) -> dict[str, dict[str, Any]]:
        return {
            "create_payment": {
                "description": "Create a payment",
                "parameters": {
                    "amount": {"type": "integer", "description": "Amount in cents", "required": True},
                    "currency": {"type": "string", "description": "Currency code", "required": True},
                    "source_id": {"type": "string", "description": "Payment source (card nonce)", "required": True},
                    "customer_id": {"type": "string", "description": "Customer ID", "required": False},
                },
            },
            "get_payment": {
                "description": "Get payment details",
                "parameters": {
                    "payment_id": {"type": "string", "description": "Payment ID", "required": True},
                },
            },
            "list_payments": {
                "description": "List payments",
                "parameters": {
                    "begin_time": {"type": "string", "description": "Start time (RFC 3339)", "required": False},
                    "end_time": {"type": "string", "description": "End time (RFC 3339)", "required": False},
                },
            },
            "refund_payment": {
                "description": "Refund a payment",
                "parameters": {
                    "payment_id": {"type": "string", "description": "Payment ID", "required": True},
                    "amount": {"type": "integer", "description": "Refund amount in cents", "required": True},
                    "currency": {"type": "string", "description": "Currency", "required": True},
                    "reason": {"type": "string", "description": "Refund reason", "required": False},
                },
            },
            "create_customer": {
                "description": "Create a customer",
                "parameters": {
                    "email_address": {"type": "string", "description": "Email", "required": False},
                    "given_name": {"type": "string", "description": "First name", "required": False},
                    "family_name": {"type": "string", "description": "Last name", "required": False},
                    "phone_number": {"type": "string", "description": "Phone", "required": False},
                },
            },
            "get_customer": {
                "description": "Get customer details",
                "parameters": {
                    "customer_id": {"type": "string", "description": "Customer ID", "required": True},
                },
            },
            "search_customers": {
                "description": "Search customers",
                "parameters": {
                    "email_address": {"type": "string", "description": "Search by email", "required": False},
                    "phone_number": {"type": "string", "description": "Search by phone", "required": False},
                },
            },
            "create_invoice": {
                "description": "Create an invoice",
                "parameters": {
                    "customer_id": {"type": "string", "description": "Customer ID", "required": True},
                    "line_items": {"type": "array", "description": "Invoice line items", "required": True},
                },
            },
            "publish_invoice": {
                "description": "Publish a draft invoice",
                "parameters": {
                    "invoice_id": {"type": "string", "description": "Invoice ID", "required": True},
                    "version": {"type": "integer", "description": "Invoice version", "required": True},
                },
            },
            "list_catalog": {
                "description": "List catalog items",
                "parameters": {
                    "types": {"type": "array", "description": "Object types to list", "required": False},
                },
            },
            "create_catalog_item": {
                "description": "Create a catalog item",
                "parameters": {
                    "name": {"type": "string", "description": "Item name", "required": True},
                    "description": {"type": "string", "description": "Description", "required": False},
                    "amount": {"type": "integer", "description": "Price in cents", "required": True},
                    "currency": {"type": "string", "description": "Currency", "required": True},
                },
            },
        }

    async def execute(self, action: str, params: dict[str, Any]) -> ConnectorResult:
        try:
            if action == "create_payment":
                return await self._create_payment(params)
            elif action == "get_payment":
                return await self._get_payment(params["payment_id"])
            elif action == "list_payments":
                return await self._list_payments(params.get("begin_time"), params.get("end_time"))
            elif action == "refund_payment":
                return await self._refund_payment(params)
            elif action == "create_customer":
                return await self._create_customer(params)
            elif action == "get_customer":
                return await self._get_customer(params["customer_id"])
            elif action == "search_customers":
                return await self._search_customers(params)
            elif action == "create_invoice":
                return await self._create_invoice(params)
            elif action == "publish_invoice":
                return await self._publish_invoice(params["invoice_id"], params["version"])
            elif action == "list_catalog":
                return await self._list_catalog(params.get("types"))
            elif action == "create_catalog_item":
                return await self._create_catalog_item(params)
            else:
                return ConnectorResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ConnectorResult(success=False, error=str(e))

    async def _create_payment(self, params: dict) -> ConnectorResult:
        import uuid
        body = {
            "idempotency_key": str(uuid.uuid4()),
            "source_id": params["source_id"],
            "amount_money": {
                "amount": params["amount"],
                "currency": params["currency"],
            },
            "location_id": self.location_id,
        }
        if params.get("customer_id"):
            body["customer_id"] = params["customer_id"]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/v2/payments",
                headers=self._headers(),
                json=body,
            )
            response.raise_for_status()
            result = response.json()
            payment = result.get("payment", {})
            return ConnectorResult(success=True, data={
                "id": payment.get("id"),
                "status": payment.get("status"),
            })

    async def _get_payment(self, payment_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/v2/payments/{payment_id}",
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data=result.get("payment", {}))

    async def _list_payments(self, begin_time: str | None, end_time: str | None) -> ConnectorResult:
        params = {"location_id": self.location_id}
        if begin_time:
            params["begin_time"] = begin_time
        if end_time:
            params["end_time"] = end_time

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/v2/payments",
                headers=self._headers(),
                params=params,
            )
            response.raise_for_status()
            result = response.json()
            payments = [{"id": p["id"], "status": p["status"], "amount": p.get("amount_money")} for p in result.get("payments", [])]
            return ConnectorResult(success=True, data={"payments": payments})

    async def _refund_payment(self, params: dict) -> ConnectorResult:
        import uuid
        body = {
            "idempotency_key": str(uuid.uuid4()),
            "payment_id": params["payment_id"],
            "amount_money": {
                "amount": params["amount"],
                "currency": params["currency"],
            },
        }
        if params.get("reason"):
            body["reason"] = params["reason"]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/v2/refunds",
                headers=self._headers(),
                json=body,
            )
            response.raise_for_status()
            result = response.json()
            refund = result.get("refund", {})
            return ConnectorResult(success=True, data={"id": refund.get("id"), "status": refund.get("status")})

    async def _create_customer(self, params: dict) -> ConnectorResult:
        import uuid
        body = {"idempotency_key": str(uuid.uuid4())}
        for field in ["email_address", "given_name", "family_name", "phone_number"]:
            if params.get(field):
                body[field] = params[field]

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/v2/customers",
                headers=self._headers(),
                json=body,
            )
            response.raise_for_status()
            result = response.json()
            customer = result.get("customer", {})
            return ConnectorResult(success=True, data={"id": customer.get("id")})

    async def _get_customer(self, customer_id: str) -> ConnectorResult:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/v2/customers/{customer_id}",
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
            return ConnectorResult(success=True, data=result.get("customer", {}))

    async def _search_customers(self, params: dict) -> ConnectorResult:
        query = {"filter": {}}
        if params.get("email_address"):
            query["filter"]["email_address"] = {"exact": params["email_address"]}
        if params.get("phone_number"):
            query["filter"]["phone_number"] = {"exact": params["phone_number"]}

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/v2/customers/search",
                headers=self._headers(),
                json={"query": query},
            )
            response.raise_for_status()
            result = response.json()
            customers = [{"id": c["id"], "email": c.get("email_address")} for c in result.get("customers", [])]
            return ConnectorResult(success=True, data={"customers": customers})

    async def _create_invoice(self, params: dict) -> ConnectorResult:
        import uuid
        body = {
            "idempotency_key": str(uuid.uuid4()),
            "invoice": {
                "location_id": self.location_id,
                "order_id": None,  # Would need to create order first
                "primary_recipient": {
                    "customer_id": params["customer_id"],
                },
                "payment_requests": [{
                    "request_type": "BALANCE",
                    "due_date": "2024-12-31",
                }],
            },
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/v2/invoices",
                headers=self._headers(),
                json=body,
            )
            response.raise_for_status()
            result = response.json()
            invoice = result.get("invoice", {})
            return ConnectorResult(success=True, data={"id": invoice.get("id"), "version": invoice.get("version")})

    async def _publish_invoice(self, invoice_id: str, version: int) -> ConnectorResult:
        import uuid
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/v2/invoices/{invoice_id}/publish",
                headers=self._headers(),
                json={"idempotency_key": str(uuid.uuid4()), "version": version},
            )
            response.raise_for_status()
            result = response.json()
            invoice = result.get("invoice", {})
            return ConnectorResult(success=True, data={"id": invoice.get("id"), "status": invoice.get("status")})

    async def _list_catalog(self, types: list | None) -> ConnectorResult:
        params = {}
        if types:
            params["types"] = ",".join(types)

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/v2/catalog/list",
                headers=self._headers(),
                params=params,
            )
            response.raise_for_status()
            result = response.json()
            objects = [{"id": o["id"], "type": o["type"]} for o in result.get("objects", [])]
            return ConnectorResult(success=True, data={"objects": objects})

    async def _create_catalog_item(self, params: dict) -> ConnectorResult:
        import uuid
        item_id = f"#{uuid.uuid4().hex[:8]}"
        body = {
            "idempotency_key": str(uuid.uuid4()),
            "object": {
                "type": "ITEM",
                "id": item_id,
                "item_data": {
                    "name": params["name"],
                    "description": params.get("description", ""),
                    "variations": [{
                        "type": "ITEM_VARIATION",
                        "id": f"{item_id}_var",
                        "item_variation_data": {
                            "item_id": item_id,
                            "name": "Regular",
                            "pricing_type": "FIXED_PRICING",
                            "price_money": {
                                "amount": params["amount"],
                                "currency": params["currency"],
                            },
                        },
                    }],
                },
            },
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/v2/catalog/object",
                headers=self._headers(),
                json=body,
            )
            response.raise_for_status()
            result = response.json()
            obj = result.get("catalog_object", {})
            return ConnectorResult(success=True, data={"id": obj.get("id")})
