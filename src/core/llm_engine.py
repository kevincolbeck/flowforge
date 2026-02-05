"""
LLM Engine for API Understanding and Integration Planning

Uses Claude to understand API documentation, plan integrations,
and generate working connector code.
"""

import json
from dataclasses import dataclass
from typing import Any

from anthropic import AsyncAnthropic

from .api_parser import APISpec, AuthType, Endpoint


@dataclass
class IntegrationPlan:
    """A plan for integrating two APIs."""

    source_api: str
    source_endpoint: Endpoint
    target_api: str
    target_endpoint: Endpoint
    data_mapping: dict[str, str]  # source_field -> target_field
    transformations: list[dict[str, Any]]  # any transformations needed
    description: str
    steps: list[str]


@dataclass
class APIUnderstanding:
    """LLM's understanding of an API from raw documentation."""

    name: str
    base_url: str
    description: str
    auth_type: str
    auth_instructions: str
    endpoints: list[dict[str, Any]]
    example_usage: str


class LLMEngine:
    """
    LLM-powered engine for understanding APIs and generating integrations.

    This is the core innovation - using LLMs to:
    1. Parse non-standard API documentation
    2. Understand what an API does in natural language
    3. Figure out how to connect two APIs
    4. Generate working integration code
    """

    def __init__(self, api_key: str | None = None):
        self.client = AsyncAnthropic(api_key=api_key) if api_key else AsyncAnthropic()
        self.model = "claude-sonnet-4-20250514"

    async def understand_api_from_docs(self, documentation: str, url_hint: str = "") -> APIUnderstanding:
        """
        Parse raw API documentation (any format) into structured understanding.

        This handles documentation that isn't in OpenAPI format - HTML pages,
        markdown docs, or even just example code.
        """
        prompt = f"""Analyze this API documentation and extract structured information.

Documentation URL hint: {url_hint}

Documentation content:
{documentation[:50000]}  # Limit to avoid token limits

Extract and return a JSON object with:
{{
    "name": "Name of the API",
    "base_url": "Base URL for API calls",
    "description": "What this API does",
    "auth_type": "none|api_key|bearer|oauth2|basic",
    "auth_instructions": "How to authenticate",
    "endpoints": [
        {{
            "path": "/path/to/endpoint",
            "method": "GET|POST|etc",
            "description": "What it does",
            "parameters": [
                {{"name": "param", "in": "query|path|body|header", "required": true, "type": "string", "description": "..."}}
            ],
            "request_body_example": {{}},
            "response_example": {{}}
        }}
    ],
    "example_usage": "A curl or code example showing basic usage"
}}

Return ONLY valid JSON, no other text."""

        response = await self.client.messages.create(
            model=self.model,
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )

        result = response.content[0].text
        # Extract JSON from response (handle markdown code blocks)
        if "```json" in result:
            result = result.split("```json")[1].split("```")[0]
        elif "```" in result:
            result = result.split("```")[1].split("```")[0]

        data = json.loads(result.strip())

        return APIUnderstanding(
            name=data.get("name", "Unknown API"),
            base_url=data.get("base_url", ""),
            description=data.get("description", ""),
            auth_type=data.get("auth_type", "none"),
            auth_instructions=data.get("auth_instructions", ""),
            endpoints=data.get("endpoints", []),
            example_usage=data.get("example_usage", ""),
        )

    async def find_matching_endpoints(
        self,
        intent: str,
        source_api: APISpec,
        target_api: APISpec,
    ) -> list[tuple[Endpoint, Endpoint, str]]:
        """
        Given a user intent, find which endpoints from source and target APIs should be connected.

        Returns list of (source_endpoint, target_endpoint, reasoning) tuples.
        """
        # Build API summaries
        source_summary = self._build_api_summary(source_api)
        target_summary = self._build_api_summary(target_api)

        prompt = f"""Given this user intent for an integration, find the best matching endpoints.

USER INTENT: {intent}

SOURCE API ({source_api.name}):
{source_summary}

TARGET API ({target_api.name}):
{target_summary}

Return a JSON array of endpoint pairs that should be connected to fulfill this intent:
[
    {{
        "source_endpoint_id": "operation_id or METHOD_path",
        "target_endpoint_id": "operation_id or METHOD_path",
        "reasoning": "Why these endpoints should be connected"
    }}
]

Return ONLY valid JSON, no other text."""

        response = await self.client.messages.create(
            model=self.model,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )

        result = response.content[0].text
        if "```json" in result:
            result = result.split("```json")[1].split("```")[0]
        elif "```" in result:
            result = result.split("```")[1].split("```")[0]

        pairs = json.loads(result.strip())
        matches = []

        for pair in pairs:
            source_ep = source_api.get_endpoint(pair["source_endpoint_id"])
            target_ep = target_api.get_endpoint(pair["target_endpoint_id"])
            if source_ep and target_ep:
                matches.append((source_ep, target_ep, pair["reasoning"]))

        return matches

    async def plan_integration(
        self,
        intent: str,
        source_api: APISpec,
        source_endpoint: Endpoint,
        target_api: APISpec,
        target_endpoint: Endpoint,
    ) -> IntegrationPlan:
        """
        Create a detailed integration plan including data mapping.
        """
        prompt = f"""Create an integration plan to connect these two API endpoints.

USER INTENT: {intent}

SOURCE ENDPOINT ({source_api.name}):
- Path: {source_endpoint.method} {source_endpoint.path}
- Description: {source_endpoint.description}
- Response Schema: {json.dumps(source_endpoint.response_schema, indent=2) if source_endpoint.response_schema else 'Not specified'}

TARGET ENDPOINT ({target_api.name}):
- Path: {target_endpoint.method} {target_endpoint.path}
- Description: {target_endpoint.description}
- Parameters: {json.dumps([{"name": p.name, "type": p.param_type, "required": p.required} for p in target_endpoint.parameters], indent=2)}
- Request Body Schema: {json.dumps(target_endpoint.request_body_schema, indent=2) if target_endpoint.request_body_schema else 'Not specified'}

Return a JSON object with the integration plan:
{{
    "description": "Human-readable description of what this integration does",
    "data_mapping": {{
        "source_field_path": "target_field_path"
    }},
    "transformations": [
        {{"type": "rename|convert|format|custom", "source": "field", "target": "field", "details": "..."}}
    ],
    "steps": [
        "Step 1: ...",
        "Step 2: ..."
    ]
}}

Return ONLY valid JSON, no other text."""

        response = await self.client.messages.create(
            model=self.model,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )

        result = response.content[0].text
        if "```json" in result:
            result = result.split("```json")[1].split("```")[0]
        elif "```" in result:
            result = result.split("```")[1].split("```")[0]

        data = json.loads(result.strip())

        return IntegrationPlan(
            source_api=source_api.name,
            source_endpoint=source_endpoint,
            target_api=target_api.name,
            target_endpoint=target_endpoint,
            data_mapping=data.get("data_mapping", {}),
            transformations=data.get("transformations", []),
            description=data.get("description", ""),
            steps=data.get("steps", []),
        )

    async def parse_natural_language_workflow(self, description: str) -> dict[str, Any]:
        """
        Parse a natural language workflow description into structured workflow definition.

        Example: "When a new order comes in on Shopify, add a row to my Google Sheet
                  and send a Slack notification"
        """
        prompt = f"""Parse this natural language workflow description into a structured workflow.

DESCRIPTION: {description}

Return a JSON workflow definition:
{{
    "name": "Generated workflow name",
    "description": "What this workflow does",
    "trigger": {{
        "type": "webhook|schedule|manual",
        "service": "Service name (e.g., Shopify, Stripe)",
        "event": "Event name (e.g., order.created, payment.completed)",
        "schedule": "Cron expression if scheduled"
    }},
    "steps": [
        {{
            "id": "step_1",
            "name": "Step name",
            "service": "Target service name",
            "action": "What to do (e.g., create_row, send_message)",
            "inputs": {{
                "field": "{{{{trigger.data.field}}}}"
            }},
            "depends_on": []
        }}
    ],
    "required_connections": ["service1", "service2"]
}}

Return ONLY valid JSON, no other text."""

        response = await self.client.messages.create(
            model=self.model,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )

        result = response.content[0].text
        if "```json" in result:
            result = result.split("```json")[1].split("```")[0]
        elif "```" in result:
            result = result.split("```")[1].split("```")[0]

        return json.loads(result.strip())

    async def generate_data_transformation(
        self,
        source_data: dict[str, Any],
        target_schema: dict[str, Any],
        context: str = "",
    ) -> str:
        """
        Generate a Python function to transform source data to target schema.
        """
        prompt = f"""Generate a Python function to transform data from source format to target format.

SOURCE DATA EXAMPLE:
{json.dumps(source_data, indent=2)}

TARGET SCHEMA:
{json.dumps(target_schema, indent=2)}

CONTEXT: {context}

Generate a Python function with this signature:
def transform(source_data: dict) -> dict:
    '''Transform source data to target format.'''
    ...

The function should:
1. Handle missing fields gracefully
2. Convert types as needed
3. Apply any necessary formatting
4. Return data matching the target schema

Return ONLY the Python code, no explanations."""

        response = await self.client.messages.create(
            model=self.model,
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )

        result = response.content[0].text
        if "```python" in result:
            result = result.split("```python")[1].split("```")[0]
        elif "```" in result:
            result = result.split("```")[1].split("```")[0]

        return result.strip()

    async def suggest_auth_setup(self, api_spec: APISpec) -> dict[str, Any]:
        """
        Provide guidance on how to set up authentication for an API.
        """
        auth_info = ""
        if api_spec.auth:
            auth_info = f"""
Auth Type: {api_spec.auth.auth_type.value}
Key Name: {api_spec.auth.key_name}
Location: {api_spec.auth.key_location}
Description: {api_spec.auth.description}
OAuth Config: {json.dumps(api_spec.auth.oauth_config) if api_spec.auth.oauth_config else 'N/A'}
"""

        prompt = f"""Provide setup instructions for authenticating with this API.

API: {api_spec.name}
Base URL: {api_spec.base_url}
{auth_info}

Return a JSON object with:
{{
    "auth_type": "api_key|bearer|oauth2|basic|none",
    "setup_steps": [
        "Step 1: ...",
        "Step 2: ..."
    ],
    "required_credentials": [
        {{"name": "credential_name", "description": "what it is", "how_to_get": "instructions"}}
    ],
    "example_header": {{"Authorization": "Bearer ..."}},
    "notes": "Any additional notes"
}}

Return ONLY valid JSON, no other text."""

        response = await self.client.messages.create(
            model=self.model,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )

        result = response.content[0].text
        if "```json" in result:
            result = result.split("```json")[1].split("```")[0]
        elif "```" in result:
            result = result.split("```")[1].split("```")[0]

        return json.loads(result.strip())

    def _build_api_summary(self, api: APISpec) -> str:
        """Build a concise summary of an API for LLM context."""
        lines = [f"Base URL: {api.base_url}", "Endpoints:"]

        for ep in api.endpoints[:30]:  # Limit for context window
            params = ", ".join(p.name for p in ep.parameters[:5])
            lines.append(f"  - {ep.method} {ep.path}: {ep.summary or ep.description[:100]}")
            if params:
                lines.append(f"    Params: {params}")

        return "\n".join(lines)

    async def explain_api(self, api_spec: APISpec) -> str:
        """Generate a human-friendly explanation of what an API does."""
        summary = self._build_api_summary(api_spec)

        prompt = f"""Explain what this API does in simple terms. Who would use it and for what?

API: {api_spec.name}
Description: {api_spec.description}

{summary}

Provide a clear, concise explanation (2-3 paragraphs)."""

        response = await self.client.messages.create(
            model=self.model,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )

        return response.content[0].text

    async def diagnose_integration_error(
        self,
        error_message: str,
        source_api: APISpec,
        target_api: APISpec,
        request_details: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Diagnose why an integration failed and suggest fixes.
        """
        prompt = f"""Diagnose this API integration error and suggest fixes.

ERROR: {error_message}

SOURCE API: {source_api.name}
TARGET API: {target_api.name}

REQUEST DETAILS:
{json.dumps(request_details, indent=2)}

Return a JSON object:
{{
    "diagnosis": "What went wrong",
    "likely_cause": "Most probable cause",
    "suggested_fixes": [
        {{"fix": "description", "confidence": "high|medium|low"}}
    ],
    "code_change": "Suggested code modification if applicable"
}}

Return ONLY valid JSON, no other text."""

        response = await self.client.messages.create(
            model=self.model,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )

        result = response.content[0].text
        if "```json" in result:
            result = result.split("```json")[1].split("```")[0]
        elif "```" in result:
            result = result.split("```")[1].split("```")[0]

        return json.loads(result.strip())
