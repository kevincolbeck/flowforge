"""
Code Generator for API Integrations

Generates working Python code for API connectors based on
APISpec and integration plans.
"""

import json
import re
from dataclasses import dataclass
from typing import Any

from anthropic import AsyncAnthropic

from .api_parser import APISpec, AuthConfig, AuthType, Endpoint, ParameterLocation
from .llm_engine import IntegrationPlan


@dataclass
class GeneratedConnector:
    """A generated connector with code and metadata."""

    name: str
    code: str
    dependencies: list[str]
    auth_requirements: list[str]
    description: str

    def get_full_code(self) -> str:
        """Get the complete code with imports."""
        imports = [
            "import httpx",
            "import json",
            "from typing import Any",
            "from datetime import datetime",
        ]
        if "async def" in self.code:
            imports.append("import asyncio")

        return "\n".join(imports) + "\n\n" + self.code


class CodeGenerator:
    """
    Generates working integration code from API specs and plans.

    Can generate:
    1. Individual API client classes
    2. Integration connectors between two APIs
    3. Complete workflow executors
    """

    def __init__(self, api_key: str | None = None):
        self.client = AsyncAnthropic(api_key=api_key) if api_key else AsyncAnthropic()
        self.model = "claude-sonnet-4-20250514"

    def generate_api_client(self, api_spec: APISpec) -> GeneratedConnector:
        """
        Generate a Python client class for an API.
        """
        class_name = self._to_class_name(api_spec.name)
        code_lines = []

        # Class definition
        code_lines.append(f'class {class_name}Client:')
        code_lines.append(f'    """Client for {api_spec.name} API."""')
        code_lines.append(f'')
        code_lines.append(f'    BASE_URL = "{api_spec.base_url}"')
        code_lines.append(f'')

        # Constructor
        code_lines.append(self._generate_init(api_spec))

        # Auth method
        code_lines.append(self._generate_auth_method(api_spec))

        # Generate method for each endpoint
        for endpoint in api_spec.endpoints:
            code_lines.append(self._generate_endpoint_method(endpoint, api_spec))

        code = "\n".join(code_lines)

        return GeneratedConnector(
            name=f"{class_name}Client",
            code=code,
            dependencies=["httpx"],
            auth_requirements=self._get_auth_requirements(api_spec),
            description=f"Auto-generated client for {api_spec.name}",
        )

    def _generate_init(self, api_spec: APISpec) -> str:
        """Generate __init__ method."""
        lines = []

        auth = api_spec.auth
        if not auth or auth.auth_type == AuthType.NONE:
            lines.append("    def __init__(self, base_url: str | None = None):")
            lines.append("        self.base_url = base_url or self.BASE_URL")
            lines.append("        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=30.0)")
        elif auth.auth_type == AuthType.API_KEY:
            lines.append(f'    def __init__(self, api_key: str, base_url: str | None = None):')
            lines.append("        self.base_url = base_url or self.BASE_URL")
            lines.append("        self.api_key = api_key")
            lines.append("        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=30.0)")
        elif auth.auth_type == AuthType.BEARER:
            lines.append('    def __init__(self, access_token: str, base_url: str | None = None):')
            lines.append("        self.base_url = base_url or self.BASE_URL")
            lines.append("        self.access_token = access_token")
            lines.append("        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=30.0)")
        elif auth.auth_type == AuthType.OAUTH2:
            lines.append('    def __init__(self, client_id: str, client_secret: str, access_token: str | None = None, base_url: str | None = None):')
            lines.append("        self.base_url = base_url or self.BASE_URL")
            lines.append("        self.client_id = client_id")
            lines.append("        self.client_secret = client_secret")
            lines.append("        self.access_token = access_token")
            lines.append("        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=30.0)")
        elif auth.auth_type == AuthType.BASIC:
            lines.append('    def __init__(self, username: str, password: str, base_url: str | None = None):')
            lines.append("        self.base_url = base_url or self.BASE_URL")
            lines.append("        self.username = username")
            lines.append("        self.password = password")
            lines.append("        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=30.0)")
        else:
            lines.append("    def __init__(self, credentials: dict[str, Any], base_url: str | None = None):")
            lines.append("        self.base_url = base_url or self.BASE_URL")
            lines.append("        self.credentials = credentials")
            lines.append("        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=30.0)")

        lines.append("")
        lines.append("    async def close(self):")
        lines.append("        await self.client.aclose()")
        lines.append("")

        return "\n".join(lines)

    def _generate_auth_method(self, api_spec: APISpec) -> str:
        """Generate method to apply authentication to requests."""
        lines = ["    def _get_auth_headers(self) -> dict[str, str]:"]
        lines.append('        """Get authentication headers."""')

        auth = api_spec.auth
        if not auth or auth.auth_type == AuthType.NONE:
            lines.append("        return {}")
        elif auth.auth_type == AuthType.API_KEY:
            if auth.key_location == "header":
                key_name = auth.key_name or "X-API-Key"
                lines.append(f'        return {{"{key_name}": self.api_key}}')
            else:
                lines.append("        return {}  # API key sent as query param")
        elif auth.auth_type == AuthType.BEARER:
            lines.append('        return {"Authorization": f"Bearer {self.access_token}"}')
        elif auth.auth_type == AuthType.OAUTH2:
            lines.append('        if self.access_token:')
            lines.append('            return {"Authorization": f"Bearer {self.access_token}"}')
            lines.append('        return {}')
        elif auth.auth_type == AuthType.BASIC:
            lines.append("        import base64")
            lines.append('        credentials = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()')
            lines.append('        return {"Authorization": f"Basic {credentials}"}')
        else:
            lines.append("        return self.credentials.get('headers', {})")

        lines.append("")

        # Add query param auth method if needed
        if auth and auth.auth_type == AuthType.API_KEY and auth.key_location == "query":
            lines.append("    def _get_auth_params(self) -> dict[str, str]:")
            lines.append('        """Get authentication query parameters."""')
            key_name = auth.key_name or "api_key"
            lines.append(f'        return {{"{key_name}": self.api_key}}')
            lines.append("")

        return "\n".join(lines)

    def _generate_endpoint_method(self, endpoint: Endpoint, api_spec: APISpec) -> str:
        """Generate a method for an API endpoint."""
        method_name = self._to_method_name(endpoint)
        lines = []

        # Build parameter list
        params = []
        for param in endpoint.parameters:
            if param.location != ParameterLocation.BODY:
                default = f" = None" if not param.required else ""
                params.append(f"{self._safe_param_name(param.name)}: {self._python_type(param.param_type)}{default}")

        # Add body parameter if needed
        if endpoint.request_body_schema or endpoint.method in ["POST", "PUT", "PATCH"]:
            params.append("body: dict[str, Any] | None = None")

        param_str = ", ".join(params)
        if param_str:
            param_str = ", " + param_str

        # Method signature
        lines.append(f"    async def {method_name}(self{param_str}) -> dict[str, Any]:")

        # Docstring
        doc = endpoint.summary or endpoint.description or f"{endpoint.method} {endpoint.path}"
        lines.append(f'        """{doc}"""')

        # Build URL with path parameters
        path = endpoint.path
        path_params = [p for p in endpoint.parameters if p.location == ParameterLocation.PATH]
        if path_params:
            for param in path_params:
                path = path.replace(f"{{{param.name}}}", f"{{{self._safe_param_name(param.name)}}}")
            lines.append(f'        url = f"{path}"')
        else:
            lines.append(f'        url = "{path}"')

        # Query parameters
        query_params = [p for p in endpoint.parameters if p.location == ParameterLocation.QUERY]
        if query_params:
            lines.append("        params = {}")
            for param in query_params:
                safe_name = self._safe_param_name(param.name)
                lines.append(f'        if {safe_name} is not None:')
                lines.append(f'            params["{param.name}"] = {safe_name}')

            # Add auth params if needed
            auth = api_spec.auth
            if auth and auth.auth_type == AuthType.API_KEY and auth.key_location == "query":
                lines.append("        params.update(self._get_auth_params())")
        else:
            auth = api_spec.auth
            if auth and auth.auth_type == AuthType.API_KEY and auth.key_location == "query":
                lines.append("        params = self._get_auth_params()")
            else:
                lines.append("        params = {}")

        # Headers
        header_params = [p for p in endpoint.parameters if p.location == ParameterLocation.HEADER]
        lines.append("        headers = self._get_auth_headers()")
        for param in header_params:
            safe_name = self._safe_param_name(param.name)
            lines.append(f'        if {safe_name} is not None:')
            lines.append(f'            headers["{param.name}"] = {safe_name}')

        # Make request
        method_lower = endpoint.method.lower()
        if endpoint.method in ["POST", "PUT", "PATCH"]:
            lines.append(f"        response = await self.client.{method_lower}(url, params=params, headers=headers, json=body)")
        else:
            lines.append(f"        response = await self.client.{method_lower}(url, params=params, headers=headers)")

        lines.append("        response.raise_for_status()")
        lines.append("        return response.json() if response.content else {}")
        lines.append("")

        return "\n".join(lines)

    async def generate_integration(
        self,
        plan: IntegrationPlan,
        source_api: APISpec,
        target_api: APISpec,
    ) -> GeneratedConnector:
        """
        Generate a complete integration connector from a plan.

        Uses LLM to generate the transformation and orchestration logic.
        """
        prompt = f"""Generate a Python async function that integrates these two APIs.

INTEGRATION PLAN:
Description: {plan.description}
Steps: {json.dumps(plan.steps, indent=2)}
Data Mapping: {json.dumps(plan.data_mapping, indent=2)}
Transformations: {json.dumps(plan.transformations, indent=2)}

SOURCE API: {source_api.name}
- Endpoint: {plan.source_endpoint.method} {plan.source_endpoint.path}
- Response Schema: {json.dumps(plan.source_endpoint.response_schema, indent=2) if plan.source_endpoint.response_schema else 'Not specified'}

TARGET API: {target_api.name}
- Endpoint: {plan.target_endpoint.method} {plan.target_endpoint.path}
- Request Schema: {json.dumps(plan.target_endpoint.request_body_schema, indent=2) if plan.target_endpoint.request_body_schema else 'Not specified'}

Generate a Python async function with this signature:
async def execute_integration(
    source_client: Any,  # Source API client
    target_client: Any,  # Target API client
    trigger_data: dict[str, Any] | None = None,  # Data that triggered the integration
) -> dict[str, Any]:
    '''Execute the integration and return the result.'''
    ...

The function should:
1. Call the source API endpoint if needed
2. Transform the data according to the mapping
3. Call the target API endpoint
4. Return a summary of what was done

Include error handling and logging.

Return ONLY the Python code, no explanations."""

        response = await self.client.messages.create(
            model=self.model,
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )

        code = response.content[0].text
        if "```python" in code:
            code = code.split("```python")[1].split("```")[0]
        elif "```" in code:
            code = code.split("```")[1].split("```")[0]

        return GeneratedConnector(
            name=f"integrate_{self._to_snake_case(source_api.name)}_to_{self._to_snake_case(target_api.name)}",
            code=code.strip(),
            dependencies=["httpx"],
            auth_requirements=self._get_auth_requirements(source_api) + self._get_auth_requirements(target_api),
            description=plan.description,
        )

    async def generate_workflow_executor(
        self,
        workflow_definition: dict[str, Any],
        api_specs: dict[str, APISpec],
    ) -> GeneratedConnector:
        """
        Generate a complete workflow executor from a workflow definition.
        """
        # Build context about available APIs
        api_context = ""
        for service_name, spec in api_specs.items():
            api_context += f"\n{service_name}:\n"
            api_context += f"  Base URL: {spec.base_url}\n"
            api_context += f"  Auth: {spec.auth.auth_type.value if spec.auth else 'none'}\n"
            api_context += "  Key Endpoints:\n"
            for ep in spec.endpoints[:10]:
                api_context += f"    - {ep.method} {ep.path}: {ep.summary[:50] if ep.summary else 'No description'}\n"

        prompt = f"""Generate a Python class that executes this workflow.

WORKFLOW DEFINITION:
{json.dumps(workflow_definition, indent=2)}

AVAILABLE APIS:
{api_context}

Generate a Python class with this structure:
class WorkflowExecutor:
    '''Executes the {workflow_definition.get('name', 'workflow')}.'''

    def __init__(self, credentials: dict[str, dict[str, str]]):
        '''Initialize with credentials for each service.'''
        ...

    async def execute(self, trigger_data: dict[str, Any] | None = None) -> dict[str, Any]:
        '''Execute the workflow and return results.'''
        ...

    async def _execute_step(self, step_id: str, context: dict[str, Any]) -> dict[str, Any]:
        '''Execute a single workflow step.'''
        ...

The class should:
1. Initialize API clients for each required service
2. Execute steps in the correct order (respecting dependencies)
3. Pass data between steps using the context
4. Handle errors gracefully
5. Return a complete execution log

Return ONLY the Python code, no explanations."""

        response = await self.client.messages.create(
            model=self.model,
            max_tokens=8192,
            messages=[{"role": "user", "content": prompt}],
        )

        code = response.content[0].text
        if "```python" in code:
            code = code.split("```python")[1].split("```")[0]
        elif "```" in code:
            code = code.split("```")[1].split("```")[0]

        return GeneratedConnector(
            name="WorkflowExecutor",
            code=code.strip(),
            dependencies=["httpx", "asyncio"],
            auth_requirements=list(workflow_definition.get("required_connections", [])),
            description=f"Executor for workflow: {workflow_definition.get('name', 'Unknown')}",
        )

    def _to_class_name(self, name: str) -> str:
        """Convert a name to PascalCase class name."""
        # Remove non-alphanumeric characters and split on spaces/underscores
        words = re.split(r'[\s_\-]+', re.sub(r'[^\w\s\-]', '', name))
        return ''.join(word.capitalize() for word in words if word)

    def _to_snake_case(self, name: str) -> str:
        """Convert a name to snake_case."""
        name = re.sub(r'[^\w\s\-]', '', name)
        name = re.sub(r'[\s\-]+', '_', name)
        name = re.sub(r'([A-Z])', r'_\1', name)
        return name.lower().strip('_')

    def _to_method_name(self, endpoint: Endpoint) -> str:
        """Generate a method name from an endpoint."""
        if endpoint.operation_id:
            return self._to_snake_case(endpoint.operation_id)

        # Generate from method + path
        parts = [endpoint.method.lower()]
        path_parts = endpoint.path.strip('/').split('/')

        for part in path_parts:
            if part.startswith('{') and part.endswith('}'):
                parts.append('by')
                parts.append(part[1:-1])
            else:
                parts.append(part)

        return '_'.join(parts)

    def _safe_param_name(self, name: str) -> str:
        """Convert parameter name to valid Python identifier."""
        # Replace hyphens and dots with underscores
        name = name.replace('-', '_').replace('.', '_')
        # Prefix with underscore if starts with digit
        if name and name[0].isdigit():
            name = '_' + name
        # Handle Python reserved words
        reserved = {'from', 'import', 'class', 'def', 'return', 'if', 'else', 'for', 'while', 'type', 'id', 'list', 'dict'}
        if name in reserved:
            name = name + '_'
        return name

    def _python_type(self, api_type: str) -> str:
        """Convert API type to Python type hint."""
        type_map = {
            'string': 'str',
            'integer': 'int',
            'number': 'float',
            'boolean': 'bool',
            'array': 'list',
            'object': 'dict[str, Any]',
        }
        return type_map.get(api_type.lower(), 'Any')

    def _get_auth_requirements(self, api_spec: APISpec) -> list[str]:
        """Get list of credentials required for an API."""
        auth = api_spec.auth
        if not auth or auth.auth_type == AuthType.NONE:
            return []
        elif auth.auth_type == AuthType.API_KEY:
            return [f"{api_spec.name}_api_key"]
        elif auth.auth_type == AuthType.BEARER:
            return [f"{api_spec.name}_access_token"]
        elif auth.auth_type == AuthType.OAUTH2:
            return [f"{api_spec.name}_client_id", f"{api_spec.name}_client_secret"]
        elif auth.auth_type == AuthType.BASIC:
            return [f"{api_spec.name}_username", f"{api_spec.name}_password"]
        else:
            return [f"{api_spec.name}_credentials"]
