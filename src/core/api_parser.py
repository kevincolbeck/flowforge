"""
API Documentation Parser

Parses OpenAPI specs, REST API docs, and raw documentation
to extract structured API information.
"""

import json
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import httpx
import yaml


class AuthType(Enum):
    NONE = "none"
    API_KEY = "api_key"
    BEARER = "bearer"
    OAUTH2 = "oauth2"
    BASIC = "basic"
    CUSTOM = "custom"


class ParameterLocation(Enum):
    QUERY = "query"
    HEADER = "header"
    PATH = "path"
    BODY = "body"
    COOKIE = "cookie"


@dataclass
class Parameter:
    name: str
    location: ParameterLocation
    required: bool = False
    param_type: str = "string"
    description: str = ""
    default: Any = None
    example: Any = None
    enum: list[str] | None = None


@dataclass
class AuthConfig:
    auth_type: AuthType
    key_name: str | None = None  # For API key: header/query param name
    key_location: str | None = None  # "header" or "query"
    oauth_config: dict[str, Any] | None = None  # OAuth2 endpoints, scopes
    description: str = ""


@dataclass
class Endpoint:
    path: str
    method: str
    operation_id: str | None = None
    summary: str = ""
    description: str = ""
    parameters: list[Parameter] = field(default_factory=list)
    request_body_schema: dict[str, Any] | None = None
    response_schema: dict[str, Any] | None = None
    tags: list[str] = field(default_factory=list)

    @property
    def unique_id(self) -> str:
        return self.operation_id or f"{self.method}_{self.path}".replace("/", "_").replace("{", "").replace("}", "")


@dataclass
class APISpec:
    name: str
    base_url: str
    version: str = ""
    description: str = ""
    auth: AuthConfig | None = None
    endpoints: list[Endpoint] = field(default_factory=list)
    schemas: dict[str, Any] = field(default_factory=dict)
    raw_spec: dict[str, Any] | None = None

    def get_endpoint(self, operation_id: str) -> Endpoint | None:
        for endpoint in self.endpoints:
            if endpoint.operation_id == operation_id or endpoint.unique_id == operation_id:
                return endpoint
        return None

    def find_endpoints(self, query: str) -> list[Endpoint]:
        """Find endpoints matching a search query."""
        query_lower = query.lower()
        results = []
        for endpoint in self.endpoints:
            if (query_lower in endpoint.path.lower() or
                query_lower in endpoint.summary.lower() or
                query_lower in endpoint.description.lower() or
                any(query_lower in tag.lower() for tag in endpoint.tags)):
                results.append(endpoint)
        return results


class APIParser:
    """Parses various API documentation formats into a unified APISpec."""

    def __init__(self):
        self.http_client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)

    async def close(self):
        await self.http_client.aclose()

    async def parse_from_url(self, url: str) -> APISpec:
        """Fetch and parse API spec from a URL."""
        response = await self.http_client.get(url)
        response.raise_for_status()

        content = response.text
        content_type = response.headers.get("content-type", "")

        # Try to parse as JSON first
        if "json" in content_type or content.strip().startswith("{"):
            try:
                spec_dict = json.loads(content)
                return self._parse_openapi(spec_dict, url)
            except json.JSONDecodeError:
                pass

        # Try YAML
        if "yaml" in content_type or "yml" in url:
            try:
                spec_dict = yaml.safe_load(content)
                if isinstance(spec_dict, dict):
                    return self._parse_openapi(spec_dict, url)
            except yaml.YAMLError:
                pass

        # Fall back to LLM parsing for non-standard docs
        raise ValueError(
            f"Could not parse API spec from {url}. "
            "Use parse_with_llm() for non-standard documentation."
        )

    def parse_from_dict(self, spec_dict: dict[str, Any], source: str = "") -> APISpec:
        """Parse an API spec from a dictionary (OpenAPI format)."""
        return self._parse_openapi(spec_dict, source)

    def parse_from_string(self, content: str, source: str = "") -> APISpec:
        """Parse API spec from a string (JSON or YAML)."""
        # Try JSON
        try:
            spec_dict = json.loads(content)
            return self._parse_openapi(spec_dict, source)
        except json.JSONDecodeError:
            pass

        # Try YAML
        try:
            spec_dict = yaml.safe_load(content)
            if isinstance(spec_dict, dict):
                return self._parse_openapi(spec_dict, source)
        except yaml.YAMLError:
            pass

        raise ValueError("Could not parse content as JSON or YAML")

    def _parse_openapi(self, spec: dict[str, Any], source: str) -> APISpec:
        """Parse OpenAPI 2.0 (Swagger) or 3.x specification."""
        # Detect version
        is_v3 = spec.get("openapi", "").startswith("3.")
        is_v2 = "swagger" in spec and spec.get("swagger", "").startswith("2.")

        if not is_v3 and not is_v2:
            # Try to parse anyway, might be a partial spec
            pass

        # Extract base info
        info = spec.get("info", {})
        name = info.get("title", "Unknown API")
        version = info.get("version", "")
        description = info.get("description", "")

        # Extract base URL
        base_url = self._extract_base_url(spec, source)

        # Extract auth configuration
        auth = self._extract_auth(spec)

        # Extract endpoints
        endpoints = self._extract_endpoints(spec, is_v3)

        # Extract schemas
        schemas = self._extract_schemas(spec, is_v3)

        return APISpec(
            name=name,
            base_url=base_url,
            version=version,
            description=description,
            auth=auth,
            endpoints=endpoints,
            schemas=schemas,
            raw_spec=spec,
        )

    def _extract_base_url(self, spec: dict[str, Any], source: str) -> str:
        """Extract base URL from spec."""
        # OpenAPI 3.x
        servers = spec.get("servers", [])
        if servers and isinstance(servers, list):
            return servers[0].get("url", "")

        # Swagger 2.x
        host = spec.get("host", "")
        base_path = spec.get("basePath", "")
        schemes = spec.get("schemes", ["https"])
        if host:
            scheme = schemes[0] if schemes else "https"
            return f"{scheme}://{host}{base_path}"

        # Try to infer from source URL
        if source.startswith("http"):
            # Remove spec file path to get base
            return re.sub(r"/[^/]*\.(json|yaml|yml)$", "", source)

        return ""

    def _extract_auth(self, spec: dict[str, Any]) -> AuthConfig | None:
        """Extract authentication configuration."""
        # OpenAPI 3.x
        components = spec.get("components", {})
        security_schemes = components.get("securitySchemes", {})

        # Swagger 2.x
        if not security_schemes:
            security_schemes = spec.get("securityDefinitions", {})

        if not security_schemes:
            return None

        # Take the first security scheme (could be enhanced to support multiple)
        for name, scheme in security_schemes.items():
            scheme_type = scheme.get("type", "").lower()

            if scheme_type == "apikey":
                return AuthConfig(
                    auth_type=AuthType.API_KEY,
                    key_name=scheme.get("name"),
                    key_location=scheme.get("in", "header"),
                    description=scheme.get("description", ""),
                )
            elif scheme_type == "http":
                http_scheme = scheme.get("scheme", "").lower()
                if http_scheme == "bearer":
                    return AuthConfig(
                        auth_type=AuthType.BEARER,
                        description=scheme.get("description", ""),
                    )
                elif http_scheme == "basic":
                    return AuthConfig(
                        auth_type=AuthType.BASIC,
                        description=scheme.get("description", ""),
                    )
            elif scheme_type == "oauth2":
                flows = scheme.get("flows", scheme.get("flow", {}))
                return AuthConfig(
                    auth_type=AuthType.OAUTH2,
                    oauth_config={
                        "flows": flows,
                        "scopes": self._extract_oauth_scopes(flows),
                    },
                    description=scheme.get("description", ""),
                )
            elif scheme_type == "bearer":  # Some specs use this directly
                return AuthConfig(
                    auth_type=AuthType.BEARER,
                    description=scheme.get("description", ""),
                )

        return None

    def _extract_oauth_scopes(self, flows: dict[str, Any]) -> dict[str, str]:
        """Extract OAuth scopes from flows configuration."""
        scopes = {}
        if isinstance(flows, dict):
            for flow_name, flow_config in flows.items():
                if isinstance(flow_config, dict) and "scopes" in flow_config:
                    scopes.update(flow_config["scopes"])
        return scopes

    def _extract_endpoints(self, spec: dict[str, Any], is_v3: bool) -> list[Endpoint]:
        """Extract all endpoints from the spec."""
        endpoints = []
        paths = spec.get("paths", {})

        for path, path_item in paths.items():
            if not isinstance(path_item, dict):
                continue

            # Path-level parameters
            path_params = path_item.get("parameters", [])

            for method in ["get", "post", "put", "patch", "delete", "head", "options"]:
                if method not in path_item:
                    continue

                operation = path_item[method]
                if not isinstance(operation, dict):
                    continue

                # Combine path and operation parameters
                all_params = path_params + operation.get("parameters", [])
                parameters = [self._parse_parameter(p, is_v3) for p in all_params]

                # Extract request body (OpenAPI 3.x)
                request_body_schema = None
                if is_v3 and "requestBody" in operation:
                    request_body = operation["requestBody"]
                    content = request_body.get("content", {})
                    json_content = content.get("application/json", {})
                    request_body_schema = json_content.get("schema")

                # Swagger 2.x: body parameter
                if not is_v3:
                    for param in all_params:
                        if param.get("in") == "body":
                            request_body_schema = param.get("schema")
                            break

                # Extract response schema
                response_schema = self._extract_response_schema(operation, is_v3)

                endpoint = Endpoint(
                    path=path,
                    method=method.upper(),
                    operation_id=operation.get("operationId"),
                    summary=operation.get("summary", ""),
                    description=operation.get("description", ""),
                    parameters=parameters,
                    request_body_schema=request_body_schema,
                    response_schema=response_schema,
                    tags=operation.get("tags", []),
                )
                endpoints.append(endpoint)

        return endpoints

    def _parse_parameter(self, param: dict[str, Any], is_v3: bool) -> Parameter:
        """Parse a single parameter definition."""
        location_str = param.get("in", "query")
        try:
            location = ParameterLocation(location_str)
        except ValueError:
            location = ParameterLocation.QUERY

        # Type extraction differs between v2 and v3
        if is_v3:
            schema = param.get("schema", {})
            param_type = schema.get("type", "string")
            default = schema.get("default")
            example = param.get("example") or schema.get("example")
            enum = schema.get("enum")
        else:
            param_type = param.get("type", "string")
            default = param.get("default")
            example = param.get("x-example") or param.get("example")
            enum = param.get("enum")

        return Parameter(
            name=param.get("name", ""),
            location=location,
            required=param.get("required", False),
            param_type=param_type,
            description=param.get("description", ""),
            default=default,
            example=example,
            enum=enum,
        )

    def _extract_response_schema(self, operation: dict[str, Any], is_v3: bool) -> dict[str, Any] | None:
        """Extract the primary response schema."""
        responses = operation.get("responses", {})

        # Look for 200, 201, or default response
        for status in ["200", "201", "default"]:
            if status not in responses:
                continue

            response = responses[status]

            if is_v3:
                content = response.get("content", {})
                json_content = content.get("application/json", {})
                if "schema" in json_content:
                    return json_content["schema"]
            else:
                if "schema" in response:
                    return response["schema"]

        return None

    def _extract_schemas(self, spec: dict[str, Any], is_v3: bool) -> dict[str, Any]:
        """Extract schema definitions."""
        if is_v3:
            components = spec.get("components", {})
            return components.get("schemas", {})
        else:
            return spec.get("definitions", {})
