"""Tests for the API parser."""

import pytest
from src.core.api_parser import APIParser, AuthType


# Sample OpenAPI spec for testing
SAMPLE_OPENAPI_SPEC = {
    "openapi": "3.0.0",
    "info": {
        "title": "Test API",
        "version": "1.0.0",
        "description": "A test API for unit testing"
    },
    "servers": [
        {"url": "https://api.test.com/v1"}
    ],
    "components": {
        "securitySchemes": {
            "bearerAuth": {
                "type": "http",
                "scheme": "bearer"
            }
        }
    },
    "paths": {
        "/users": {
            "get": {
                "operationId": "listUsers",
                "summary": "List all users",
                "parameters": [
                    {
                        "name": "limit",
                        "in": "query",
                        "required": False,
                        "schema": {"type": "integer"}
                    }
                ],
                "responses": {
                    "200": {
                        "description": "Success",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {"$ref": "#/components/schemas/User"}
                                }
                            }
                        }
                    }
                }
            },
            "post": {
                "operationId": "createUser",
                "summary": "Create a new user",
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/User"}
                        }
                    }
                },
                "responses": {
                    "201": {"description": "Created"}
                }
            }
        },
        "/users/{id}": {
            "get": {
                "operationId": "getUser",
                "summary": "Get a user by ID",
                "parameters": [
                    {
                        "name": "id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "string"}
                    }
                ],
                "responses": {
                    "200": {"description": "Success"}
                }
            }
        }
    }
}


def test_parse_openapi_basic():
    """Test parsing a basic OpenAPI spec."""
    parser = APIParser()
    spec = parser.parse_from_dict(SAMPLE_OPENAPI_SPEC)

    assert spec.name == "Test API"
    assert spec.version == "1.0.0"
    assert spec.base_url == "https://api.test.com/v1"


def test_parse_openapi_auth():
    """Test parsing authentication configuration."""
    parser = APIParser()
    spec = parser.parse_from_dict(SAMPLE_OPENAPI_SPEC)

    assert spec.auth is not None
    assert spec.auth.auth_type == AuthType.BEARER


def test_parse_openapi_endpoints():
    """Test parsing endpoints."""
    parser = APIParser()
    spec = parser.parse_from_dict(SAMPLE_OPENAPI_SPEC)

    assert len(spec.endpoints) == 3

    # Check GET /users
    get_users = spec.get_endpoint("listUsers")
    assert get_users is not None
    assert get_users.method == "GET"
    assert get_users.path == "/users"

    # Check POST /users
    create_user = spec.get_endpoint("createUser")
    assert create_user is not None
    assert create_user.method == "POST"

    # Check GET /users/{id}
    get_user = spec.get_endpoint("getUser")
    assert get_user is not None
    assert len(get_user.parameters) == 1
    assert get_user.parameters[0].name == "id"
    assert get_user.parameters[0].required is True


def test_find_endpoints():
    """Test searching for endpoints."""
    parser = APIParser()
    spec = parser.parse_from_dict(SAMPLE_OPENAPI_SPEC)

    results = spec.find_endpoints("user")
    assert len(results) == 3

    results = spec.find_endpoints("list")
    assert len(results) == 1
    assert results[0].operation_id == "listUsers"
