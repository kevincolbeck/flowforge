"""
Workflow Execution Engine

Executes workflows with proper error handling, retries, and sandboxing.
Integrates with the ConnectorRegistry to support 50+ services.
"""

import asyncio
import hashlib
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

import httpx

from ..core.workflow import (
    StepStatus,
    Workflow,
    WorkflowExecution,
    WorkflowStep,
)
from ..connectors.registry import ConnectorRegistry, execute_connector

logger = logging.getLogger(__name__)


@dataclass
class ExecutionContext:
    """Context passed through workflow execution."""

    execution_id: str
    workflow_id: str
    trigger_data: dict[str, Any] = field(default_factory=dict)
    steps: dict[str, dict[str, Any]] = field(default_factory=dict)  # Results by step ID
    variables: dict[str, Any] = field(default_factory=dict)  # User-defined variables
    credentials: dict[str, dict[str, str]] = field(default_factory=dict)  # Service credentials

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for template resolution."""
        return {
            "trigger": {"data": self.trigger_data},
            "steps": self.steps,
            "variables": self.variables,
        }

    def get_step_output(self, step_id: str, path: str = None) -> Any:
        """Get output from a previous step, optionally with a path."""
        step_result = self.steps.get(step_id, {})
        if not path:
            return step_result.get("data") or step_result.get("output")

        # Navigate path
        current = step_result.get("data") or step_result.get("output") or step_result
        for part in path.split("."):
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                idx = int(part)
                current = current[idx] if idx < len(current) else None
            else:
                return None
        return current


class StepExecutor:
    """Executes individual workflow steps using the ConnectorRegistry."""

    def __init__(
        self,
        credentials: dict[str, dict[str, str]] | None = None,
        action_handlers: dict[str, Callable] | None = None,
    ):
        self.credentials = credentials or {}
        self.action_handlers = action_handlers or {}
        self.http_client = httpx.AsyncClient(timeout=60.0)

    async def close(self):
        await self.http_client.aclose()

    def register_action(self, service: str, action: str, handler: Callable):
        """Register a custom action handler."""
        key = f"{service}:{action}"
        self.action_handlers[key] = handler

    async def execute(
        self,
        step: WorkflowStep,
        context: ExecutionContext,
    ) -> dict[str, Any]:
        """Execute a single workflow step."""
        logger.info(f"Executing step: {step.id} ({step.name})")

        # Check condition if present
        if step.condition and not self._evaluate_condition(step.condition, context):
            logger.info(f"Step {step.id} skipped due to condition")
            return {"status": "skipped", "reason": "condition not met"}

        # Resolve inputs
        resolved_inputs = step.resolve_inputs(context.to_dict())

        # Get credentials for this service
        service_creds = context.credentials.get(step.service) or self.credentials.get(step.service, {})

        # Check for custom handler first
        handler_key = f"{step.service}:{step.action}"
        if handler_key in self.action_handlers:
            return await self.action_handlers[handler_key](resolved_inputs, service_creds, context)

        # Check if this is a registered connector
        if ConnectorRegistry.service_exists(step.service):
            return await self._execute_connector_action(step, resolved_inputs, service_creds)

        # Fallback: HTTP-based action for unknown services
        return await self._execute_http_action(step, resolved_inputs, service_creds)

    async def _execute_connector_action(
        self,
        step: WorkflowStep,
        inputs: dict[str, Any],
        credentials: dict[str, str],
    ) -> dict[str, Any]:
        """Execute an action using the ConnectorRegistry."""
        try:
            # Execute using the registry
            result = await execute_connector(
                service=step.service,
                action=step.action,
                inputs=inputs,
                credentials=credentials,
            )

            if result.success:
                return {
                    "status": "success",
                    "data": result.data,
                    "output": result.data,  # Alias for template access
                }
            else:
                return {
                    "status": "error",
                    "error": result.error,
                }

        except Exception as e:
            logger.error(f"Connector execution failed for {step.service}:{step.action}: {e}")
            return {"status": "error", "error": str(e)}

    async def _execute_http_action(
        self,
        step: WorkflowStep,
        inputs: dict[str, Any],
        credentials: dict[str, str],
    ) -> dict[str, Any]:
        """Execute an HTTP-based action for services not in the registry."""
        # Extract HTTP details from inputs
        url = inputs.get("url", "")
        method = inputs.get("method", "POST").upper()
        headers = inputs.get("headers", {})
        body = inputs.get("body", {})
        params = inputs.get("params", {})

        # Apply credentials
        if "api_key" in credentials:
            headers["Authorization"] = f"Bearer {credentials['api_key']}"
        elif "access_token" in credentials:
            headers["Authorization"] = f"Bearer {credentials['access_token']}"
        elif "token" in credentials:
            headers["Authorization"] = f"Bearer {credentials['token']}"

        # Make request with retries
        max_retries = step.retry_config.get("max_retries", 3)
        for attempt in range(max_retries):
            try:
                response = await self.http_client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=body if method in ["POST", "PUT", "PATCH"] else None,
                    params=params,
                    timeout=step.timeout,
                )

                if response.status_code >= 400:
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    return {
                        "status": "error",
                        "status_code": response.status_code,
                        "error": response.text,
                    }

                # Parse response
                try:
                    data = response.json() if response.content else {}
                except Exception:
                    data = {"raw": response.text}

                return {
                    "status": "success",
                    "status_code": response.status_code,
                    "data": data,
                    "output": data,
                }

            except Exception as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return {"status": "error", "error": str(e)}

        return {"status": "error", "error": "Max retries exceeded"}

    def _evaluate_condition(self, condition: str, context: ExecutionContext) -> bool:
        """Evaluate a condition expression safely."""
        try:
            ctx = context.to_dict()

            # Handle various comparison operators
            operators = [
                ("==", lambda a, b: str(a) == b),
                ("!=", lambda a, b: str(a) != b),
                (">=", lambda a, b: float(a) >= float(b)),
                ("<=", lambda a, b: float(a) <= float(b)),
                (">", lambda a, b: float(a) > float(b)),
                ("<", lambda a, b: float(a) < float(b)),
                (" in ", lambda a, b: str(a) in b),
                (" not in ", lambda a, b: str(a) not in b),
            ]

            for op, func in operators:
                if op in condition:
                    left, right = condition.split(op, 1)
                    left_val = self._get_value(left.strip(), ctx)
                    right_val = right.strip().strip("'\"")
                    return func(left_val, right_val)

            # Boolean checks
            if condition.lower() in ("true", "1"):
                return True
            elif condition.lower() in ("false", "0"):
                return False

            # Check if a value exists/is truthy
            val = self._get_value(condition.strip(), ctx)
            return bool(val)

        except Exception as e:
            logger.warning(f"Failed to evaluate condition '{condition}': {e}")
            return True  # Default to true if can't evaluate

    def _get_value(self, path: str, ctx: dict) -> Any:
        """Get a value from context by path."""
        parts = path.split(".")
        current = ctx
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part, "")
            elif isinstance(current, list) and part.isdigit():
                idx = int(part)
                current = current[idx] if idx < len(current) else ""
            else:
                return ""
        return current


class WorkflowExecutor:
    """Executes complete workflows with full connector support."""

    def __init__(
        self,
        step_executor: StepExecutor | None = None,
        on_step_start: Callable | None = None,
        on_step_complete: Callable | None = None,
        on_workflow_complete: Callable | None = None,
    ):
        self.step_executor = step_executor or StepExecutor()
        self.on_step_start = on_step_start
        self.on_step_complete = on_step_complete
        self.on_workflow_complete = on_workflow_complete
        self._running_executions: dict[str, WorkflowExecution] = {}

    async def close(self):
        await self.step_executor.close()

    async def execute(
        self,
        workflow: Workflow,
        trigger_data: dict[str, Any] | None = None,
        credentials: dict[str, dict[str, str]] | None = None,
    ) -> WorkflowExecution:
        """Execute a workflow and return the execution record."""
        execution_id = str(uuid.uuid4())

        execution = WorkflowExecution(
            id=execution_id,
            workflow_id=workflow.id,
            status=StepStatus.RUNNING,
            trigger_data=trigger_data or {},
            started_at=datetime.utcnow(),
        )

        self._running_executions[execution_id] = execution

        context = ExecutionContext(
            execution_id=execution_id,
            workflow_id=workflow.id,
            trigger_data=trigger_data or {},
            credentials=credentials or {},
        )

        try:
            # Get steps in execution order (topologically sorted)
            ordered_steps = workflow.get_execution_order()

            for step in ordered_steps:
                # Check if dependencies completed successfully
                deps_ok = all(
                    context.steps.get(dep, {}).get("status") in ("success", "skipped")
                    for dep in step.depends_on
                )

                if not deps_ok:
                    context.steps[step.id] = {
                        "status": "skipped",
                        "reason": "dependency failed",
                    }
                    execution.step_results[step.id] = context.steps[step.id]
                    continue

                # Notify step start
                if self.on_step_start:
                    await self._safe_callback(self.on_step_start, execution, step)

                # Execute step
                try:
                    result = await self.step_executor.execute(step, context)
                    context.steps[step.id] = result
                    execution.step_results[step.id] = result

                    # Log success/failure
                    if result.get("status") == "success":
                        logger.info(f"Step {step.id} completed successfully")
                    else:
                        logger.warning(f"Step {step.id} status: {result.get('status')}")

                    # Notify step complete
                    if self.on_step_complete:
                        await self._safe_callback(self.on_step_complete, execution, step, result)

                except Exception as e:
                    error_result = {"status": "error", "error": str(e)}
                    context.steps[step.id] = error_result
                    execution.step_results[step.id] = error_result
                    logger.error(f"Step {step.id} failed: {e}")

            # Determine overall status
            statuses = [r.get("status") for r in context.steps.values()]
            if all(s in ("success", "skipped") for s in statuses):
                execution.status = StepStatus.COMPLETED
            elif any(s == "error" for s in statuses):
                execution.status = StepStatus.FAILED
            else:
                execution.status = StepStatus.COMPLETED

            execution.context = context.to_dict()
            execution.completed_at = datetime.utcnow()

        except Exception as e:
            execution.status = StepStatus.FAILED
            execution.error = str(e)
            execution.completed_at = datetime.utcnow()
            logger.error(f"Workflow execution failed: {e}")

        finally:
            del self._running_executions[execution_id]

            # Notify workflow complete
            if self.on_workflow_complete:
                await self._safe_callback(self.on_workflow_complete, execution)

        return execution

    async def execute_step_isolated(
        self,
        workflow: Workflow,
        step_id: str,
        test_data: dict[str, Any],
        credentials: dict[str, dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        """Execute a single step in isolation for testing."""
        step = workflow.get_step(step_id)
        if not step:
            return {"status": "error", "error": f"Step {step_id} not found"}

        context = ExecutionContext(
            execution_id=str(uuid.uuid4()),
            workflow_id=workflow.id,
            trigger_data=test_data,
            credentials=credentials or {},
        )

        return await self.step_executor.execute(step, context)

    def get_running_executions(self) -> list[WorkflowExecution]:
        """Get list of currently running executions."""
        return list(self._running_executions.values())

    async def _safe_callback(self, callback: Callable, *args):
        """Execute a callback safely."""
        try:
            result = callback(*args)
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            logger.error(f"Callback failed: {e}")


class DataPipeline:
    """
    High-level helper for common data integration patterns.

    Provides simple methods for:
    - Fetching data from any API (with authentication)
    - Transforming data
    - Loading into any database
    """

    def __init__(self, credentials: dict[str, dict[str, str]] = None):
        self.credentials = credentials or {}

    async def fetch_from_api(
        self,
        service: str = "auth_http",
        action: str = "get",
        credentials: dict = None,
        **inputs,
    ) -> dict[str, Any]:
        """
        Fetch data from any API.

        Examples:
            # Using authenticated HTTP
            data = await pipeline.fetch_from_api(
                service="auth_http",
                action="get",
                credentials={
                    "auth_type": "oauth2_client",
                    "token_url": "https://api.example.com/oauth/token",
                    "client_id": "xxx",
                    "client_secret": "xxx",
                },
                url="/api/data",
            )

            # Using Salesforce connector
            data = await pipeline.fetch_from_api(
                service="salesforce",
                action="query",
                credentials={"instance_url": "...", "access_token": "..."},
                query="SELECT Id, Name FROM Account",
            )
        """
        creds = credentials or self.credentials.get(service, {})
        result = await execute_connector(service, action, inputs, creds)

        if result.success:
            return {"success": True, "data": result.data}
        return {"success": False, "error": result.error}

    async def load_to_database(
        self,
        service: str,
        action: str = "insert",
        credentials: dict = None,
        **inputs,
    ) -> dict[str, Any]:
        """
        Load data into any database.

        Examples:
            # Insert into PostgreSQL
            result = await pipeline.load_to_database(
                service="postgresql",
                action="insert",
                credentials={"host": "...", "database": "...", ...},
                table="customers",
                data={"name": "John", "email": "john@example.com"},
            )

            # Bulk insert into Azure SQL
            result = await pipeline.load_to_database(
                service="azure_sql",
                action="bulk_insert",
                credentials={"server": "...", "database": "...", ...},
                table="orders",
                records=[...],
            )

            # Insert into MongoDB
            result = await pipeline.load_to_database(
                service="mongodb",
                action="insert_one",
                credentials={"connection_string": "...", "database": "..."},
                collection="events",
                document={...},
            )
        """
        creds = credentials or self.credentials.get(service, {})
        result = await execute_connector(service, action, inputs, creds)

        if result.success:
            return {"success": True, "data": result.data}
        return {"success": False, "error": result.error}

    async def extract_transform_load(
        self,
        source_service: str,
        source_action: str,
        source_credentials: dict,
        source_inputs: dict,
        dest_service: str,
        dest_action: str,
        dest_credentials: dict,
        dest_inputs_template: dict,
        transform: Callable[[dict], dict] = None,
    ) -> dict[str, Any]:
        """
        Complete ETL pipeline: Extract from source, transform, load to destination.

        Example:
            result = await pipeline.extract_transform_load(
                # Source: RepairDispatch API
                source_service="auth_http",
                source_action="get",
                source_credentials={
                    "auth_type": "login",
                    "auth_url": "https://api.repairdispatch.com/auth",
                    "auth_body": {"email": "...", "password": "..."},
                    "token_path": "data.token",
                },
                source_inputs={"url": "https://api.repairdispatch.com/jobs"},

                # Destination: Azure SQL
                dest_service="azure_sql",
                dest_action="bulk_insert",
                dest_credentials={
                    "server": "myserver.database.windows.net",
                    "database": "mydb",
                    "username": "admin",
                    "password": "...",
                },
                dest_inputs_template={
                    "table": "repair_jobs",
                    "records": "{{data.jobs}}",  # Template from source data
                },

                # Optional transform
                transform=lambda data: {
                    "jobs": [
                        {"id": j["id"], "status": j["status"], "created": j["date"]}
                        for j in data.get("jobs", [])
                    ]
                },
            )
        """
        # Extract
        extract_result = await self.fetch_from_api(
            service=source_service,
            action=source_action,
            credentials=source_credentials,
            **source_inputs,
        )

        if not extract_result.get("success"):
            return {"success": False, "error": f"Extract failed: {extract_result.get('error')}"}

        source_data = extract_result.get("data", {})

        # Transform
        if transform:
            try:
                source_data = transform(source_data)
            except Exception as e:
                return {"success": False, "error": f"Transform failed: {e}"}

        # Resolve destination inputs from template
        dest_inputs = self._resolve_template(dest_inputs_template, source_data)

        # Load
        load_result = await self.load_to_database(
            service=dest_service,
            action=dest_action,
            credentials=dest_credentials,
            **dest_inputs,
        )

        if not load_result.get("success"):
            return {"success": False, "error": f"Load failed: {load_result.get('error')}"}

        return {
            "success": True,
            "extracted": source_data,
            "loaded": load_result.get("data"),
        }

    def _resolve_template(self, template: dict, data: dict) -> dict:
        """Resolve template placeholders with data."""
        import re

        def resolve_value(value, data):
            if isinstance(value, str) and "{{" in value:
                # Simple template resolution
                match = re.match(r"^\{\{(.+)\}\}$", value.strip())
                if match:
                    path = match.group(1).strip()
                    return self._get_nested(data, path)
                # Partial replacement
                def replacer(m):
                    path = m.group(1).strip()
                    return str(self._get_nested(data, path) or "")
                return re.sub(r"\{\{(.+?)\}\}", replacer, value)
            elif isinstance(value, dict):
                return {k: resolve_value(v, data) for k, v in value.items()}
            elif isinstance(value, list):
                return [resolve_value(v, data) for v in value]
            return value

        return resolve_value(template, data)

    def _get_nested(self, data: dict, path: str) -> Any:
        """Get nested value from dict using dot notation."""
        current = data
        for part in path.split("."):
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                idx = int(part)
                current = current[idx] if idx < len(current) else None
            else:
                return None
        return current


class SandboxedExecutor:
    """
    Executes generated code in a sandboxed environment.

    For production, this would use Docker, Firecracker, or similar
    for true isolation. This implementation provides basic safety.
    """

    ALLOWED_IMPORTS = {
        "json",
        "datetime",
        "re",
        "math",
        "hashlib",
        "base64",
        "urllib.parse",
        "collections",
    }

    BLOCKED_BUILTINS = {
        "eval",
        "exec",
        "compile",
        "__import__",
        "open",
        "input",
        "breakpoint",
    }

    def __init__(self, timeout: int = 30):
        self.timeout = timeout

    async def execute(
        self,
        code: str,
        inputs: dict[str, Any],
        function_name: str = "execute_integration",
    ) -> dict[str, Any]:
        """Execute generated code safely."""
        # Basic security checks
        self._validate_code(code)

        # Create restricted globals
        safe_globals = self._create_safe_globals()
        safe_globals["inputs"] = inputs

        try:
            # Compile and execute
            compiled = compile(code, "<generated>", "exec")

            # Execute in restricted namespace
            exec(compiled, safe_globals)

            # Get the function and call it
            if function_name in safe_globals:
                func = safe_globals[function_name]
                result = await asyncio.wait_for(
                    self._run_function(func, inputs),
                    timeout=self.timeout,
                )
                return {"status": "success", "result": result}
            else:
                return {"status": "error", "error": f"Function {function_name} not found"}

        except asyncio.TimeoutError:
            return {"status": "error", "error": "Execution timed out"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    async def _run_function(self, func: Callable, inputs: dict) -> Any:
        """Run a function, handling both sync and async."""
        result = func(inputs)
        if asyncio.iscoroutine(result):
            return await result
        return result

    def _validate_code(self, code: str):
        """Validate code for obvious security issues."""
        blocked_patterns = [
            r"\bos\.",
            r"\bsys\.",
            r"\bsubprocess\.",
            r"\b__class__\b",
            r"\b__bases__\b",
            r"\b__subclasses__\b",
            r"\b__globals__\b",
            r"\b__builtins__\b",
            r"\bgetattr\s*\(",
            r"\bsetattr\s*\(",
            r"\bdelattr\s*\(",
        ]

        import re

        for pattern in blocked_patterns:
            if re.search(pattern, code):
                raise ValueError(f"Code contains blocked pattern: {pattern}")

    def _create_safe_globals(self) -> dict[str, Any]:
        """Create a restricted globals dictionary."""
        safe_builtins = {
            k: v for k, v in __builtins__.items()
            if k not in self.BLOCKED_BUILTINS
        } if isinstance(__builtins__, dict) else {
            k: getattr(__builtins__, k)
            for k in dir(__builtins__)
            if not k.startswith("_") and k not in self.BLOCKED_BUILTINS
        }

        return {
            "__builtins__": safe_builtins,
            "json": __import__("json"),
            "datetime": __import__("datetime"),
            "re": __import__("re"),
            "hashlib": __import__("hashlib"),
            "base64": __import__("base64"),
        }
