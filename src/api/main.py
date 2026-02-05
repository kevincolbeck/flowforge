"""
Universal Integrator API

FastAPI application providing the main interface for:
- Workflow management
- API parsing and connection
- Credential management
- Webhook handling
- Execution monitoring
"""

import logging
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from ..core import APIParser, CodeGenerator, LLMEngine, Workflow, WorkflowStep, Trigger
from ..core.workflow import TriggerType, WorkflowStatus
from ..core.service_registry import ServiceRegistry
from ..runtime import WebhookManager, WorkflowExecutor, WorkflowScheduler
from ..utils.credentials import CredentialManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============== Pydantic Models ==============

class ParseAPIRequest(BaseModel):
    url: str | None = None
    content: str | None = None
    documentation_url: str | None = None  # For non-standard docs


class NaturalLanguageWorkflowRequest(BaseModel):
    description: str = Field(..., description="Natural language description of the workflow")


class CreateWorkflowRequest(BaseModel):
    name: str
    description: str = ""
    trigger: dict[str, Any] | None = None
    steps: list[dict[str, Any]]


class UpdateWorkflowRequest(BaseModel):
    name: str | None = None
    description: str | None = None
    trigger: dict[str, Any] | None = None
    steps: list[dict[str, Any]] | None = None
    status: str | None = None


class StoreCredentialRequest(BaseModel):
    service: str
    name: str
    credential_type: str  # api_key, oauth2, basic
    data: dict[str, str]


class ExecuteWorkflowRequest(BaseModel):
    trigger_data: dict[str, Any] = Field(default_factory=dict)


class ConnectAPIsRequest(BaseModel):
    source_api_url: str
    target_api_url: str
    intent: str = Field(..., description="What you want to achieve, e.g., 'sync new orders to spreadsheet'")


# ============== Application State ==============

class AppState:
    def __init__(self):
        self.api_parser = APIParser()
        self.llm_engine = LLMEngine()
        self.code_generator = CodeGenerator()
        self.credential_manager = CredentialManager()
        self.workflow_executor = WorkflowExecutor()
        self.service_registry = ServiceRegistry()
        self.workflows: dict[str, Workflow] = {}
        self.api_specs: dict[str, Any] = {}  # Cached API specs
        self.webhook_manager: WebhookManager | None = None
        self.scheduler: WorkflowScheduler | None = None

    def _get_workflow_credentials(self, workflow: Workflow) -> dict[str, dict[str, str]]:
        """Get credentials for all services used in a workflow."""
        credentials = {}
        services = {step.service for step in workflow.steps}
        if workflow.trigger and workflow.trigger.service:
            services.add(workflow.trigger.service)

        for service in services:
            creds = self.credential_manager.get_credentials_for_service(service)
            if creds:
                credentials[service] = self.credential_manager.decrypt_for_use(creds[0])

        return credentials

    async def close(self):
        await self.api_parser.close()
        await self.workflow_executor.close()


# ============== Application Setup ==============

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    app.state.app_state = AppState()

    # Initialize database
    try:
        from ..db.database import init_db
        init_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.warning(f"Database initialization skipped: {e}")
        logger.warning("App will run without persistent storage")

    # Setup webhook manager
    async def execute_workflow_by_id(workflow_id: str, trigger_data: dict):
        workflow = app.state.app_state.workflows.get(workflow_id)
        if workflow:
            return await app.state.app_state.workflow_executor.execute(
                workflow,
                trigger_data,
                app.state.app_state._get_workflow_credentials(workflow),
            )

    app.state.app_state.webhook_manager = WebhookManager(execute_workflow_by_id)

    # Setup scheduler
    async def execute_scheduled(workflow: Workflow, trigger_data: dict):
        return await app.state.app_state.workflow_executor.execute(
            workflow,
            trigger_data,
            app.state.app_state._get_workflow_credentials(workflow),
        )

    app.state.app_state.scheduler = WorkflowScheduler(execute_scheduled)
    app.state.app_state.scheduler.start()

    logger.info("Universal Integrator started")

    yield

    # Shutdown
    if app.state.app_state.scheduler:
        app.state.app_state.scheduler.stop()
    await app.state.app_state.close()
    logger.info("Universal Integrator stopped")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Universal Integrator",
        description="AI-powered universal API integration platform",
        version="0.1.0",
        lifespan=lifespan,
    )

    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure appropriately for production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Mount static files
    static_path = Path(__file__).parent.parent / "web" / "static"
    if static_path.exists():
        app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

    return app


app = create_app()


def get_state() -> AppState:
    return app.state.app_state


# ============== UI Route ==============

@app.get("/", tags=["UI"])
async def serve_ui():
    """Serve the web UI."""
    static_path = Path(__file__).parent.parent / "web" / "static" / "index.html"
    if static_path.exists():
        return FileResponse(str(static_path))
    return {"message": "Universal Integrator API", "docs": "/docs"}


# ============== Services Endpoints ==============

@app.get("/services", tags=["Services"])
async def list_services(category: str | None = None, search: str | None = None):
    """List available pre-configured services."""
    state = get_state()

    if search:
        services = state.service_registry.search(search)
    elif category:
        services = state.service_registry.list_by_category(category)
    else:
        services = state.service_registry.list_all()

    return {
        "services": [
            {
                "name": s.name,
                "display_name": s.display_name,
                "description": s.description,
                "category": s.category,
                "auth_type": s.auth_type,
                "has_openapi": s.openapi_url is not None,
                "common_triggers": s.common_triggers,
                "common_actions": s.common_actions,
            }
            for s in services
        ],
        "categories": state.service_registry.get_categories(),
    }


@app.get("/services/{service_name}", tags=["Services"])
async def get_service(service_name: str):
    """Get details for a specific service."""
    state = get_state()

    service = state.service_registry.get(service_name)
    if not service:
        raise HTTPException(404, f"Service '{service_name}' not found")

    return {
        "name": service.name,
        "display_name": service.display_name,
        "description": service.description,
        "category": service.category,
        "base_url": service.base_url,
        "openapi_url": service.openapi_url,
        "docs_url": service.docs_url,
        "auth_type": service.auth_type,
        "auth_config": service.auth_config,
        "common_triggers": service.common_triggers,
        "common_actions": service.common_actions,
    }


# ============== API Parsing Endpoints ==============

@app.post("/api/parse", tags=["API Management"])
async def parse_api(request: ParseAPIRequest):
    """Parse an API specification from URL or content."""
    state = get_state()

    try:
        if request.url:
            spec = await state.api_parser.parse_from_url(request.url)
        elif request.content:
            spec = state.api_parser.parse_from_string(request.content)
        elif request.documentation_url:
            # Use LLM to parse non-standard documentation
            import httpx
            async with httpx.AsyncClient() as client:
                response = await client.get(request.documentation_url)
                doc_content = response.text

            understanding = await state.llm_engine.understand_api_from_docs(
                doc_content, request.documentation_url
            )
            return {
                "type": "llm_parsed",
                "understanding": {
                    "name": understanding.name,
                    "base_url": understanding.base_url,
                    "description": understanding.description,
                    "auth_type": understanding.auth_type,
                    "auth_instructions": understanding.auth_instructions,
                    "endpoints": understanding.endpoints,
                    "example_usage": understanding.example_usage,
                }
            }
        else:
            raise HTTPException(400, "Provide url, content, or documentation_url")

        # Cache the spec
        state.api_specs[spec.name] = spec

        return {
            "name": spec.name,
            "base_url": spec.base_url,
            "version": spec.version,
            "description": spec.description,
            "auth": {
                "type": spec.auth.auth_type.value if spec.auth else "none",
                "key_name": spec.auth.key_name if spec.auth else None,
            } if spec.auth else None,
            "endpoint_count": len(spec.endpoints),
            "endpoints": [
                {
                    "path": ep.path,
                    "method": ep.method,
                    "summary": ep.summary,
                    "operation_id": ep.operation_id,
                }
                for ep in spec.endpoints[:20]  # Limit for response size
            ],
        }

    except Exception as e:
        logger.error(f"Failed to parse API: {e}")
        raise HTTPException(400, f"Failed to parse API: {str(e)}")


@app.post("/api/connect", tags=["API Management"])
async def connect_apis(request: ConnectAPIsRequest):
    """
    Connect two APIs based on natural language intent.

    This is the magic endpoint - describe what you want in plain English,
    and it figures out how to connect the APIs.
    """
    state = get_state()

    try:
        # Parse both APIs
        source_spec = await state.api_parser.parse_from_url(request.source_api_url)
        target_spec = await state.api_parser.parse_from_url(request.target_api_url)

        # Find matching endpoints
        matches = await state.llm_engine.find_matching_endpoints(
            request.intent, source_spec, target_spec
        )

        if not matches:
            return {
                "status": "no_matches",
                "message": "Could not find suitable endpoints for this integration",
                "suggestion": "Try being more specific about what data to sync",
            }

        # Create integration plan for the best match
        source_ep, target_ep, reasoning = matches[0]
        plan = await state.llm_engine.plan_integration(
            request.intent, source_spec, source_ep, target_spec, target_ep
        )

        # Generate the integration code
        connector = await state.code_generator.generate_integration(
            plan, source_spec, target_spec
        )

        return {
            "status": "success",
            "integration": {
                "source_api": source_spec.name,
                "source_endpoint": f"{source_ep.method} {source_ep.path}",
                "target_api": target_spec.name,
                "target_endpoint": f"{target_ep.method} {target_ep.path}",
                "reasoning": reasoning,
                "description": plan.description,
                "steps": plan.steps,
                "data_mapping": plan.data_mapping,
            },
            "generated_code": connector.code,
            "auth_requirements": connector.auth_requirements,
        }

    except Exception as e:
        logger.error(f"Failed to connect APIs: {e}")
        raise HTTPException(400, f"Failed to connect APIs: {str(e)}")


@app.post("/api/generate-client", tags=["API Management"])
async def generate_client(request: ParseAPIRequest):
    """Generate a Python client for an API."""
    state = get_state()

    try:
        if request.url:
            spec = await state.api_parser.parse_from_url(request.url)
        elif request.content:
            spec = state.api_parser.parse_from_string(request.content)
        else:
            raise HTTPException(400, "Provide url or content")

        connector = state.code_generator.generate_api_client(spec)

        return {
            "name": connector.name,
            "code": connector.get_full_code(),
            "dependencies": connector.dependencies,
            "auth_requirements": connector.auth_requirements,
        }

    except Exception as e:
        logger.error(f"Failed to generate client: {e}")
        raise HTTPException(400, f"Failed to generate client: {str(e)}")


# ============== Workflow Endpoints ==============

@app.post("/workflows", tags=["Workflows"])
async def create_workflow(request: CreateWorkflowRequest):
    """Create a new workflow."""
    state = get_state()

    workflow_id = str(uuid.uuid4())

    trigger = None
    if request.trigger:
        trigger = Trigger(
            trigger_type=TriggerType(request.trigger.get("type", "manual")),
            service=request.trigger.get("service"),
            event=request.trigger.get("event"),
            schedule=request.trigger.get("schedule"),
            config=request.trigger.get("config", {}),
        )

    steps = [
        WorkflowStep(
            id=s.get("id", f"step_{i}"),
            name=s.get("name", f"Step {i+1}"),
            service=s["service"],
            action=s["action"],
            inputs=s.get("inputs", {}),
            outputs=s.get("outputs", {}),
            depends_on=s.get("depends_on", []),
            condition=s.get("condition"),
        )
        for i, s in enumerate(request.steps)
    ]

    workflow = Workflow(
        id=workflow_id,
        name=request.name,
        description=request.description,
        trigger=trigger,
        steps=steps,
    )

    # Validate
    errors = workflow.validate()
    if errors:
        raise HTTPException(400, {"errors": errors})

    state.workflows[workflow_id] = workflow

    # Register webhook if needed
    if trigger and trigger.trigger_type == TriggerType.WEBHOOK:
        state.webhook_manager.register_webhook(
            workflow_id=workflow_id,
            service=trigger.service,
            event=trigger.event,
            secret=trigger.config.get("secret"),
        )

    # Schedule if needed
    if trigger and trigger.trigger_type in (TriggerType.SCHEDULE, TriggerType.API_POLL):
        state.scheduler.schedule_workflow(workflow)

    return {"id": workflow_id, "workflow": workflow.to_dict()}


@app.post("/workflows/from-description", tags=["Workflows"])
async def create_workflow_from_description(request: NaturalLanguageWorkflowRequest):
    """
    Create a workflow from a natural language description.

    Example: "When a new order comes in on Shopify, add a row to Google Sheets
             and send a Slack notification"
    """
    state = get_state()

    try:
        # Parse natural language into workflow definition
        workflow_def = await state.llm_engine.parse_natural_language_workflow(
            request.description
        )

        # Create the workflow
        workflow_id = str(uuid.uuid4())

        trigger = None
        if workflow_def.get("trigger"):
            t = workflow_def["trigger"]
            trigger = Trigger(
                trigger_type=TriggerType(t.get("type", "manual")),
                service=t.get("service"),
                event=t.get("event"),
                schedule=t.get("schedule"),
            )

        steps = [
            WorkflowStep(
                id=s.get("id", f"step_{i}"),
                name=s.get("name", f"Step {i+1}"),
                service=s["service"],
                action=s["action"],
                inputs=s.get("inputs", {}),
                depends_on=s.get("depends_on", []),
            )
            for i, s in enumerate(workflow_def.get("steps", []))
        ]

        workflow = Workflow(
            id=workflow_id,
            name=workflow_def.get("name", "Generated Workflow"),
            description=workflow_def.get("description", request.description),
            trigger=trigger,
            steps=steps,
        )

        state.workflows[workflow_id] = workflow

        return {
            "id": workflow_id,
            "workflow": workflow.to_dict(),
            "required_connections": workflow_def.get("required_connections", []),
            "message": "Workflow created. Configure credentials for the required services.",
        }

    except Exception as e:
        logger.error(f"Failed to create workflow from description: {e}")
        raise HTTPException(400, f"Failed to parse workflow description: {str(e)}")


@app.get("/workflows", tags=["Workflows"])
async def list_workflows():
    """List all workflows."""
    state = get_state()
    return {
        "workflows": [w.to_dict() for w in state.workflows.values()]
    }


@app.get("/workflows/{workflow_id}", tags=["Workflows"])
async def get_workflow(workflow_id: str):
    """Get a specific workflow."""
    state = get_state()

    if workflow_id not in state.workflows:
        raise HTTPException(404, "Workflow not found")

    return state.workflows[workflow_id].to_dict()


@app.put("/workflows/{workflow_id}", tags=["Workflows"])
async def update_workflow(workflow_id: str, request: UpdateWorkflowRequest):
    """Update a workflow."""
    state = get_state()

    if workflow_id not in state.workflows:
        raise HTTPException(404, "Workflow not found")

    workflow = state.workflows[workflow_id]

    if request.name is not None:
        workflow.name = request.name
    if request.description is not None:
        workflow.description = request.description
    if request.status is not None:
        workflow.status = WorkflowStatus(request.status)

    # Validate
    errors = workflow.validate()
    if errors:
        raise HTTPException(400, {"errors": errors})

    return workflow.to_dict()


@app.delete("/workflows/{workflow_id}", tags=["Workflows"])
async def delete_workflow(workflow_id: str):
    """Delete a workflow."""
    state = get_state()

    if workflow_id not in state.workflows:
        raise HTTPException(404, "Workflow not found")

    # Unregister webhooks and schedules
    state.webhook_manager.unregister_workflow_webhooks(workflow_id)
    state.scheduler.unschedule_workflow(workflow_id)

    del state.workflows[workflow_id]
    return {"status": "deleted"}


@app.post("/workflows/{workflow_id}/execute", tags=["Workflows"])
async def execute_workflow(
    workflow_id: str,
    request: ExecuteWorkflowRequest,
    background_tasks: BackgroundTasks,
):
    """Execute a workflow manually."""
    state = get_state()

    if workflow_id not in state.workflows:
        raise HTTPException(404, "Workflow not found")

    workflow = state.workflows[workflow_id]

    # Get credentials for services used in workflow
    credentials = state._get_workflow_credentials(workflow)

    # Execute in background
    execution = await state.workflow_executor.execute(
        workflow,
        request.trigger_data,
        credentials,
    )

    return execution.to_dict()


@app.post("/workflows/{workflow_id}/activate", tags=["Workflows"])
async def activate_workflow(workflow_id: str):
    """Activate a workflow (enable triggers)."""
    state = get_state()

    if workflow_id not in state.workflows:
        raise HTTPException(404, "Workflow not found")

    workflow = state.workflows[workflow_id]
    workflow.status = WorkflowStatus.ACTIVE

    # Setup triggers
    if workflow.trigger:
        if workflow.trigger.trigger_type == TriggerType.WEBHOOK:
            config = state.webhook_manager.register_webhook(
                workflow_id=workflow_id,
                service=workflow.trigger.service,
                event=workflow.trigger.event,
            )
            return {"status": "activated", "webhook_url": config.path}

        elif workflow.trigger.trigger_type in (TriggerType.SCHEDULE, TriggerType.API_POLL):
            job_id = state.scheduler.schedule_workflow(workflow)
            next_run = state.scheduler.get_next_run_time(workflow_id)
            return {
                "status": "activated",
                "schedule_job_id": job_id,
                "next_run": next_run.isoformat() if next_run else None,
            }

    return {"status": "activated"}


@app.post("/workflows/{workflow_id}/deactivate", tags=["Workflows"])
async def deactivate_workflow(workflow_id: str):
    """Deactivate a workflow (disable triggers)."""
    state = get_state()

    if workflow_id not in state.workflows:
        raise HTTPException(404, "Workflow not found")

    workflow = state.workflows[workflow_id]
    workflow.status = WorkflowStatus.PAUSED

    state.webhook_manager.unregister_workflow_webhooks(workflow_id)
    state.scheduler.unschedule_workflow(workflow_id)

    return {"status": "deactivated"}


# ============== Credential Endpoints ==============

@app.post("/credentials", tags=["Credentials"])
async def store_credential(request: StoreCredentialRequest):
    """Store a new credential."""
    state = get_state()

    credential = state.credential_manager.store_credential(
        service=request.service,
        name=request.name,
        credential_type=request.credential_type,
        data=request.data,
    )

    return {
        "id": credential.id,
        "service": credential.service,
        "name": credential.name,
        "credential_type": credential.credential_type,
        "created_at": credential.created_at.isoformat(),
    }


@app.get("/credentials", tags=["Credentials"])
async def list_credentials(service: str | None = None):
    """List credentials (metadata only, not secrets)."""
    state = get_state()

    if service:
        credentials = state.credential_manager.get_credentials_for_service(service)
    else:
        state.credential_manager._load_all_credentials()
        credentials = list(state.credential_manager._credentials.values())

    return {
        "credentials": [
            state.credential_manager.export_credential_info(c.id)
            for c in credentials
        ]
    }


@app.delete("/credentials/{credential_id}", tags=["Credentials"])
async def delete_credential(credential_id: str):
    """Delete a credential."""
    state = get_state()

    if state.credential_manager.delete_credential(credential_id):
        return {"status": "deleted"}
    raise HTTPException(404, "Credential not found")


# ============== Webhook Endpoints ==============

@app.post("/webhooks/{path:path}", tags=["Webhooks"])
@app.get("/webhooks/{path:path}", tags=["Webhooks"])
async def handle_webhook(path: str, request: Request):
    """Handle incoming webhooks."""
    state = get_state()

    from ..runtime.webhooks import WebhookEvent

    body = await request.body()

    event = WebhookEvent(
        path=f"/webhooks/{path}",
        method=request.method,
        headers=dict(request.headers),
        body=body,
        query_params=dict(request.query_params),
    )

    result = await state.webhook_manager.handle_webhook(event)
    return JSONResponse(result)


@app.get("/webhooks", tags=["Webhooks"])
async def list_webhooks():
    """List all registered webhooks."""
    state = get_state()
    return {"webhooks": state.webhook_manager.list_webhooks()}


# ============== Connectors Endpoints ==============

@app.get("/api/connectors", tags=["Connectors"])
async def list_connectors():
    """List all available connectors with their actions."""
    from ..connectors import ConnectorRegistry

    connectors = ConnectorRegistry.list_connectors()
    return {"connectors": connectors}


@app.get("/api/connectors/{service}", tags=["Connectors"])
async def get_connector_info(service: str):
    """Get detailed info about a specific connector."""
    from ..connectors import ConnectorRegistry, get_service_info

    if not ConnectorRegistry.service_exists(service):
        raise HTTPException(404, f"Connector '{service}' not found")

    actions = ConnectorRegistry.get_actions(service)
    info = get_service_info(service)

    return {
        "service": service,
        "display_name": info.get("name") if info else service.title(),
        "description": info.get("description") if info else "",
        "icon": info.get("icon") if info else "🔌",
        "category": info.get("category") if info else "other",
        "actions": actions,
        "auth_fields": info.get("auth_fields", []) if info else [],
    }


# ============== Templates Endpoints ==============

@app.get("/templates", tags=["Templates"])
async def list_templates(category: str | None = None):
    """List available workflow templates."""
    from ..core.templates import WORKFLOW_TEMPLATES

    templates = WORKFLOW_TEMPLATES

    if category:
        templates = [t for t in templates if t.category == category]

    return {
        "templates": [
            {
                "id": t.id,
                "name": t.name,
                "description": t.description,
                "category": t.category,
                "trigger_app": t.trigger_service,
                "action_app": t.action_service,
                "icon": t.icon,
            }
            for t in templates
        ]
    }


@app.get("/templates/{template_id}", tags=["Templates"])
async def get_template(template_id: str):
    """Get a specific template."""
    from ..core.templates import WORKFLOW_TEMPLATES

    template = next((t for t in WORKFLOW_TEMPLATES if t.id == template_id), None)
    if not template:
        raise HTTPException(404, "Template not found")

    return template.to_dict()


@app.post("/templates/{template_id}/use", tags=["Templates"])
async def use_template(template_id: str):
    """Create a workflow from a template."""
    from ..core.templates import WORKFLOW_TEMPLATES

    template = next((t for t in WORKFLOW_TEMPLATES if t.id == template_id), None)
    if not template:
        raise HTTPException(404, "Template not found")

    state = get_state()
    workflow_id = str(uuid.uuid4())

    # Convert template to workflow
    trigger = Trigger(
        trigger_type=TriggerType(template.trigger.get("type", "webhook")),
        service=template.trigger_service,
        event=template.trigger.get("event"),
        config=template.trigger.get("config", {}),
    )

    steps = [
        WorkflowStep(
            id=s.get("id", f"step_{i}"),
            name=s.get("name", f"Step {i+1}"),
            service=s.get("service", ""),
            action=s.get("action", ""),
            inputs=s.get("inputs", {}),
            depends_on=s.get("depends_on", []),
        )
        for i, s in enumerate(template.steps)
    ]

    workflow = Workflow(
        id=workflow_id,
        name=template.name,
        description=template.description,
        trigger=trigger,
        steps=steps,
    )

    state.workflows[workflow_id] = workflow

    return {
        "id": workflow_id,
        "workflow": workflow.to_dict(),
        "required_connections": [template.trigger_service, template.action_service],
    }


# ============== Execution Logs Endpoints ==============

@app.get("/runs", tags=["Execution"])
async def list_runs(workflow_id: str | None = None, limit: int = 50):
    """List recent execution runs."""
    from ..runtime.logger import get_execution_logger

    exec_logger = get_execution_logger()

    if workflow_id:
        runs = exec_logger.get_workflow_runs(workflow_id, limit)
    else:
        runs = exec_logger.get_recent_runs("default", limit)

    return {"runs": runs}


@app.get("/runs/{run_id}", tags=["Execution"])
async def get_run_details(run_id: str):
    """Get details of a specific run."""
    from ..db.database import get_db

    db = get_db()
    run = db.get_run(run_id)

    if not run:
        raise HTTPException(404, "Run not found")

    return {
        "id": run.id,
        "workflow_id": run.workflow_id,
        "status": run.status,
        "trigger_data": run.trigger_data,
        "step_results": run.step_results,
        "started_at": run.started_at.isoformat(),
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        "duration_ms": run.duration_ms,
        "error": run.error_message,
    }


@app.get("/runs/{run_id}/logs", tags=["Execution"])
async def get_run_logs(run_id: str):
    """Get logs for a specific run."""
    from ..runtime.logger import get_execution_logger

    exec_logger = get_execution_logger()
    logs = exec_logger.get_run_logs(run_id)

    return {"logs": logs}


@app.get("/workflows/{workflow_id}/stats", tags=["Execution"])
async def get_workflow_stats(workflow_id: str):
    """Get execution statistics for a workflow."""
    from ..runtime.logger import get_execution_logger

    exec_logger = get_execution_logger()
    stats = exec_logger.get_execution_stats(workflow_id)

    return stats


@app.get("/workflows/{workflow_id}/runs", tags=["Execution"])
async def get_workflow_runs(workflow_id: str, limit: int = 50):
    """Get recent runs for a specific workflow."""
    from ..runtime.logger import get_execution_logger

    exec_logger = get_execution_logger()
    runs = exec_logger.get_workflow_runs(workflow_id, limit)

    return {"runs": runs}


# ============== Health & Status ==============

@app.get("/health", tags=["System"])
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "version": "0.1.0"}


@app.get("/status", tags=["System"])
async def system_status():
    """Get system status including running workflows."""
    state = get_state()

    return {
        "workflows_count": len(state.workflows),
        "active_workflows": len([
            w for w in state.workflows.values()
            if w.status == WorkflowStatus.ACTIVE
        ]),
        "registered_webhooks": len(state.webhook_manager.list_webhooks()),
        "scheduled_jobs": state.scheduler.get_scheduled_workflows(),
        "running_executions": [
            e.to_dict() for e in state.workflow_executor.get_running_executions()
        ],
    }


