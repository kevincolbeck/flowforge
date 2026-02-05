"""
Workflow Definition and Management

Defines the structure of workflows, triggers, and steps.
"""

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class TriggerType(Enum):
    WEBHOOK = "webhook"
    SCHEDULE = "schedule"
    MANUAL = "manual"
    API_POLL = "api_poll"


class StepStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class WorkflowStatus(Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    ARCHIVED = "archived"


@dataclass
class Trigger:
    """Defines what starts a workflow."""

    trigger_type: TriggerType
    service: str | None = None  # e.g., "shopify", "stripe"
    event: str | None = None  # e.g., "order.created", "payment.completed"
    schedule: str | None = None  # Cron expression for scheduled triggers
    poll_interval: int | None = None  # Seconds between polls for API_POLL
    config: dict[str, Any] = field(default_factory=dict)

    @property
    def webhook_path(self) -> str:
        """Generate a unique webhook path for this trigger."""
        if self.service and self.event:
            return f"/webhooks/{self.service}/{self.event.replace('.', '/')}"
        return f"/webhooks/{uuid.uuid4().hex[:8]}"


@dataclass
class WorkflowStep:
    """A single step in a workflow."""

    id: str
    name: str
    service: str  # Which API/service to call
    action: str  # What action to perform
    inputs: dict[str, Any] = field(default_factory=dict)  # Input mapping (can reference previous steps)
    outputs: dict[str, str] = field(default_factory=dict)  # Output field mapping
    depends_on: list[str] = field(default_factory=list)  # Step IDs this depends on
    condition: str | None = None  # Optional condition expression
    retry_config: dict[str, Any] = field(default_factory=lambda: {"max_retries": 3, "backoff": "exponential"})
    timeout: int = 30  # Seconds

    def resolve_inputs(self, context: dict[str, Any]) -> dict[str, Any]:
        """Resolve input references from context."""
        resolved = {}
        for key, value in self.inputs.items():
            resolved[key] = self._resolve_value(value, context)
        return resolved

    def _resolve_value(self, value: Any, context: dict[str, Any]) -> Any:
        """Resolve a single value, handling template references."""
        if isinstance(value, str) and "{{" in value:
            # Handle template syntax like {{trigger.data.field}} or {{steps.step_1.output.field}}
            import re

            def replace_ref(match):
                ref_path = match.group(1).strip()
                return str(self._get_nested_value(context, ref_path.split(".")))

            return re.sub(r"\{\{(.+?)\}\}", replace_ref, value)
        elif isinstance(value, dict):
            return {k: self._resolve_value(v, context) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._resolve_value(v, context) for v in value]
        return value

    def _get_nested_value(self, obj: dict, path: list[str]) -> Any:
        """Get a value from a nested dictionary using a path."""
        current = obj
        for key in path:
            if isinstance(current, dict):
                current = current.get(key, "")
            else:
                return ""
        return current


@dataclass
class Workflow:
    """A complete workflow definition."""

    id: str
    name: str
    description: str = ""
    trigger: Trigger | None = None
    steps: list[WorkflowStep] = field(default_factory=list)
    status: WorkflowStatus = WorkflowStatus.DRAFT
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    owner_id: str | None = None
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def get_step(self, step_id: str) -> WorkflowStep | None:
        """Get a step by ID."""
        for step in self.steps:
            if step.id == step_id:
                return step
        return None

    def get_execution_order(self) -> list[WorkflowStep]:
        """Get steps in execution order (topological sort)."""
        # Build dependency graph
        in_degree = {step.id: len(step.depends_on) for step in self.steps}
        dependents = {step.id: [] for step in self.steps}

        for step in self.steps:
            for dep in step.depends_on:
                if dep in dependents:
                    dependents[dep].append(step.id)

        # Topological sort
        queue = [step_id for step_id, degree in in_degree.items() if degree == 0]
        order = []

        while queue:
            step_id = queue.pop(0)
            order.append(step_id)

            for dependent_id in dependents[step_id]:
                in_degree[dependent_id] -= 1
                if in_degree[dependent_id] == 0:
                    queue.append(dependent_id)

        # Return steps in order
        step_map = {step.id: step for step in self.steps}
        return [step_map[step_id] for step_id in order if step_id in step_map]

    def validate(self) -> list[str]:
        """Validate the workflow and return list of errors."""
        errors = []

        if not self.name:
            errors.append("Workflow name is required")

        if not self.steps:
            errors.append("Workflow must have at least one step")

        # Check for missing dependencies
        step_ids = {step.id for step in self.steps}
        for step in self.steps:
            for dep in step.depends_on:
                if dep not in step_ids:
                    errors.append(f"Step '{step.id}' depends on unknown step '{dep}'")

        # Check for circular dependencies
        if self._has_circular_dependency():
            errors.append("Workflow has circular dependencies")

        return errors

    def _has_circular_dependency(self) -> bool:
        """Check if workflow has circular dependencies."""
        visited = set()
        rec_stack = set()

        def dfs(step_id: str) -> bool:
            visited.add(step_id)
            rec_stack.add(step_id)

            step = self.get_step(step_id)
            if step:
                for dep in step.depends_on:
                    if dep not in visited:
                        if dfs(dep):
                            return True
                    elif dep in rec_stack:
                        return True

            rec_stack.remove(step_id)
            return False

        for step in self.steps:
            if step.id not in visited:
                if dfs(step.id):
                    return True

        return False

    def to_dict(self) -> dict[str, Any]:
        """Convert workflow to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "trigger": {
                "type": self.trigger.trigger_type.value,
                "service": self.trigger.service,
                "event": self.trigger.event,
                "schedule": self.trigger.schedule,
                "config": self.trigger.config,
            } if self.trigger else None,
            "steps": [
                {
                    "id": step.id,
                    "name": step.name,
                    "service": step.service,
                    "action": step.action,
                    "inputs": step.inputs,
                    "outputs": step.outputs,
                    "depends_on": step.depends_on,
                    "condition": step.condition,
                }
                for step in self.steps
            ],
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "tags": self.tags,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Workflow":
        """Create workflow from dictionary."""
        trigger = None
        if data.get("trigger"):
            t = data["trigger"]
            trigger = Trigger(
                trigger_type=TriggerType(t["type"]),
                service=t.get("service"),
                event=t.get("event"),
                schedule=t.get("schedule"),
                config=t.get("config", {}),
            )

        steps = [
            WorkflowStep(
                id=s["id"],
                name=s["name"],
                service=s["service"],
                action=s["action"],
                inputs=s.get("inputs", {}),
                outputs=s.get("outputs", {}),
                depends_on=s.get("depends_on", []),
                condition=s.get("condition"),
            )
            for s in data.get("steps", [])
        ]

        return cls(
            id=data["id"],
            name=data["name"],
            description=data.get("description", ""),
            trigger=trigger,
            steps=steps,
            status=WorkflowStatus(data.get("status", "draft")),
            created_at=datetime.fromisoformat(data["created_at"]) if "created_at" in data else datetime.utcnow(),
            updated_at=datetime.fromisoformat(data["updated_at"]) if "updated_at" in data else datetime.utcnow(),
            owner_id=data.get("owner_id"),
            tags=data.get("tags", []),
            metadata=data.get("metadata", {}),
        )


@dataclass
class WorkflowExecution:
    """Represents a single execution of a workflow."""

    id: str
    workflow_id: str
    status: StepStatus = StepStatus.PENDING
    trigger_data: dict[str, Any] = field(default_factory=dict)
    context: dict[str, Any] = field(default_factory=dict)  # Accumulated data from steps
    step_results: dict[str, dict[str, Any]] = field(default_factory=dict)  # Results by step ID
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert execution to dictionary."""
        return {
            "id": self.id,
            "workflow_id": self.workflow_id,
            "status": self.status.value,
            "trigger_data": self.trigger_data,
            "context": self.context,
            "step_results": self.step_results,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error": self.error,
        }
