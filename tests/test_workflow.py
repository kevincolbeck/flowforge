"""Tests for workflow functionality."""

import pytest
from src.core.workflow import (
    Workflow,
    WorkflowStep,
    Trigger,
    TriggerType,
    WorkflowStatus,
)


def test_workflow_creation():
    """Test creating a basic workflow."""
    workflow = Workflow(
        id="test-workflow-1",
        name="Test Workflow",
        description="A test workflow",
        steps=[
            WorkflowStep(
                id="step_1",
                name="First Step",
                service="test_service",
                action="test_action",
            )
        ],
    )

    assert workflow.id == "test-workflow-1"
    assert workflow.name == "Test Workflow"
    assert len(workflow.steps) == 1
    assert workflow.status == WorkflowStatus.DRAFT


def test_workflow_validation():
    """Test workflow validation."""
    # Valid workflow
    workflow = Workflow(
        id="test-1",
        name="Valid Workflow",
        steps=[
            WorkflowStep(
                id="step_1",
                name="Step 1",
                service="service_a",
                action="action_a",
            )
        ],
    )
    errors = workflow.validate()
    assert len(errors) == 0

    # Invalid: no name
    workflow_no_name = Workflow(
        id="test-2",
        name="",
        steps=[
            WorkflowStep(
                id="step_1",
                name="Step 1",
                service="service_a",
                action="action_a",
            )
        ],
    )
    errors = workflow_no_name.validate()
    assert "Workflow name is required" in errors

    # Invalid: no steps
    workflow_no_steps = Workflow(
        id="test-3",
        name="No Steps",
        steps=[],
    )
    errors = workflow_no_steps.validate()
    assert "Workflow must have at least one step" in errors


def test_workflow_execution_order():
    """Test that steps are ordered correctly by dependencies."""
    workflow = Workflow(
        id="test-order",
        name="Order Test",
        steps=[
            WorkflowStep(
                id="step_3",
                name="Third",
                service="svc",
                action="act",
                depends_on=["step_2"],
            ),
            WorkflowStep(
                id="step_1",
                name="First",
                service="svc",
                action="act",
            ),
            WorkflowStep(
                id="step_2",
                name="Second",
                service="svc",
                action="act",
                depends_on=["step_1"],
            ),
        ],
    )

    order = workflow.get_execution_order()
    step_ids = [s.id for s in order]

    # step_1 must come before step_2, step_2 must come before step_3
    assert step_ids.index("step_1") < step_ids.index("step_2")
    assert step_ids.index("step_2") < step_ids.index("step_3")


def test_workflow_circular_dependency_detection():
    """Test that circular dependencies are detected."""
    workflow = Workflow(
        id="test-circular",
        name="Circular Test",
        steps=[
            WorkflowStep(
                id="step_1",
                name="Step 1",
                service="svc",
                action="act",
                depends_on=["step_2"],
            ),
            WorkflowStep(
                id="step_2",
                name="Step 2",
                service="svc",
                action="act",
                depends_on=["step_1"],
            ),
        ],
    )

    errors = workflow.validate()
    assert "Workflow has circular dependencies" in errors


def test_workflow_step_input_resolution():
    """Test that step inputs are resolved correctly."""
    step = WorkflowStep(
        id="test_step",
        name="Test",
        service="svc",
        action="act",
        inputs={
            "name": "{{trigger.data.user_name}}",
            "email": "{{steps.step_1.output.email}}",
            "static": "hello",
        },
    )

    context = {
        "trigger": {"data": {"user_name": "John"}},
        "steps": {"step_1": {"output": {"email": "john@example.com"}}},
    }

    resolved = step.resolve_inputs(context)

    assert resolved["name"] == "John"
    assert resolved["email"] == "john@example.com"
    assert resolved["static"] == "hello"


def test_workflow_serialization():
    """Test workflow to/from dict conversion."""
    original = Workflow(
        id="test-serial",
        name="Serialization Test",
        description="Testing serialization",
        trigger=Trigger(
            trigger_type=TriggerType.WEBHOOK,
            service="github",
            event="push",
        ),
        steps=[
            WorkflowStep(
                id="step_1",
                name="First",
                service="slack",
                action="send_message",
                inputs={"channel": "#general"},
            )
        ],
    )

    # Convert to dict and back
    data = original.to_dict()
    restored = Workflow.from_dict(data)

    assert restored.id == original.id
    assert restored.name == original.name
    assert restored.trigger.trigger_type == original.trigger.trigger_type
    assert restored.trigger.service == original.trigger.service
    assert len(restored.steps) == len(original.steps)
    assert restored.steps[0].inputs == original.steps[0].inputs
