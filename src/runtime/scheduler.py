"""
Workflow Scheduler

Handles scheduled workflow execution using cron expressions.
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Callable

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from ..core.workflow import TriggerType, Workflow

logger = logging.getLogger(__name__)


class WorkflowScheduler:
    """
    Manages scheduled workflow executions.

    Uses APScheduler for cron-based scheduling with support for:
    - Cron expressions
    - Interval-based polling
    - One-time scheduled runs
    """

    def __init__(
        self,
        execute_callback: Callable[[Workflow, dict[str, Any]], Any],
        timezone: str = "UTC",
    ):
        self.execute_callback = execute_callback
        self.scheduler = AsyncIOScheduler(timezone=timezone)
        self._scheduled_workflows: dict[str, str] = {}  # workflow_id -> job_id
        self._running = False

    def start(self):
        """Start the scheduler."""
        if not self._running:
            self.scheduler.start()
            self._running = True
            logger.info("Workflow scheduler started")

    def stop(self):
        """Stop the scheduler."""
        if self._running:
            self.scheduler.shutdown()
            self._running = False
            logger.info("Workflow scheduler stopped")

    def schedule_workflow(self, workflow: Workflow) -> str | None:
        """
        Schedule a workflow based on its trigger configuration.

        Returns the job ID if scheduled, None otherwise.
        """
        if not workflow.trigger:
            logger.warning(f"Workflow {workflow.id} has no trigger")
            return None

        trigger = workflow.trigger

        if trigger.trigger_type == TriggerType.SCHEDULE:
            if not trigger.schedule:
                logger.warning(f"Workflow {workflow.id} has schedule trigger but no cron expression")
                return None

            return self._schedule_cron(workflow, trigger.schedule)

        elif trigger.trigger_type == TriggerType.API_POLL:
            interval = trigger.poll_interval or 300  # Default 5 minutes
            return self._schedule_interval(workflow, interval)

        else:
            logger.debug(f"Workflow {workflow.id} has non-scheduled trigger type: {trigger.trigger_type}")
            return None

    def _schedule_cron(self, workflow: Workflow, cron_expression: str) -> str:
        """Schedule a workflow with a cron expression."""
        # Parse cron expression (supports standard 5-field format)
        try:
            parts = cron_expression.split()
            if len(parts) == 5:
                minute, hour, day, month, day_of_week = parts
            elif len(parts) == 6:
                # Support optional seconds
                second, minute, hour, day, month, day_of_week = parts
            else:
                raise ValueError(f"Invalid cron expression: {cron_expression}")

            trigger = CronTrigger(
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
            )

            job = self.scheduler.add_job(
                self._execute_workflow,
                trigger=trigger,
                args=[workflow],
                id=f"workflow_{workflow.id}",
                name=f"Workflow: {workflow.name}",
                replace_existing=True,
            )

            self._scheduled_workflows[workflow.id] = job.id
            logger.info(f"Scheduled workflow {workflow.id} with cron: {cron_expression}")
            return job.id

        except Exception as e:
            logger.error(f"Failed to schedule workflow {workflow.id}: {e}")
            return None

    def _schedule_interval(self, workflow: Workflow, interval_seconds: int) -> str:
        """Schedule a workflow with an interval."""
        job = self.scheduler.add_job(
            self._execute_workflow,
            "interval",
            seconds=interval_seconds,
            args=[workflow],
            id=f"workflow_{workflow.id}",
            name=f"Workflow: {workflow.name}",
            replace_existing=True,
        )

        self._scheduled_workflows[workflow.id] = job.id
        logger.info(f"Scheduled workflow {workflow.id} with interval: {interval_seconds}s")
        return job.id

    def unschedule_workflow(self, workflow_id: str) -> bool:
        """Remove a workflow from the schedule."""
        if workflow_id not in self._scheduled_workflows:
            return False

        job_id = self._scheduled_workflows[workflow_id]
        try:
            self.scheduler.remove_job(job_id)
            del self._scheduled_workflows[workflow_id]
            logger.info(f"Unscheduled workflow {workflow_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to unschedule workflow {workflow_id}: {e}")
            return False

    def pause_workflow(self, workflow_id: str) -> bool:
        """Pause a scheduled workflow."""
        if workflow_id not in self._scheduled_workflows:
            return False

        job_id = self._scheduled_workflows[workflow_id]
        try:
            self.scheduler.pause_job(job_id)
            logger.info(f"Paused workflow {workflow_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to pause workflow {workflow_id}: {e}")
            return False

    def resume_workflow(self, workflow_id: str) -> bool:
        """Resume a paused workflow."""
        if workflow_id not in self._scheduled_workflows:
            return False

        job_id = self._scheduled_workflows[workflow_id]
        try:
            self.scheduler.resume_job(job_id)
            logger.info(f"Resumed workflow {workflow_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to resume workflow {workflow_id}: {e}")
            return False

    def get_next_run_time(self, workflow_id: str) -> datetime | None:
        """Get the next scheduled run time for a workflow."""
        if workflow_id not in self._scheduled_workflows:
            return None

        job_id = self._scheduled_workflows[workflow_id]
        job = self.scheduler.get_job(job_id)
        return job.next_run_time if job else None

    def get_scheduled_workflows(self) -> dict[str, dict[str, Any]]:
        """Get info about all scheduled workflows."""
        result = {}
        for workflow_id, job_id in self._scheduled_workflows.items():
            job = self.scheduler.get_job(job_id)
            if job:
                result[workflow_id] = {
                    "job_id": job_id,
                    "name": job.name,
                    "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
                    "pending": job.pending,
                }
        return result

    async def _execute_workflow(self, workflow: Workflow):
        """Execute a workflow (called by scheduler)."""
        logger.info(f"Executing scheduled workflow: {workflow.id}")
        try:
            trigger_data = {
                "trigger_type": "schedule",
                "scheduled_time": datetime.utcnow().isoformat(),
            }
            result = self.execute_callback(workflow, trigger_data)
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            logger.error(f"Scheduled workflow {workflow.id} failed: {e}")

    def run_once(self, workflow: Workflow, run_at: datetime) -> str | None:
        """Schedule a one-time workflow execution."""
        try:
            job = self.scheduler.add_job(
                self._execute_workflow,
                "date",
                run_date=run_at,
                args=[workflow],
                id=f"workflow_{workflow.id}_once_{run_at.timestamp()}",
                name=f"Workflow (one-time): {workflow.name}",
            )
            logger.info(f"Scheduled one-time workflow {workflow.id} for {run_at}")
            return job.id
        except Exception as e:
            logger.error(f"Failed to schedule one-time workflow {workflow.id}: {e}")
            return None
