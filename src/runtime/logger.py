"""
Execution Logger

Persistent logging for workflow executions with real-time updates.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable
from enum import Enum

from ..db.database import get_db

logger = logging.getLogger(__name__)


class LogLevel(Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class LogEntry:
    """A single log entry."""
    timestamp: datetime
    level: LogLevel
    message: str
    step_id: str | None = None
    data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "level": self.level.value,
            "message": self.message,
            "step_id": self.step_id,
            "data": self.data,
        }


@dataclass
class ExecutionLog:
    """Complete execution log for a workflow run."""
    run_id: str
    workflow_id: str
    workflow_name: str
    status: str
    started_at: datetime
    completed_at: datetime | None = None
    duration_ms: int | None = None
    trigger_data: dict[str, Any] = field(default_factory=dict)
    step_results: dict[str, Any] = field(default_factory=dict)
    logs: list[LogEntry] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "workflow_id": self.workflow_id,
            "workflow_name": self.workflow_name,
            "status": self.status,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_ms": self.duration_ms,
            "trigger_data": self.trigger_data,
            "step_results": self.step_results,
            "logs": [log.to_dict() for log in self.logs],
            "error": self.error,
        }


class ExecutionLogger:
    """Logs workflow executions to the database."""

    def __init__(self):
        self._current_logs: dict[str, ExecutionLog] = {}
        self._on_log_callbacks: list[Callable[[str, LogEntry], None]] = []

    def on_log(self, callback: Callable[[str, LogEntry], None]):
        """Register a callback for real-time log updates."""
        self._on_log_callbacks.append(callback)

    def start_execution(
        self,
        run_id: str,
        workflow_id: str,
        workflow_name: str,
        trigger_data: dict[str, Any],
    ) -> ExecutionLog:
        """Start logging a new execution."""
        execution_log = ExecutionLog(
            run_id=run_id,
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            status="running",
            started_at=datetime.utcnow(),
            trigger_data=trigger_data,
        )

        self._current_logs[run_id] = execution_log

        self._log(
            run_id,
            LogLevel.INFO,
            f"Starting workflow: {workflow_name}",
            data={"trigger_data": trigger_data},
        )

        return execution_log

    def log_step_start(self, run_id: str, step_id: str, step_name: str):
        """Log step start."""
        self._log(
            run_id,
            LogLevel.INFO,
            f"Starting step: {step_name}",
            step_id=step_id,
        )

    def log_step_complete(
        self,
        run_id: str,
        step_id: str,
        step_name: str,
        result: dict[str, Any],
    ):
        """Log step completion."""
        level = LogLevel.INFO if result.get("status") == "success" else LogLevel.ERROR

        self._log(
            run_id,
            level,
            f"Step completed: {step_name} - {result.get('status', 'unknown')}",
            step_id=step_id,
            data={"result": result},
        )

        # Store step result
        if run_id in self._current_logs:
            self._current_logs[run_id].step_results[step_id] = result

    def log_step_skip(self, run_id: str, step_id: str, step_name: str, reason: str):
        """Log step skip."""
        self._log(
            run_id,
            LogLevel.WARNING,
            f"Step skipped: {step_name} - {reason}",
            step_id=step_id,
        )

    def log_step_error(self, run_id: str, step_id: str, step_name: str, error: str):
        """Log step error."""
        self._log(
            run_id,
            LogLevel.ERROR,
            f"Step failed: {step_name} - {error}",
            step_id=step_id,
            data={"error": error},
        )

    def log_message(
        self,
        run_id: str,
        level: LogLevel,
        message: str,
        step_id: str | None = None,
        data: dict[str, Any] | None = None,
    ):
        """Log a custom message."""
        self._log(run_id, level, message, step_id=step_id, data=data or {})

    def complete_execution(
        self,
        run_id: str,
        status: str,
        error: str | None = None,
    ) -> ExecutionLog | None:
        """Complete logging an execution and save to database."""
        if run_id not in self._current_logs:
            return None

        execution_log = self._current_logs[run_id]
        execution_log.status = status
        execution_log.completed_at = datetime.utcnow()
        execution_log.error = error

        if execution_log.started_at:
            delta = execution_log.completed_at - execution_log.started_at
            execution_log.duration_ms = int(delta.total_seconds() * 1000)

        self._log(
            run_id,
            LogLevel.INFO if status == "completed" else LogLevel.ERROR,
            f"Workflow {status}: {execution_log.workflow_name}",
            data={"duration_ms": execution_log.duration_ms, "error": error},
        )

        # Save to database
        self._persist_execution(execution_log)

        # Clean up
        del self._current_logs[run_id]

        return execution_log

    def get_run_logs(self, run_id: str) -> list[dict[str, Any]]:
        """Get logs for a specific run."""
        db = get_db()
        run = db.get_run(run_id)
        if run:
            return run.step_results
        return []

    def get_workflow_runs(
        self,
        workflow_id: str,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Get recent runs for a workflow."""
        db = get_db()
        runs = db.get_workflow_runs(workflow_id, limit)
        return [
            {
                "id": run.id,
                "status": run.status,
                "started_at": run.started_at.isoformat(),
                "completed_at": run.completed_at.isoformat() if run.completed_at else None,
                "duration_ms": run.duration_ms,
                "error": run.error_message,
            }
            for run in runs
        ]

    def get_recent_runs(self, user_id: str, limit: int = 50) -> list[dict[str, Any]]:
        """Get recent runs for a user."""
        db = get_db()
        runs = db.get_user_runs(user_id, limit)
        return [
            {
                "id": run.id,
                "workflow_id": run.workflow_id,
                "status": run.status,
                "started_at": run.started_at.isoformat(),
                "completed_at": run.completed_at.isoformat() if run.completed_at else None,
                "duration_ms": run.duration_ms,
                "error": run.error_message,
            }
            for run in runs
        ]

    def get_execution_stats(self, workflow_id: str) -> dict[str, Any]:
        """Get execution statistics for a workflow."""
        db = get_db()
        runs = db.get_workflow_runs(workflow_id, 100)

        if not runs:
            return {
                "total_runs": 0,
                "success_count": 0,
                "failure_count": 0,
                "success_rate": 0,
                "avg_duration_ms": 0,
            }

        total = len(runs)
        success = sum(1 for r in runs if r.status == "completed")
        failed = sum(1 for r in runs if r.status == "failed")
        durations = [r.duration_ms for r in runs if r.duration_ms]
        avg_duration = sum(durations) / len(durations) if durations else 0

        return {
            "total_runs": total,
            "success_count": success,
            "failure_count": failed,
            "success_rate": round(success / total * 100, 1) if total > 0 else 0,
            "avg_duration_ms": round(avg_duration),
            "last_run": runs[0].started_at.isoformat() if runs else None,
        }

    def _log(
        self,
        run_id: str,
        level: LogLevel,
        message: str,
        step_id: str | None = None,
        data: dict[str, Any] | None = None,
    ):
        """Internal logging method."""
        entry = LogEntry(
            timestamp=datetime.utcnow(),
            level=level,
            message=message,
            step_id=step_id,
            data=data or {},
        )

        if run_id in self._current_logs:
            self._current_logs[run_id].logs.append(entry)

        # Notify callbacks
        for callback in self._on_log_callbacks:
            try:
                callback(run_id, entry)
            except Exception as e:
                logger.error(f"Log callback error: {e}")

        # Also log to standard logger
        log_func = getattr(logger, level.value)
        log_func(f"[{run_id[:8]}] {message}")

    def _persist_execution(self, execution_log: ExecutionLog):
        """Save execution to database."""
        try:
            db = get_db()

            # Update the run record
            db.update_run(
                run_id=execution_log.run_id,
                status=execution_log.status,
                step_results=list(execution_log.step_results.values()),
                error_message=execution_log.error,
                completed=True,
            )

            logger.info(f"Saved execution log: {execution_log.run_id}")

        except Exception as e:
            logger.error(f"Failed to persist execution log: {e}")


# Global logger instance
_execution_logger: ExecutionLogger | None = None


def get_execution_logger() -> ExecutionLogger:
    """Get the global execution logger."""
    global _execution_logger
    if _execution_logger is None:
        _execution_logger = ExecutionLogger()
    return _execution_logger
