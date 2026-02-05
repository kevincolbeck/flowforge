"""
Database models for FlowForge
SQLAlchemy models for PostgreSQL
"""
from datetime import datetime
from typing import Optional
from sqlalchemy import (
    Column, String, Integer, DateTime, Boolean, JSON, Text, ForeignKey, Index, Enum as SQLEnum
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import enum

Base = declarative_base()


class WorkflowStatus(str, enum.Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    ARCHIVED = "archived"


class ExecutionStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class User(Base):
    """User/Organization model for multi-tenancy"""
    __tablename__ = "users"

    id = Column(String(36), primary_key=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    name = Column(String(255))
    api_key = Column(String(64), unique=True, index=True)  # For API authentication
    organization = Column(String(255))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    workflows = relationship("Workflow", back_populates="user", cascade="all, delete-orphan")
    credentials = relationship("Credential", back_populates="user", cascade="all, delete-orphan")
    executions = relationship("WorkflowExecution", back_populates="user")


class Workflow(Base):
    """Workflow definition"""
    __tablename__ = "workflows"

    id = Column(String(36), primary_key=True)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    trigger = Column(JSON)  # Trigger configuration
    steps = Column(JSON, nullable=False)  # List of workflow steps
    status = Column(SQLEnum(WorkflowStatus), default=WorkflowStatus.DRAFT, index=True)
    tags = Column(JSON, default=list)
    extra_data = Column(JSON, default=dict)  # Renamed from metadata (reserved word)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    user = relationship("User", back_populates="workflows")
    executions = relationship("WorkflowExecution", back_populates="workflow", cascade="all, delete-orphan")

    __table_args__ = (
        Index("idx_user_status", "user_id", "status"),
        Index("idx_user_created", "user_id", "created_at"),
    )


class Credential(Base):
    """Encrypted credential storage"""
    __tablename__ = "credentials"

    id = Column(String(36), primary_key=True)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False, index=True)
    service = Column(String(100), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    credential_type = Column(String(50), nullable=False)  # api_key, oauth2, etc.
    encrypted_data = Column(Text, nullable=False)  # Encrypted JSON
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    expires_at = Column(DateTime, nullable=True)

    # Relationships
    user = relationship("User", back_populates="credentials")

    __table_args__ = (
        Index("idx_user_service", "user_id", "service"),
    )


class WorkflowExecution(Base):
    """Workflow execution log"""
    __tablename__ = "workflow_executions"

    id = Column(String(36), primary_key=True)
    workflow_id = Column(String(36), ForeignKey("workflows.id"), nullable=False, index=True)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False, index=True)
    status = Column(SQLEnum(ExecutionStatus), default=ExecutionStatus.PENDING, index=True)
    trigger_data = Column(JSON, default=dict)
    step_results = Column(JSON, default=dict)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime, default=datetime.utcnow, index=True)
    completed_at = Column(DateTime, nullable=True)
    duration_ms = Column(Integer, nullable=True)

    # Relationships
    workflow = relationship("Workflow", back_populates="executions")
    user = relationship("User", back_populates="executions")
    logs = relationship("ExecutionLog", back_populates="execution", cascade="all, delete-orphan")

    __table_args__ = (
        Index("idx_workflow_status", "workflow_id", "status"),
        Index("idx_user_started", "user_id", "started_at"),
    )


class ExecutionLog(Base):
    """Detailed execution logs"""
    __tablename__ = "execution_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    execution_id = Column(String(36), ForeignKey("workflow_executions.id"), nullable=False, index=True)
    step_id = Column(String(100))
    level = Column(String(20))  # INFO, WARNING, ERROR
    message = Column(Text)
    log_data = Column(JSON, default=dict)  # Renamed from metadata (reserved word)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)

    # Relationships
    execution = relationship("WorkflowExecution", back_populates="logs")


class WebhookConfig(Base):
    """Webhook registration"""
    __tablename__ = "webhook_configs"

    id = Column(String(36), primary_key=True)
    workflow_id = Column(String(36), ForeignKey("workflows.id"), nullable=False, index=True)
    path = Column(String(255), unique=True, nullable=False, index=True)
    secret = Column(String(255), nullable=True)
    service = Column(String(100))
    event = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)


class ScheduledJob(Base):
    """Scheduled workflow jobs"""
    __tablename__ = "scheduled_jobs"

    id = Column(String(36), primary_key=True)
    workflow_id = Column(String(36), ForeignKey("workflows.id"), nullable=False, index=True)
    schedule = Column(String(100), nullable=False)  # Cron expression
    next_run_at = Column(DateTime, index=True)
    last_run_at = Column(DateTime, nullable=True)
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class APIUsage(Base):
    """API usage tracking for rate limiting"""
    __tablename__ = "api_usage"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False, index=True)
    endpoint = Column(String(255))
    method = Column(String(10))
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    response_status = Column(Integer)
    duration_ms = Column(Integer)

    __table_args__ = (
        Index("idx_user_timestamp", "user_id", "timestamp"),
    )
