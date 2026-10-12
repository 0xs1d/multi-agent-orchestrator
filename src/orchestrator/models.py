from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Route(str, Enum):
    SINGLE = "single"
    AGENT = "agent"
    CHAIN = "chain"


class EventType(str, Enum):
    STARTED = "started"
    ROUTED = "routed"
    AGENT_STARTED = "agent_started"
    TOKEN = "token"
    AGENT_COMPLETED = "agent_completed"
    AGENT_FAILED = "agent_failed"
    PARTIAL = "partial"
    COMPLETED = "completed"
    KILLED = "killed"
    RATE_LIMITED = "rate_limited"
    BUDGET_EXCEEDED = "budget_exceeded"
    IDEMPOTENT_REPLAY = "idempotent_replay"


@dataclass(slots=True)
class Request:
    query: str
    request_id: str = "request-1"
    timeout_s: float = 30.0
    max_retries: int = 2
    tenant_id: str = "default"
    idempotency_key: str | None = None
    budget_usd: float | None = None
    trace_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Usage:
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0

    def add(self, other: "Usage") -> None:
        self.input_tokens += other.input_tokens
        self.output_tokens += other.output_tokens
        self.cost_usd += other.cost_usd


@dataclass(slots=True)
class AgentResult:
    agent: str
    text: str
    usage: Usage
    success: bool = True
    error: str | None = None


@dataclass(slots=True)
class StreamEvent:
    type: EventType
    request_id: str
    data: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Response:
    request_id: str
    text: str
    route: Route
    results: list[AgentResult]
    usage: Usage
    killed: bool = False
    cached: bool = False
    request_fingerprint: str = ""
