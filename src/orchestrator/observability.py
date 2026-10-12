from __future__ import annotations

import re
import time
from collections import Counter
from dataclasses import dataclass

from .models import EventType, StreamEvent


class Redactor:
    """Best-effort log redaction; callers should still avoid sending secrets as prompts."""

    _patterns = (
        (re.compile(r"(?i)bearer\s+[A-Za-z0-9._~+/=-]+"), "Bearer [REDACTED]"),
        (re.compile(r"(?i)(api[_-]?key|token|secret)\s*[:=]\s*[^\s,;]+"), r"\1=[REDACTED]"),
        (re.compile(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}"), "[EMAIL REDACTED]"),
    )

    def text(self, value: str) -> str:
        for pattern, replacement in self._patterns:
            value = pattern.sub(replacement, value)
        return value

    def data(self, value: dict) -> dict:
        redacted = dict(value)
        for key in ("text", "query", "reason", "error"):
            if isinstance(redacted.get(key), str):
                redacted[key] = self.text(redacted[key])
        return redacted


@dataclass(frozen=True, slots=True)
class MetricsSnapshot:
    events: dict[str, int]
    tokens: int
    started_at: float


class Metrics:
    """Vendor-neutral metrics hook that can be exported to Prometheus/OpenTelemetry."""

    def __init__(self):
        self._events: Counter[str] = Counter()
        self._tokens = 0
        self._started_at = time.time()

    def record(self, event: StreamEvent) -> None:
        self._events[event.type.value] += 1
        if event.type == EventType.TOKEN:
            self._tokens += 1

    def snapshot(self) -> MetricsSnapshot:
        return MetricsSnapshot(dict(self._events), self._tokens, self._started_at)


@dataclass(frozen=True, slots=True)
class AlertThresholds:
    max_kills: int = 0
    max_failures: int = 10
    max_failure_ratio: float = 0.5


class AlertEvaluator:
    def evaluate(self, snapshot: MetricsSnapshot, thresholds: AlertThresholds) -> list[str]:
        failures = snapshot.events.get(EventType.AGENT_FAILED.value, 0)
        completed = snapshot.events.get(EventType.AGENT_COMPLETED.value, 0)
        total = failures + completed
        alerts = []
        if snapshot.events.get(EventType.KILLED.value, 0) > thresholds.max_kills:
            alerts.append("kill-switch threshold exceeded")
        if failures > thresholds.max_failures:
            alerts.append("agent failure threshold exceeded")
        if total and failures / total > thresholds.max_failure_ratio:
            alerts.append("agent failure ratio threshold exceeded")
        return alerts


class TraceCollector:
    """Minimal span collector; export its spans to OpenTelemetry in deployment."""

    def __init__(self):
        self.spans: list[dict[str, object]] = []

    def start(self, name: str, trace_id: str) -> int:
        self.spans.append({"name": name, "trace_id": trace_id, "started": time.time()})
        return len(self.spans) - 1

    def end(self, span_id: int, *, status: str = "ok") -> None:
        self.spans[span_id]["ended"] = time.time()
        self.spans[span_id]["status"] = status
