from __future__ import annotations

import asyncio
import sqlite3
import time
from dataclasses import dataclass
from typing import Protocol


@dataclass(slots=True)
class Bucket:
    tokens: float
    updated_at: float


class TenantRateLimiter:
    """Small in-process token bucket; replace with Redis for multi-process deployment."""

    def __init__(self, rate_per_second: float = 1000.0, burst: int = 100):
        self.rate_per_second = rate_per_second
        self.burst = float(burst)
        self._buckets: dict[str, Bucket] = {}
        self._lock = asyncio.Lock()

    async def acquire(self, tenant_id: str, tokens: int = 1) -> tuple[bool, float]:
        async with self._lock:
            now = time.monotonic()
            bucket = self._buckets.setdefault(tenant_id, Bucket(self.burst, now))
            bucket.tokens = min(self.burst, bucket.tokens + (now - bucket.updated_at) * self.rate_per_second)
            bucket.updated_at = now
            if bucket.tokens >= tokens:
                bucket.tokens -= tokens
                return True, 0.0
            wait = (tokens - bucket.tokens) / self.rate_per_second if self.rate_per_second else float("inf")
            return False, wait


class SQLiteTenantPolicyStore:
    """SQLite-backed shared rate/budget state with transactional updates.

    It is suitable for a single host or shared filesystem. Replace the same
    methods with Redis/Postgres for a multi-host deployment.
    """

    def __init__(self, path: str):
        self.path = path
        import pathlib
        pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._setup()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=5)
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    def _setup(self):
        with self._connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS tenant_buckets (
                    tenant_id TEXT PRIMARY KEY, tokens REAL NOT NULL, updated_at REAL NOT NULL,
                    spent REAL NOT NULL DEFAULT 0
                );
            """)

    async def acquire(self, tenant_id: str, tokens: int = 1, *, rate_per_second: float = 1000.0, burst: int = 100) -> tuple[bool, float]:
        return self._acquire(tenant_id, tokens, rate_per_second, burst)

    def _acquire(self, tenant_id, tokens, rate, burst):
        now = time.monotonic()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT tokens, updated_at FROM tenant_buckets WHERE tenant_id=?", (tenant_id,)).fetchone()
            available, updated = (float(burst), now) if row is None else row
            available = min(float(burst), available + (now - updated) * rate)
            if available < tokens:
                db.execute("INSERT INTO tenant_buckets(tenant_id,tokens,updated_at) VALUES(?,?,?) ON CONFLICT(tenant_id) DO UPDATE SET tokens=excluded.tokens,updated_at=excluded.updated_at", (tenant_id, available, now))
                return False, (tokens - available) / rate if rate else float("inf")
            available -= tokens
            db.execute("INSERT INTO tenant_buckets(tenant_id,tokens,updated_at) VALUES(?,?,?) ON CONFLICT(tenant_id) DO UPDATE SET tokens=excluded.tokens,updated_at=excluded.updated_at", (tenant_id, available, now))
            return True, 0.0

    async def record(self, tenant_id: str, amount: float, limit: float | None) -> tuple[bool, float]:
        return self._record(tenant_id, amount, limit)

    def _record(self, tenant_id, amount, limit):
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT spent FROM tenant_buckets WHERE tenant_id=?", (tenant_id,)).fetchone()
            spent = (row[0] if row else 0.0) + amount
            if limit is not None and spent > limit:
                return False, spent
            db.execute("INSERT INTO tenant_buckets(tenant_id,tokens,updated_at,spent) VALUES(?,?,?,?) ON CONFLICT(tenant_id) DO UPDATE SET spent=excluded.spent", (tenant_id, 0.0, time.monotonic(), spent))
            return True, spent

    async def spent(self, tenant_id: str) -> float:
        return self._spent(tenant_id)

    def _spent(self, tenant_id):
        with self._connect() as db:
            row = db.execute("SELECT spent FROM tenant_buckets WHERE tenant_id=?", (tenant_id,)).fetchone()
            return row[0] if row else 0.0


class TenantBudgetLedger:
    """Tracks spend by tenant. The ledger is intentionally explicit and injectable."""

    def __init__(self):
        self._spent: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def record(self, tenant_id: str, amount: float, limit: float | None) -> tuple[bool, float]:
        async with self._lock:
            next_total = self._spent.get(tenant_id, 0.0) + amount
            if limit is not None and next_total > limit:
                return False, next_total
            self._spent[tenant_id] = next_total
            return True, next_total

    async def spent(self, tenant_id: str) -> float:
        async with self._lock:
            return self._spent.get(tenant_id, 0.0)


class RetryPolicy(Protocol):
    def retryable(self, error: BaseException) -> bool: ...


class DefaultRetryPolicy:
    """Retries transient-looking failures, not authorization or input errors."""

    def retryable(self, error: BaseException) -> bool:
        return not isinstance(error, (PermissionError, ValueError, TypeError))


class ProviderRetryPolicy:
    """Provider-aware policy that retries only explicitly transient exception types."""

    def __init__(self, transient_types: tuple[type[BaseException], ...] = (TimeoutError, ConnectionError, OSError)):
        self.transient_types = transient_types

    def retryable(self, error: BaseException) -> bool:
        return isinstance(error, self.transient_types) and not isinstance(error, PermissionError)


@dataclass(slots=True)
class BreakerState:
    failures: int = 0
    opened_at: float | None = None


class CircuitBreakerRegistry:
    """Per-agent circuit breaker; half-opens after a recovery interval."""

    def __init__(self, failure_threshold: int = 3, recovery_s: float = 30.0):
        self.failure_threshold = failure_threshold
        self.recovery_s = recovery_s
        self._states: dict[str, BreakerState] = {}
        self._lock = asyncio.Lock()

    async def allow(self, name: str) -> bool:
        async with self._lock:
            state = self._states.setdefault(name, BreakerState())
            if state.opened_at is None:
                return True
            if time.monotonic() - state.opened_at >= self.recovery_s:
                state.opened_at = None
                state.failures = 0
                return True
            return False

    async def success(self, name: str) -> None:
        async with self._lock:
            self._states[name] = BreakerState()

    async def failure(self, name: str) -> None:
        async with self._lock:
            state = self._states.setdefault(name, BreakerState())
            state.failures += 1
            if state.failures >= self.failure_threshold:
                state.opened_at = time.monotonic()
