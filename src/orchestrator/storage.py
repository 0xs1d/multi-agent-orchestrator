from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from dataclasses import asdict, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Protocol

from .models import AgentResult, Response, Route, StreamEvent, Usage


def _jsonable(value):
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {key: _jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


class EventSink(Protocol):
    async def append(self, event: StreamEvent) -> None: ...


class NullEventSink:
    async def append(self, event: StreamEvent) -> None:
        return None


class InMemoryEventSink:
    def __init__(self):
        self.events: list[StreamEvent] = []
        self._lock = asyncio.Lock()

    async def append(self, event: StreamEvent) -> None:
        async with self._lock:
            self.events.append(event)


class JsonlEventSink:
    """Append-only audit sink. A log shipper can tail this file in deployment."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._lock = asyncio.Lock()

    async def append(self, event: StreamEvent) -> None:
        line = json.dumps(_jsonable(event), sort_keys=True) + "\n"
        async with self._lock:
            # The local adapter writes one small, flushed record while holding the
            # lock. Production deployments should swap this port for an async
            # log shipper or database writer rather than block on local storage.
            self._append(line)

    def _append(self, line: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()


class SQLiteEventSink:
    """Transactional local database sink for events and simple replay queries."""

    def __init__(self, path: str):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._setup()

    def _setup(self):
        with sqlite3.connect(self.path) as db:
            db.execute("CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, request_id TEXT NOT NULL, event_type TEXT NOT NULL, payload TEXT NOT NULL, created_at REAL NOT NULL)")

    async def append(self, event: StreamEvent) -> None:
        with sqlite3.connect(self.path) as db:
            db.execute("INSERT INTO events(request_id,event_type,payload,created_at) VALUES(?,?,?,?)", (event.request_id, event.type.value, json.dumps(_jsonable(event.data), sort_keys=True), time.time()))

    def for_request(self, request_id: str) -> list[dict]:
        with sqlite3.connect(self.path) as db:
            rows = db.execute("SELECT event_type,payload,created_at FROM events WHERE request_id=? ORDER BY id", (request_id,)).fetchall()
        return [{"type": event_type, "data": json.loads(payload), "created_at": created_at} for event_type, payload, created_at in rows]


class IdempotencyStore(Protocol):
    async def get(self, tenant_id: str, key: str) -> Response | None: ...
    async def put(self, tenant_id: str, key: str, response: Response) -> None: ...


class InMemoryIdempotencyStore:
    def __init__(self):
        self._values: dict[tuple[str, str], Response] = {}
        self._lock = asyncio.Lock()

    async def get(self, tenant_id: str, key: str) -> Response | None:
        async with self._lock:
            return self._values.get((tenant_id, key))

    async def put(self, tenant_id: str, key: str, response: Response) -> None:
        async with self._lock:
            self._values.setdefault((tenant_id, key), response)


class JsonFileIdempotencyStore:
    """Tiny durable store for demos; use a transactional database in production."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._lock = asyncio.Lock()

    async def get(self, tenant_id: str, key: str) -> Response | None:
        async with self._lock:
            values = self._read()
            raw = values.get(f"{tenant_id}:{key}")
            if raw is None:
                return None
            return Response(raw["request_id"], raw["text"], Route(raw["route"]),
                            [AgentResult(item["agent"], item["text"], Usage(**item["usage"]), item["success"], item.get("error")) for item in raw["results"]],
                            Usage(**raw["usage"]), raw.get("killed", False), True, raw.get("request_fingerprint", ""))

    async def put(self, tenant_id: str, key: str, response: Response) -> None:
        async with self._lock:
            values = self._read()
            values.setdefault(f"{tenant_id}:{key}", _jsonable(response))
            self._write(values)

    def _read(self) -> dict:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))

    def _write(self, values: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(json.dumps(values, sort_keys=True), encoding="utf-8")
        temporary.replace(self.path)


class SQLiteIdempotencyStore:
    """Transactional idempotency table with TTL and ownership claims."""

    def __init__(self, path: str, ttl_s: float = 86_400):
        self.path = path
        self.ttl_s = ttl_s
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._setup()

    def _connect(self):
        return sqlite3.connect(self.path, timeout=5)

    def _setup(self):
        with self._connect() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS idempotency (
                tenant_id TEXT NOT NULL, key TEXT NOT NULL, fingerprint TEXT NOT NULL,
                status TEXT NOT NULL, owner TEXT, expires_at REAL NOT NULL, response TEXT,
                PRIMARY KEY(tenant_id, key)
            )""")

    async def claim(self, tenant_id: str, key: str, fingerprint: str, owner: str) -> str:
        return self._claim(tenant_id, key, fingerprint, owner)

    def _claim(self, tenant_id, key, fingerprint, owner):
        now = time.time()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT fingerprint,status,expires_at FROM idempotency WHERE tenant_id=? AND key=?", (tenant_id, key)).fetchone()
            if row and row[2] <= now:
                db.execute("DELETE FROM idempotency WHERE tenant_id=? AND key=?", (tenant_id, key))
                row = None
            if row:
                if row[0] != fingerprint:
                    return "conflict"
                return "completed" if row[1] == "completed" else "busy"
            db.execute("INSERT INTO idempotency VALUES(?,?,?,?,?,?,?)", (tenant_id, key, fingerprint, "running", owner, now + self.ttl_s, None))
            return "claimed"

    async def get(self, tenant_id: str, key: str) -> Response | None:
        return self._get(tenant_id, key)

    def _get(self, tenant_id, key):
        with self._connect() as db:
            row = db.execute("SELECT response,expires_at FROM idempotency WHERE tenant_id=? AND key=? AND status='completed'", (tenant_id, key)).fetchone()
            if not row or row[1] <= time.time():
                return None
            raw = json.loads(row[0])
            return Response(raw["request_id"], raw["text"], Route(raw["route"]), [AgentResult(item["agent"], item["text"], Usage(**item["usage"]), item["success"], item.get("error")) for item in raw["results"]], Usage(**raw["usage"]), raw.get("killed", False), True, raw.get("request_fingerprint", ""))

    async def put(self, tenant_id: str, key: str, response: Response) -> None:
        self._put(tenant_id, key, response)

    def _put(self, tenant_id, key, response):
        with self._connect() as db:
            db.execute("UPDATE idempotency SET status='completed', response=?, expires_at=? WHERE tenant_id=? AND key=?", (json.dumps(_jsonable(response)), time.time() + self.ttl_s, tenant_id, key))

    async def release(self, tenant_id: str, key: str, owner: str) -> None:
        self._release(tenant_id, key, owner)

    def _release(self, tenant_id, key, owner):
        with self._connect() as db:
            db.execute("DELETE FROM idempotency WHERE tenant_id=? AND key=? AND owner=? AND status='running'", (tenant_id, key, owner))

    async def purge_expired(self) -> int:
        return self._purge()

    def _purge(self):
        with self._connect() as db:
            cursor = db.execute("DELETE FROM idempotency WHERE expires_at <= ?", (time.time(),))
            return cursor.rowcount
