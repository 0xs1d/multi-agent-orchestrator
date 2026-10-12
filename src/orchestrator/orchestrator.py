from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from dataclasses import replace

from .agents import Agent, AgentContext, CodeAgent, MathAgent, PlannerAgent, SearchAgent, VisionAgent
from .content import ContentPolicy
from .models import AgentResult, EventType, Request, Response, Route, StreamEvent, Usage
from .observability import Metrics, Redactor, TraceCollector
from .policy import CircuitBreakerRegistry, DefaultRetryPolicy, RetryPolicy, TenantBudgetLedger, TenantRateLimiter
from .router import ExecutionPlan, PlanTask, RuleBasedClassifier
from .security import IdempotencyConflict, IdempotencyInProgress, Principal, RequestLimits, validate_request
from .storage import EventSink, IdempotencyStore, NullEventSink


Task = PlanTask


class Orchestrator:
    """Async coordinator with bounded retries, deadlines, and streamed events."""

    def __init__(self, agents: dict[str, Agent] | None = None, *, event_sink: EventSink | None = None,
                 rate_limiter: TenantRateLimiter | None = None, budget_ledger: TenantBudgetLedger | None = None,
                 idempotency_store: IdempotencyStore | None = None, classifier: RuleBasedClassifier | None = None,
                 limits: RequestLimits | None = None, principal: Principal | None = None,
                 circuit_breakers: CircuitBreakerRegistry | None = None, metrics: Metrics | None = None,
                 redactor: Redactor | None = None, queue_size: int = 256, fail_on_sink_error: bool = False,
                 retry_policy: RetryPolicy | None = None):
        self.agents = agents or {
            "planner": PlannerAgent(), "code": CodeAgent(), "search": SearchAgent(),
            "math": MathAgent(), "vision": VisionAgent(),
        }
        self.event_sink = event_sink or NullEventSink()
        self.rate_limiter = rate_limiter or TenantRateLimiter()
        self.budget_ledger = budget_ledger or TenantBudgetLedger()
        self.idempotency_store = idempotency_store
        self.classifier = classifier or RuleBasedClassifier()
        self.limits = limits or RequestLimits()
        self.principal = principal
        self.circuit_breakers = circuit_breakers or CircuitBreakerRegistry()
        self.metrics = metrics or Metrics()
        self.redactor = redactor or Redactor()
        self.queue_size = queue_size
        self.fail_on_sink_error = fail_on_sink_error
        self.retry_policy = retry_policy or DefaultRetryPolicy()
        self.content_policy = ContentPolicy()
        self.traces = TraceCollector()
        self._idempotency_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._idempotency_locks_guard = asyncio.Lock()

    async def stream(self, request: Request) -> AsyncIterator[StreamEvent]:
        validate_request(request, self.limits, self.principal)
        self.content_policy.enforce(request.query)
        if self.queue_size < 1:
            raise ValueError("queue_size must be positive")
        queue: asyncio.Queue[StreamEvent | None] = asyncio.Queue(maxsize=self.queue_size)
        producer = asyncio.create_task(self._produce(request, queue))
        try:
            while True:
                event = await queue.get()
                if event is None:
                    break
                yield event
        finally:
            if not producer.done():
                producer.cancel()
                await asyncio.gather(producer, return_exceptions=True)

    async def run(self, request: Request) -> Response:
        validate_request(request, self.limits, self.principal)
        self.content_policy.enforce(request.query)
        if request.idempotency_key and self.idempotency_store:
            async with await self._idempotency_lock(request.tenant_id, request.idempotency_key):
                cached = await self.idempotency_store.get(request.tenant_id, request.idempotency_key)
                fingerprint = self._fingerprint(request)
                owner = request.request_id
                claimed = False
                if hasattr(self.idempotency_store, "claim"):
                    state = await self.idempotency_store.claim(request.tenant_id, request.idempotency_key, fingerprint, owner)
                    if state == "conflict":
                        raise IdempotencyConflict("idempotency key was reused for a different request")
                    if state == "busy":
                        raise IdempotencyInProgress("idempotency key is owned by another request")
                    claimed = state == "claimed"
                    if state == "completed":
                        cached = await self.idempotency_store.get(request.tenant_id, request.idempotency_key)
                if cached:
                    if cached.request_fingerprint and cached.request_fingerprint != fingerprint:
                        raise IdempotencyConflict("idempotency key was reused for a different request")
                    replay = replace(cached, cached=True)
                    await self.event_sink.append(StreamEvent(EventType.IDEMPOTENT_REPLAY, request.request_id,
                                                             {"tenant_id": request.tenant_id, "key": request.idempotency_key}))
                    return replay
                response = await self._run_uncached(request, fingerprint)
                if not response.killed:
                    await self.idempotency_store.put(request.tenant_id, request.idempotency_key, response)
                elif claimed and hasattr(self.idempotency_store, "release"):
                    await self.idempotency_store.release(request.tenant_id, request.idempotency_key, owner)
                return response
        return await self._run_uncached(request, self._fingerprint(request))

    async def _idempotency_lock(self, tenant_id: str, key: str) -> asyncio.Lock:
        async with self._idempotency_locks_guard:
            return self._idempotency_locks.setdefault((tenant_id, key), asyncio.Lock())

    @staticmethod
    def _fingerprint(request: Request) -> str:
        payload = json.dumps({"query": request.query, "metadata": request.metadata,
                              "budget_usd": request.budget_usd}, sort_keys=True, default=str)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    async def _run_uncached(self, request: Request, fingerprint: str) -> Response:
        events: list[StreamEvent] = []
        async for event in self.stream(request):
            events.append(event)
        results = [AgentResult(e.data["agent"], e.data["text"], e.data["usage"],
                               e.type == EventType.AGENT_COMPLETED, e.data.get("error"))
                   for e in events if e.type in (EventType.AGENT_COMPLETED, EventType.AGENT_FAILED)]
        usage = Usage()
        for result in results:
            usage.add(result.usage)
        route = next((e.data["route"] for e in events if e.type == EventType.ROUTED), Route.SINGLE)
        text = "\n".join(r.text for r in results if r.success)
        killed = any(e.type == EventType.KILLED for e in events)
        return Response(request.request_id, text, route, results, usage, killed, False, fingerprint)

    async def _produce(self, request: Request, queue: asyncio.Queue[StreamEvent | None]) -> None:
        span = self.traces.start("orchestrator.request", request.trace_id or request.request_id)
        async def emit(event_type: EventType, **data: object) -> None:
            data.setdefault("trace_id", request.trace_id or request.request_id)
            event = StreamEvent(event_type, request.request_id, self.redactor.data(data))
            self.metrics.record(event)
            try:
                await self.event_sink.append(event)
            except Exception:
                if self.fail_on_sink_error:
                    raise
            await queue.put(event)

        try:
            await asyncio.wait_for(self._execute(request, emit), timeout=request.timeout_s)
        except asyncio.TimeoutError:
            await emit(EventType.KILLED, reason=f"total deadline of {request.timeout_s:.1f}s exceeded")
        finally:
            self.traces.end(span, status="ok")
            await queue.put(None)

    async def _execute(self, request: Request, emit) -> None:
        await emit(EventType.STARTED)
        plan = self.classifier.classify(request, set(self.agents))
        await emit(EventType.ROUTED, route=plan.route, reason=plan.reason,
                   tasks=[task.name for task in plan.tasks])
        tasks = list(plan.tasks)
        completed: dict[str, str] = {}
        results: list[AgentResult] = []
        while len(completed) < len(tasks):
            ready = [t for t in tasks if t.name not in completed and all(d in completed for d in t.depends_on)]
            if not ready:
                break
            batch = await asyncio.gather(*(self._run_task(t, request, completed, emit) for t in ready))
            for task, result in zip(ready, batch):
                results.append(result)
                if result.success:
                    completed[task.name] = result.text
                else:
                    await emit(EventType.PARTIAL, task=task.name, reason=result.error)
                    completed[task.name] = "[unavailable]"
        await emit(EventType.COMPLETED, successful=sum(r.success for r in results), total=len(results))

    async def _run_task(self, task: Task, request: Request, completed: dict[str, str], emit) -> AgentResult:
        agent = self.agents[task.agent]
        context = "\n".join(completed.values())
        if len(context) > self.limits.max_context_chars:
            context = context[-self.limits.max_context_chars:]
        query = request.query + ("\nContext:\n" + context if context else "")
        await emit(EventType.AGENT_STARTED, agent=agent.name, task=task.name)
        last_error = "unknown failure"
        for attempt in range(request.max_retries + 1):
            try:
                if not await self.circuit_breakers.allow(agent.name):
                    error = "agent circuit breaker is open"
                    await emit(EventType.AGENT_FAILED, agent=agent.name, task=task.name, text="", usage=Usage(), error=error)
                    return AgentResult(agent.name, "", Usage(), False, error)
                allowed, retry_after = await self.rate_limiter.acquire(request.tenant_id)
                if not allowed:
                    await emit(EventType.RATE_LIMITED, agent=agent.name, task=task.name, retry_after_s=retry_after)
                    return AgentResult(agent.name, "", Usage(), False, "tenant rate limit exceeded")
                async def token(text: str) -> None:
                    await emit(EventType.TOKEN, agent=agent.name, task=task.name, text=text, attempt=attempt + 1)
                result = await asyncio.wait_for(agent.run(query, AgentContext(request.request_id, attempt + 1, completed), token), timeout=min(10.0, request.timeout_s))
                await self.circuit_breakers.success(agent.name)
                allowed_budget, spent = await self.budget_ledger.record(request.tenant_id, result.usage.cost_usd, request.budget_usd)
                if not allowed_budget:
                    error = f"tenant budget exceeded: ${spent:.6f} > ${request.budget_usd:.6f}"
                    await emit(EventType.BUDGET_EXCEEDED, agent=agent.name, task=task.name, error=error)
                    failed = AgentResult(agent.name, "", result.usage, False, error)
                    await emit(EventType.AGENT_FAILED, agent=agent.name, task=task.name, text="", usage=result.usage, error=error)
                    return failed
                await emit(EventType.AGENT_COMPLETED, agent=agent.name, task=task.name, text=result.text, usage=result.usage)
                return result
            except asyncio.TimeoutError:
                last_error = "agent deadline exceeded"
                await self.circuit_breakers.failure(agent.name)
            except Exception as exc:  # isolate one worker from the rest of the DAG
                last_error = str(exc)
                await self.circuit_breakers.failure(agent.name)
                if not self.retry_policy.retryable(exc):
                    break
        failed = AgentResult(agent.name, "", Usage(), False, f"{last_error} after {request.max_retries + 1} attempts")
        await emit(EventType.AGENT_FAILED, agent=agent.name, task=task.name, text="", usage=failed.usage, error=failed.error)
        return failed
