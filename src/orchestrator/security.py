from __future__ import annotations

from dataclasses import dataclass, field

from .models import Request


class RequestValidationError(ValueError):
    """Raised when a request violates the service boundary contract."""


class IdempotencyConflict(ValueError):
    """Raised when a key is reused with a different request payload."""


class IdempotencyInProgress(RuntimeError):
    """Raised when another replica owns the same idempotency key."""


@dataclass(frozen=True, slots=True)
class Principal:
    """Trusted identity injected by an API gateway, never parsed from user input."""

    tenant_id: str
    subject: str
    scopes: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True, slots=True)
class RequestLimits:
    max_query_chars: int = 20_000
    max_metadata_items: int = 32
    max_timeout_s: float = 30.0
    max_retries: int = 5
    max_idempotency_key_chars: int = 256
    max_context_chars: int = 40_000


def validate_request(request: Request, limits: RequestLimits, principal: Principal | None = None) -> None:
    if not request.query.strip():
        raise RequestValidationError("query must not be empty")
    if len(request.query) > limits.max_query_chars:
        raise RequestValidationError("query exceeds maximum length")
    if request.timeout_s <= 0 or request.timeout_s > limits.max_timeout_s:
        raise RequestValidationError(f"timeout_s must be between 0 and {limits.max_timeout_s}")
    if request.max_retries < 0 or request.max_retries > limits.max_retries:
        raise RequestValidationError("max_retries exceeds configured limit")
    if request.budget_usd is not None and request.budget_usd < 0:
        raise RequestValidationError("budget_usd must not be negative")
    if len(request.metadata) > limits.max_metadata_items:
        raise RequestValidationError("metadata contains too many items")
    if request.idempotency_key and len(request.idempotency_key) > limits.max_idempotency_key_chars:
        raise RequestValidationError("idempotency key exceeds maximum length")
    if principal is not None and principal.tenant_id != request.tenant_id:
        raise RequestValidationError("request tenant does not match authenticated principal")
