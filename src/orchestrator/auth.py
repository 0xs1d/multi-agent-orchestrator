from __future__ import annotations

from dataclasses import dataclass

from .security import Principal


class AuthenticationError(PermissionError):
    pass


class AuthorizationError(PermissionError):
    pass


@dataclass(frozen=True, slots=True)
class LocalIdentityProvider:
    """Deterministic identity adapter for tests; replace with OIDC/JWT verification."""

    tokens: dict[str, Principal]

    def authenticate(self, bearer_token: str) -> Principal:
        try:
            return self.tokens[bearer_token]
        except KeyError as exc:
            raise AuthenticationError("invalid bearer token") from exc


class AuthorizationService:
    def require(self, principal: Principal, scope: str) -> None:
        if scope not in principal.scopes:
            raise AuthorizationError(f"missing required scope: {scope}")
