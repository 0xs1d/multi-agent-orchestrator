from __future__ import annotations

import re
from dataclasses import dataclass


class ContentPolicyViolation(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    allowed: bool
    reasons: tuple[str, ...] = ()


class ContentPolicy:
    """Offline baseline policy for obvious secrets and unsafe prompt markers."""

    rules = (
        (re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"), "private key"),
        (re.compile(r"\bAKIA[0-9A-Z]{16}\b"), "cloud access key"),
        (re.compile(r"(?i)ignore previous instructions"), "prompt injection marker"),
    )

    def inspect(self, text: str) -> PolicyDecision:
        reasons = tuple(label for pattern, label in self.rules if pattern.search(text))
        return PolicyDecision(not reasons, reasons)

    def enforce(self, text: str) -> None:
        decision = self.inspect(text)
        if not decision.allowed:
            raise ContentPolicyViolation("content policy blocked: " + ", ".join(decision.reasons))
