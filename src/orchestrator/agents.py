from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable

from .models import AgentResult, Usage

TokenSink = Callable[[str], Awaitable[None]]


@dataclass(slots=True)
class AgentContext:
    request_id: str
    attempt: int
    inputs: dict[str, str]


class Agent:
    """Base worker contract. Production adapters can replace ``answer``."""

    name = "agent"
    cost_per_1k_input = 0.001
    cost_per_1k_output = 0.002
    token_delay_s = 0.002

    async def answer(self, query: str, context: AgentContext) -> str:
        return f"{self.name} result for: {query}"

    async def run(self, query: str, context: AgentContext, on_token: TokenSink) -> AgentResult:
        text = await self.answer(query, context)
        words = text.split()
        for index, word in enumerate(words):
            await on_token(word + (" " if index < len(words) - 1 else ""))
            await asyncio.sleep(self.token_delay_s)
        input_tokens = max(1, len(query.split()))
        output_tokens = max(1, len(words))
        usage = Usage(input_tokens, output_tokens,
                      input_tokens / 1000 * self.cost_per_1k_input +
                      output_tokens / 1000 * self.cost_per_1k_output)
        return AgentResult(self.name, text, usage)


class PlannerAgent(Agent):
    name = "planner"

    async def answer(self, query: str, context: AgentContext) -> str:
        return "Plan: gather evidence, calculate implications, then synthesize a recommendation."


class CodeAgent(Agent):
    name = "code"

    async def answer(self, query: str, context: AgentContext) -> str:
        return f"Implementation sketch: isolate the core logic, add tests, and instrument it. ({query})"


class SearchAgent(Agent):
    name = "search"

    async def answer(self, query: str, context: AgentContext) -> str:
        return f"Evidence summary: relevant signals found for '{query}', with sources to validate."


class MathAgent(Agent):
    name = "math"

    async def answer(self, query: str, context: AgentContext) -> str:
        return "Calculation: assumptions are explicit; the derived result is internally consistent."


class VisionAgent(Agent):
    name = "vision"

    async def answer(self, query: str, context: AgentContext) -> str:
        return f"Visual interpretation: detected the primary objects and relationships in '{query}'."

