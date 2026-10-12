from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .agents import Agent, AgentContext
from .models import AgentResult, Usage


@dataclass(frozen=True, slots=True)
class ProviderReply:
    text: str
    input_tokens: int
    output_tokens: int


class ModelProvider(Protocol):
    async def complete(self, prompt: str, *, context: AgentContext) -> ProviderReply: ...


class ProviderAgent(Agent):
    """Adapter for a real provider without putting credentials in Request."""

    def __init__(self, name: str, provider: ModelProvider, *, input_price: float = 0.001, output_price: float = 0.002):
        self.name = name
        self.provider = provider
        self.cost_per_1k_input = input_price
        self.cost_per_1k_output = output_price

    async def answer(self, query: str, context: AgentContext) -> str:
        reply = await self.provider.complete(query, context=context)
        return reply.text

    async def run(self, query: str, context: AgentContext, on_token) -> AgentResult:
        reply = await self.provider.complete(query, context=context)
        words = reply.text.split()
        for index, word in enumerate(words):
            await on_token(word + (" " if index < len(words) - 1 else ""))
        usage = Usage(reply.input_tokens, reply.output_tokens,
                      reply.input_tokens / 1000 * self.cost_per_1k_input +
                      reply.output_tokens / 1000 * self.cost_per_1k_output)
        return AgentResult(self.name, reply.text, usage)


class DeterministicProvider:
    """Offline provider substitute used for integration tests and local demos."""

    def __init__(self, response: str = "provider response"):
        self.response = response

    async def complete(self, prompt: str, *, context: AgentContext) -> ProviderReply:
        return ProviderReply(self.response, max(1, len(prompt.split())), max(1, len(self.response.split())))
