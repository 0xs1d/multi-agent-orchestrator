from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from .security import Principal


@dataclass(frozen=True, slots=True)
class Tool:
    name: str
    required_scope: str
    handler: Callable[[dict[str, Any]], Awaitable[Any]]
    side_effecting: bool = False


class ToolRegistry:
    """Explicit tool permission boundary; agents must never call arbitrary functions."""

    def __init__(self, tools: list[Tool] | None = None):
        self._tools = {tool.name: tool for tool in (tools or [])}

    async def call(self, name: str, arguments: dict[str, Any], principal: Principal) -> Any:
        tool = self._tools.get(name)
        if tool is None:
            raise PermissionError(f"tool is not registered: {name}")
        if tool.required_scope not in principal.scopes:
            raise PermissionError(f"principal lacks scope: {tool.required_scope}")
        return await tool.handler(arguments)
