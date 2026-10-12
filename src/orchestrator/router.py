from __future__ import annotations

from dataclasses import dataclass

from .models import Request, Route


@dataclass(frozen=True, slots=True)
class PlanTask:
    """Validated unit of work in an execution plan."""

    name: str
    agent: str
    depends_on: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    route: Route
    reason: str
    tasks: tuple[PlanTask, ...]

    def validate(self, available_agents: set[str]) -> None:
        names = {task.name for task in self.tasks}
        if len(names) != len(self.tasks):
            raise ValueError("execution plan contains duplicate task names")
        for task in self.tasks:
            if task.agent not in available_agents:
                raise ValueError(f"execution plan references unknown agent: {task.agent}")
            if any(dep not in names for dep in task.depends_on):
                raise ValueError(f"task {task.name} references an unknown dependency")
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(name: str) -> None:
            if name in visiting:
                raise ValueError("execution plan contains a dependency cycle")
            if name in visited:
                return
            visiting.add(name)
            task = next(t for t in self.tasks if t.name == name)
            for dependency in task.depends_on:
                visit(dependency)
            visiting.remove(name)
            visited.add(name)

        for task in self.tasks:
            visit(task.name)


class StructuredClassifier:
    """Offline classifier that returns a typed, validated plan."""

    def classify(self, request: Request, available_agents: set[str]) -> ExecutionPlan:
        route, reason = choose_route(request)
        if route == Route.CHAIN:
            tasks = (
                PlanTask("plan", "planner"),
                PlanTask("evidence", "search", ("plan",)),
                PlanTask("calculation", "math", ("plan",)),
                PlanTask("synthesis", "code", ("evidence", "calculation")),
            )
        else:
            tasks = (PlanTask("request", select_agent(request.query)),)
        plan = ExecutionPlan(route, reason, tasks)
        plan.validate(available_agents)
        return plan


class RuleBasedClassifier(StructuredClassifier):
    """Backward-compatible name for the structured offline classifier."""


def build_plan(request: Request, available_agents: set[str] | None = None) -> ExecutionPlan:
    return StructuredClassifier().classify(request, available_agents or {"planner", "code", "search", "math", "vision"})


def choose_route(request: Request) -> tuple[Route, str]:
    query = request.query.lower()
    if any(word in query for word in ("chain", "compare", "research", "end to end", "plan and")):
        return Route.CHAIN, "query implies dependent specialist work"
    if any(word in query for word in ("image", "diagram", "visual", "screenshot")):
        return Route.AGENT, "vision keywords"
    if any(word in query for word in ("calculate", "equation", "percentage", "math")):
        return Route.AGENT, "math keywords"
    if any(word in query for word in ("code", "bug", "implement", "python", "api")):
        return Route.AGENT, "code keywords"
    if any(word in query for word in ("search", "latest", "find", "source")):
        return Route.AGENT, "search keywords"
    return Route.SINGLE, "simple query"


def select_agent(query: str) -> str:
    query = query.lower()
    if any(w in query for w in ("image", "diagram", "visual", "screenshot")):
        return "vision"
    if any(w in query for w in ("calculate", "equation", "percentage", "math")):
        return "math"
    if any(w in query for w in ("code", "bug", "implement", "python", "api")):
        return "code"
    return "search" if any(w in query for w in ("search", "latest", "find", "source")) else "planner"
