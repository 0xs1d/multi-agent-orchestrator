import asyncio

from .models import Request
from .orchestrator import Orchestrator


async def demo() -> None:
    request = Request("Research and compare a Python API design, then calculate the tradeoffs")
    async for event in Orchestrator().stream(request):
        if event.type.value == "token":
            print(event.data["text"], end="", flush=True)
        elif event.type.value in {"started", "routed", "partial", "killed", "completed"}:
            print(f"\n[{event.type.value}] {event.data}")
    print()


def main() -> None:
    asyncio.run(demo())


if __name__ == "__main__":
    main()
