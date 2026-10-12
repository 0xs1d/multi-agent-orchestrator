
Search squads, people, interview experiences, posts…


← Back to Exposure
Multi-agent Orchestrator
Route queries across 5 specialized agents with streaming, partial-failure tolerance, and a kill-switch
120 minutes
Advanced
46 points total
Start Project (Premium) →
Booking a slot and starting a project is a Premium feature. You can explore the full brief here.
Upgrade to Premium →
The Brief
# Multi-agent Orchestrator

Given a user query, the orchestrator decides: (a) single LLM call, (b) route to one of 5 specialized agents (planner / code / search / math / vision), or (c) chain agents in a DAG.

Constraints: stream tokens to the user even when agents are chained, per-agent timeouts + retries, partial results when one agent fails, cost tracking per request, and a kill-switch if total time > 30s.

What You'll Be Scored On
4 phases. Auto-graded by a deterministic rubric on your design — no live environment. Hints cost points.
Phase 1 — Topology
0 / 13 pts
1.1
Orchestrator + agents topology (13 pts)
Phase 2 — Agent Classes
0 / 13 pts
2.1
Agent class hierarchy (13 pts)
Phase 3 — Execution States
0 / 10 pts
3.1
Single-agent execution state machine (10 pts)
Phase 4 — Streaming
0 / 10 pts
4.1
Streaming under agent chain (10 pts)
Skills You'll Verify
orchestration
streaming
partial-failure
cost-tracking
Prerequisites
✓
Familiarity with multi-agent LLM systems + the planner/worker pattern
✓
Comfort with topology + class + state + sequence diagrams (all four canvas modes used)
✓
Basic understanding of streaming back-pressure and per-call timeouts/kill-switches
Environment
No containers — an in-browser design canvas (topology, class, state-machine & sequence editors)
Topology
Class diagrams
State machines
Sequence diagrams
In-browser design canvas — no containers, no shell
Deterministic rubric auto-grading (no LLM)
Auto-saves your work between sessions
15-min inactivity timeout (no data loss)
Pure design project: you model the orchestrator topology, the agent class hierarchy, the single-agent execution states & the streaming sequence; a deterministic rubric scores them. Nothing to deploy, no shell.
Related Projects

Idempotent Payment Retry Service
Design the service that never charges a customer twice — even when networks fail and retries collide

Multi-dimensional Rate Limiter
Per-tenant, per-route, per-LLM-token-quota rate limiting that survives Redis going down

LLM-powered Support Chatbot SDK
Design the SDK that other engineers will embed to add a chat assistant to their app
Booking a slot is a Premium feature. Upgrade to reserve a slot and start the project.
Upgrade →
Slots are 120 min (your project length), shown in IST for the next 12 days. Each slot fits 5 people. You can hold up to 4 bookings in any 7-day window.
Free
Filling
Full
Your booking
Past
Mon
12 Oct
Tue
13 Oct
Wed
14 Oct
Thu
15 Oct
Fri
16 Oct
Sat
17 Oct
Sun
18 Oct
Mon
19 Oct
Tue
20 Oct
Wed
21 Oct
Thu
22 Oct
Fri
23 Oct
00:00
02:00
04:00
06:00
08:00
10:00
12:00
14:00
16:00
18:00
20:00
22:00
