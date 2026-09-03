# Pi-Hive

A self-hosted orchestrator that supervises [pi](https://github.com/earendil-works/pi) agent subprocesses as a tree of named agents — spawn, steer, abort, follow up on, and peek at them, over a WebSocket API and a web GUI.

Pi-Hive runs each agent as an isolated pi process, supervises its lifecycle (spawn / result / abort / steer / follow-up / glimpse), tracks its live output and token usage, and exposes two ports:

- **Port 1 (Web GUI)** — a React dashboard over the agent tree (HTTP + WebSocket).
- **Port 2 (API)** — an HTTP + WebSocket API for driving agents programmatically.

## Use case: human-visible LLM calls within an agentic loop

Inside an agentic loop, the usual way to use a model is to call the LLM directly from the loop's code — a black box to everyone else. Pi-Hive is an alternative way to make that LLM call: instead of a direct model call, the loop drives supervised pi agents through the hub's WebSocket API. Because those agents run as visible, supervised processes, a human can:

- watch the working process live (output tail, tool calls, token/usage), and
- intervene mid-turn — `steer` a running agent, abort it, or follow up — rather than fire-and-forget.

When you ask an AI to write an agentic loop, have it reference the bundled **`pi-hive-driver`** skill — that reference is what lets the AI write the loop so its LLM calls go through Pi-Hive's supervised interfaces, instead of directly calling the model.

## Features

- **Primary / subagent tree** — a root ("primary") spawns supervised subagents with a tree hierarchy.
- **Full subagent lifecycle** — spawn, poll results, abort (hard stop by default, see ADR-0001), steer a running agent mid-turn, follow up on a finished session, glimpse the live output tail.
- **Liveness telemetry** — live output tail, moving `liveOutputChars` counter, live token/cost usage, phase labels, and per-agent event heartbeats.
- **Per-agent tool & MCP allowlists** — each named agent profile gates which tools and which MCP servers (from `~/.pi/agent/mcp.json`) are visible; visibility is enforced at the hub, never on the MCP server itself (ADR-0002).
- **Named agent profiles** — reusable, configurable profiles (`tester`, `coder`, `reviewer`, …) with per-agent model, concurrency ceiling, tools, skills and system prompts.
- **LLM capability profiles** — structured metadata (context window, pricing, capabilities incl. vision, speed) injected to the primary so it can distinguish a "slow, working within limits" subagent from a "stuck" one.
- **Durable state** — advertises agent graph metadata and id↔session-file mapping across restarts; full conversation history stays in each pi session's own `.jsonl`.

## Requirements

- Python 3.11+
- Node.js 18+ (only to build the GUI)
- The [pi](https://github.com/earendil-works/pi) CLI available on `PATH` (the hub spawns it to run agents)
- A configured LLM (via pi's provider configuration)

## Setup

```bash
# Python dependencies
pip install -r requirements.txt

# Build the web GUI (produces hive/gui/dist)
cd hive/gui
npm install
npm run build
cd ../..

# Create your private configuration from the example
cp hive/hive.config.json.example hive/hive.config.json
```

Edit `hive/hive.config.json` — set real model ids for the primary and worker agents, and fill in the `llm` capability profiles.

## Quickstart

```bash
python -m hive.main
```

- Open the web GUI at `http://localhost:3000`.
- Drive agents programmatically over the API WebSocket at `ws://localhost:3001/ws` (`prompt`, `steer`, `follow_up`, `abort`, `get_tree`, `subscribe`).

An AI agent can drive Pi-Hive end-to-end using only the WebSocket channel — see the bundled **`pi-hive-driver`** skill (`.agents/skills/pi-hive-driver/`) and its self-contained Python reference client (`scripts/python_client.py`).

## Configuration

`hive/hive.config.json` (private — copy the tracked `hive.config.json.example`):

- `server` — bind address and ports.
- `primary` — the root agent profile.
- `agents` — named subagent profiles.
- `llm` — capability profiles keyed by model id, injected to the primary.

The private `hive/hive.config.json` is git-ignored; only the `.example` is tracked. Env vars: `PI_HIVE_CWD`, `PI_HIVE_API_BASE`, `PI_HIVE_SUBAGENTS`.

## License

[MIT](./LICENSE) © 2026 rokund
