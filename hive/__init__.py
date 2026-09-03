# pi-hive

"""Python orchestrator that manages multiple `pi` subprocess agents via the
`pi --mode rpc` stdin/stdout JSONL protocol.

- `hive.models` — Pydantic data models (AgentNode / AgentResult / AgentProfile / HiveEvent)
- `hive.config` — typed configuration loader (`hive.config.json`)
- `hive.state` — persistence of the agent graph + session-file mapping (`hive/state/hive.state.json`)
- `hive.process_manager` — `PiSubprocess` lifecycle over `asyncio` + RPC JSONL framing
- `hive.agent_graph` — in-memory agent tree
- `hive.server` — FastAPI GUI (Port 1) and internal/API (Port 2) servers
- `hive.main` — entrypoint: wires everything together and runs the servers
"""

__version__ = "0.1.0"
