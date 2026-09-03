/**
 * Subagent extension for pi-hive.
 *
 * Registers five LLM-callable tools:
 *   - `subagent_spawn`    : ask the hive to start a named-agent subagent process
 *                           and feed it a task prompt (SPEC §5.1).
 *   - `subagent_result`   : poll the hive for the subagent's outcome (done /
 *                           running / aborted / failed), optionally waiting
 *                           `wait_time` ms before reporting "running".
 *   - `subagent_abort`    : abort a still-running subagent (SPEC §5.1 / M2).
 *   - `subagent_followup` : reuse a previously completed subagent with a new
 *                           task, resuming its persisted session (context)
 *                           instead of starting fresh (SPEC §5.1 reuse).
 *   - `subagent_glimpse`  : peek at the tail (last N chars, max 1K) of what a
 *                           subagent is producing RIGHT NOW — including thinking
 *                           and tool-call arguments as they stream in — without
 *                           blocking or waiting for completion.
 *
 * The hive is located through `PI_HIVE_API_BASE` in the process environment
 * (set by the hive's process_manager when it spawns the pi child), falling back
 * to `http://127.0.0.1:3001`. This extension therefore only works inside the
 * pi-hive harness — it is the subagent's bridge back to its parent.
 *
 * The primary agent (and any agent that may spawn further subagents) is
 * launched with `--extension <this file>` and with
 * `subagent_spawn,subagent_result,subagent_abort` merged into its `--tools`
 * allowlist, since pi gates extension-registered tools behind the `--tools`
 * allowlist.
 */

import { Type } from "typebox";
import { Text } from "@earendil-works/pi-tui";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

/** Hive API base URL. Set on the pi subprocess env by hive/process_manager.py. */
const HIVE_API_BASE: string =
	process.env.PI_HIVE_API_BASE ?? "http://127.0.0.1:3001";

/** Named subagents this agent may spawn, injected by the hive as
 *  PI_HIVE_SUBAGENTS (JSON: [{name, description?, max_concurrency?}, ...]).
 *  Used to advertise the valid `name` values (their purposes and per-agent
 *  concurrency ceilings) directly in the tool description, so the LLM never
 *  has to guess what to fill in. */
interface AllowedSubagent {
	name: string;
	description?: string;
	max_concurrency?: number;
}

function allowedSubagents(): AllowedSubagent[] {
	try {
		const raw = process.env.PI_HIVE_SUBAGENTS;
		if (!raw) return [];
		const parsed: unknown = JSON.parse(raw);
		if (!Array.isArray(parsed)) return [];
		return parsed.filter(
			(e): e is AllowedSubagent =>
				!!e && typeof e === "object" && typeof (e as AllowedSubagent).name === "string",
			);
	} catch {
		return [];
	}
}

const ALLOWED = allowedSubagents();

/** Human suffix describing one allowed subagent: purpose + per-agent concurrency cap. */
function describeSubagent(a: AllowedSubagent): string {
	const parts = [`"${a.name}"`];
	if (a.description) parts.push(`— ${a.description}`);
	if (typeof a.max_concurrency === "number") {
		parts.push(`(max ${a.max_concurrency} concurrent)`);
	}
	return parts.join(" ");
}

const SPAWN_NAME_HELP = ALLOWED.length
	? `MUST be one of: ${ALLOWED.map((a) => `"${a.name}"`).join(", ")}.`
	: "Name of the named-agent profile to spawn (see hive.config.json).";

const SPAWN_DESCRIPTION = [
	"Spawn a named subagent through the pi-hive and hand it a task prompt.",
	"The subagent runs as its own isolated pi process managed by the hive.",
	"Returns the hive subagent id; poll progress with subagent_result.",
	"Each subagent type has a max_concurrency ceiling (concurrent instances " +
		"that may run at once, beyond the hive's global cap).",
	"When a type is at its concurrency limit you CANNOT just spawn another; " +
		"a reused subagent (subagent_followup) still counts as a running " +
		"instance while active. To free a slot, wait for one to settle " +
		"(subagent_result) or abort a currently-running one (subagent_abort).",
	"You CAN however reuse a finished subagent's session for context via " +
		"subagent_followup instead of spawning a brand-new one.",
	ALLOWED.length
		? `Available subagents: ${ALLOWED.map(describeSubagent).join("; ")}.`
		: "",
]
	.filter(Boolean)
	.join(" ");

/** The parent agent's hive node id == pi session id (see hive/models.py). */
function parentId(ctx: { sessionManager: { getSessionId(): string } }): string {
	return ctx.sessionManager.getSessionId();
}

async function hivePost(
	path: string,
	body: Record<string, unknown>,
): Promise<{ ok: boolean; data?: Record<string, unknown>; error?: string }> {
	try {
		const res = await fetch(`${HIVE_API_BASE}${path}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(body),
		});
		const parsed = (await res.json().catch(() => ({}))) as Record<
			string,
			unknown
		>;
		if (!res.ok) {
			return {
				ok: false,
				error:
					(typeof parsed.error === "string" && parsed.error) ||
					`hive returned HTTP ${res.status}`,
			};
		}
		return { ok: true, data: parsed };
	} catch (err) {
		return {
			ok: false,
			error: `cannot reach hive at ${HIVE_API_BASE}${path}: ${String(err)}`,
		};
	}
}

export default function (pi: ExtensionAPI) {
	pi.registerTool({
		name: "subagent_spawn",
		label: "Spawn Subagent",
		description: SPAWN_DESCRIPTION,
		parameters: Type.Object({
			name: Type.String({ description: SPAWN_NAME_HELP }),
			prompt: Type.String({ description: "Task prompt for the subagent." }),
			options: Type.Optional(
				Type.Object({
					cwd: Type.Optional(Type.String({ description: "Working directory for the subagent process." })),
				}),
			),
		}),
		async execute(toolCallId, params, _signal, _onUpdate, ctx) {
			const result = await hivePost("/hive/subagent/spawn", {
				name: params.name,
				prompt: params.prompt,
				cwd: params.options?.cwd,
				parentId: parentId(ctx),
			});
			return {
				content: [
					{
						type: "text",
						text: JSON.stringify({
							ok: result.ok,
							id: result.data?.id,
							error: result.error ?? result.data?.error ?? undefined,
						}),
					},
				],
				details: {},
			};
		},
		renderCall(args, theme, _context) {
			return new Text(
				theme.fg("toolTitle", theme.bold("subagent_spawn ")) +
					theme.fg("accent", args.name) +
					theme.fg("dim", ` ${(args.prompt || "").slice(0, 60)}`),
				0,
				0,
			);
		},
		renderResult(result, _options, theme, _context) {
			const text = result.content[0];
			const summary =
				text?.type === "text"
					? text.text
					: JSON.stringify({ ok: false, error: "no payload" });
			return new Text(theme.fg("toolOutput", summary), 0, 0);
		},
	});

	pi.registerTool({
		name: "subagent_result",
		label: "Subagent Result",
		description: [
			"Poll the pi-hive for the outcome of a previously spawned subagent.",
			"Optionally wait wait_time ms before reporting; if still running, returns",
			'status "running" plus a `progress` object. Anti-stall guidance:',
			"- `recentlyActive` (true while events are arriving) means it is NOT stuck",
			"  and you MUST keep waiting — do NOT abort.",
			"- `liveOutputChars` (monotonic) and `usage` (live, from the streaming",
			"  provider feed) grow while the subagent is actually producing — thinking,",
			"  text and tool-call arguments. If these numbers are moving, it is working.",
			"- `phase` (thinking / generating / toolcalling / tool_running / ...) is a",
			"  best-effort label of what it is doing right now.",
			"Only consider aborting when recentlyActive is false across several",
			"consecutive polls (roughly a minute or more of total silence). When done",
			"returns the subagent final text and usage.",
		].join(" "),
		parameters: Type.Object({
			id: Type.String({ description: "Hive subagent id returned by subagent_spawn." }),
			wait_time: Type.Optional(
				Type.Number({ description: "Milliseconds to wait for completion before returning." }),
			),
		}),
		async execute(toolCallId, params, _signal, _onUpdate, ctx) {
			const result = await hivePost("/hive/subagent/result", {
				id: params.id,
				wait_time: params.wait_time ?? 0,
			});
			return {
				content: [
					{
						type: "text",
						text: JSON.stringify(result.ok ? result.data : { ok: false, error: result.error }),
					},
				],
				details: {},
			};
		},
		renderCall(args, theme, _context) {
			return new Text(
				theme.fg("toolTitle", theme.bold("subagent_result ")) +
					theme.fg("accent", args.id) +
					theme.fg("dim", args.wait_time ? ` wait ${args.wait_time}ms` : ""),
				0,
				0,
			);
		},
		renderResult(result, _options, theme, _context) {
			const text = result.content[0];
			const summary =
				text?.type === "text"
					? text.text
					: JSON.stringify({ ok: false, error: "no payload" });
			return new Text(theme.fg("toolOutput", summary), 0, 0);
		},
	});

	pi.registerTool({
		name: "subagent_abort",
		label: "Abort Subagent",
		description: [
			"Abort a still-running subagent you previously spawned with subagent_spawn.",
			"This is a HARD STOP: the hive first sends a cooperative abort, and if the",
			"subagent does not stop within a short grace it KILLS the subagent's pi",
			"process so its concurrency slot frees immediately — a long-running tool",
			"can no longer keep a subagent alive forever.",
			"`aborted` is terminal: subagent_result reports `status: aborted` forever",
			"and it is never rewritten to done. The subagent's session is preserved",
			"on disk, so you can still reuse it later with subagent_followup.",
			"Use when the task became moot or the subagent is clearly going down the",
			"wrong path; nudge it instead with subagent_steer when you just want to",
			"redirect a working subagent.",
		].join(" "),
		parameters: Type.Object({
			id: Type.String({ description: "Hive subagent id returned by subagent_spawn." }),
			reason: Type.Optional(
				Type.String({ description: "Why the subagent is being aborted." }),
			),
		}),
		async execute(toolCallId, params, _signal, _onUpdate, ctx) {
			const result = await hivePost("/hive/subagent/abort", {
				id: params.id,
				reason: params.reason ?? "aborted by parent agent",
				by: parentId(ctx),
			});
			return {
				content: [
					{
						type: "text",
						text: JSON.stringify(
							result.ok
								? { ok: true, id: params.id, status: "aborted" }
								: { ok: false, error: result.error },
						),
					},
				],
				details: {},
			};
		},
		renderCall(args, theme, _context) {
			return new Text(
				theme.fg("toolTitle", theme.bold("subagent_abort ")) +
					theme.fg("accent", args.id) +
					theme.fg("dim", args.reason ? ` ${(args.reason || "").slice(0, 60)}` : ""),
				0,
				0,
			);
		},
		renderResult(result, _options, theme, _context) {
			const text = result.content[0];
			const summary =
				text?.type === "text"
					? text.text
					: JSON.stringify({ ok: false, error: "no payload" });
			return new Text(theme.fg("toolOutput", summary), 0, 0);
		},
	});

	pi.registerTool({
		name: "subagent_steer",
		label: "Steer Subagent",
		description: [
			"Send a steering message to a still-RUNNING subagent you spawned with",
			"subagent_spawn. It is delivered mid-execution, after the subagent's",
			"current tool calls finish and before its next model call — use it to",
			"redirect a subagent that is going down the wrong path, or to nudge it",
			"to use one of its own tools, WITHOUT aborting it (that is subagent_abort).",
			"Steering a subagent that is already done/idle does nothing (it has no",
			"live turn to steer); to continue a finished subagent use subagent_followup.",
		].join(" "),
		parameters: Type.Object({
			id: Type.String({ description: "Hive subagent id returned by subagent_spawn." }),
			message: Type.String({ description: "Steering instruction for the subagent." }),
		}),
		async execute(toolCallId, params, _signal, _onUpdate, ctx) {
			const result = await hivePost("/hive/subagent/steer", {
				id: params.id,
				message: params.message,
				by: parentId(ctx),
			});
			return {
				content: [
					{
						type: "text",
						text: JSON.stringify(
							result.ok
								? { ok: true, id: params.id, status: "steered" }
								: { ok: false, error: result.error },
						),
					},
				],
				details: {},
			};
		},
		renderCall(args, theme, _context) {
			return new Text(
				theme.fg("toolTitle", theme.bold("subagent_steer ")) +
					theme.fg("accent", args.id) +
					theme.fg("dim", ` ${(args.message || "").slice(0, 60)}`),
				0,
				0,
			);
		},
		renderResult(result, _options, theme, _context) {
			const text = result.content[0];
			const summary =
				text?.type === "text"
					? text.text
					: JSON.stringify({ ok: false, error: "no payload" });
			return new Text(theme.fg("toolOutput", summary), 0, 0);
		},
	});

	pi.registerTool({
		name: "subagent_followup",
		label: "Subagent Follow-up",
		description: [
			"Send a follow-up task to a previously spawned subagent, reusing its",
			"persisted session so it continues with full context instead of",
			"starting fresh. Use this to reuse a finished subagent (spawn once,",
			"follow up many times) rather than spawning a new one each time.",
			"The hive lazily restarts the subagent from its session if its process",
			"was closed/reaped (idle cleanup), so the conversation continues.",
			"The reuse counts as a new run, so it is still subject to the subagent",
			"result/abort lifecycle; poll subagent_result for the new outcome, and",
			"subagent_abort if the follow-up must be cancelled.",
		].join(" "),
		parameters: Type.Object({
			id: Type.String({
				description: "Hive subagent id returned by subagent_spawn.",
			}),
			prompt: Type.String({
				description: "Follow-up task prompt for the same subagent.",
			}),
		}),
		async execute(toolCallId, params, _signal, _onUpdate, ctx) {
			const result = await hivePost("/hive/subagent/followup", {
				id: params.id,
				prompt: params.prompt,
			});
			return {
				content: [
					{
						type: "text",
						text: JSON.stringify(
							result.ok ? result.data : { ok: false, error: result.error },
						),
					},
				],
				details: {},
			};
		},
		renderCall(args, theme, _context) {
			return new Text(
				theme.fg("toolTitle", theme.bold("subagent_followup ")) +
					theme.fg("accent", args.id) +
					theme.fg("dim", ` ${(args.prompt || "").slice(0, 60)}`),
				0,
				0,
			);
		},
		renderResult(result, _options, theme, _context) {
			const text = result.content[0];
			const summary =
				text?.type === "text"
					? text.text
					: JSON.stringify({ ok: false, error: "no payload" });
			return new Text(theme.fg("toolOutput", summary), 0, 0);
		},
	});

	pi.registerTool({
		name: "subagent_glimpse",
		label: "Subagent Glimpse",
		description: [
			"Peek at the tail (last N characters, max 1024) of what a subagent",
			"is producing RIGHT NOW, WITHOUT blocking or waiting for completion.",
			"It returns the live streamed output tail — final text, thinking and",
			"tool-call arguments as they are being generated — so you can see",
			"whether a subagent that has been quiet is actually thinking /",
			"working instead of stuck. This is the cheap non-blocking peek;",
			"use subagent_result (with wait_time) for the full outcome when done.",
			"The payload includes `phase` (thinking/generating/toolcalling/",
			"tool_running/done/...) and `complete` (false while the text is a",
			"live fragment, true once it is the authoritative final output), so",
			"never mistake a live fragment for a final answer.",
		].join(" "),
		parameters: Type.Object({
			id: Type.String({ description: "Hive subagent id returned by subagent_spawn." }),
			n: Type.Optional(
				Type.Number({
					description: "Max characters of the tail to return. Clamped to [1, 1024] server-side. Default 1024.",
				}),
			),
		}),
		async execute(_toolCallId, params, _signal, _onUpdate, ctx) {
			const result = await hivePost("/hive/subagent/glimpse", {
				id: params.id,
				n: params.n ?? 1024,
			});
			return {
				content: [
					{
						type: "text",
						text: JSON.stringify(result.ok ? result.data : { ok: false, error: result.error }),
					},
				],
				details: {},
			};
		},
		renderCall(args, theme, _context) {
			return new Text(
				theme.fg("toolTitle", theme.bold("subagent_glimpse ")) +
					theme.fg("accent", args.id) +
					theme.fg("dim", args.n ? ` n=${args.n}` : ""),
				0,
				0,
			);
		},
		renderResult(result, _options, theme, _context) {
			const text = result.content[0];
			const summary =
				text?.type === "text"
					? text.text
					: JSON.stringify({ ok: false, error: "no payload" });
			return new Text(theme.fg("toolOutput", summary), 0, 0);
		},
	});
}
