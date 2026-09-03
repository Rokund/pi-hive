/**
 * Resolve the hive's GUI WebSocket (Port 1) and HTTP API (Port 2) endpoints.
 *
 * The GUI pages are served from Port 1 and both servers bind to 0.0.0.0.
 * Hardcoding `localhost`/`127.0.0.1` only worked when the browser ran on the
 * same machine as the hive; loading the page from another machine would make
 * every fetch/WebSocket reconnect to the REMOTE browser's own loopback and
 * fail. Deriving the host from the address the page was actually loaded from
 * (`window.location.hostname`) makes the same build work for local access AND
 * for network access when the hive is bound to 0.0.0.0.
 *
 * Override hook: set `window.__PI_HIVE_HOST__` (e.g. from a build inline script)
 * before the bundle loads to force a different hive host (host, or `host:port`
 * needs ports 3000/3001 only — host alone is enough unless served elsewhere).
 */

declare global {
  interface Window {
    /** Optional override for the host the hive is reached at. */
    __PI_HIVE_HOST__?: string;
  }
}

function resolveHiveHost(): string {
  if (typeof window !== "undefined" && window.__PI_HIVE_HOST__) {
    return String(window.__PI_HIVE_HOST__);
  }
  if (typeof window !== "undefined" && window.location.hostname) {
    return window.location.hostname;
  }
  return "127.0.0.1";
}

const wsScheme =
  typeof window !== "undefined" && window.location.protocol === "https:"
    ? "wss"
    : "ws";

/** HTTP API base (Port 2), e.g. `http://<host>:3001`. */
export const API_BASE = `http://${resolveHiveHost()}:3001`;

/** GUI WebSocket URL (Port 1), e.g. `ws://<host>:3000/ws`. */
export const GUI_WS_URL = `${wsScheme}://${resolveHiveHost()}:3000/ws`;
