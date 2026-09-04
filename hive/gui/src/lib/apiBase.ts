/**
 * Resolve the hive's GUI WebSocket (Port 1) and HTTP API (Port 2) endpoints
 * WITHOUT hardcoding ports.
 *
 * The GUI pages and the GUI WebSocket are served from the SAME port
 * (`server.guiPort`, endpoint `/ws`), so the WebSocket URL is derived from the
 * origin this page was actually loaded from — no hardcoded port, and it works
 * for local access, network access, and the vite dev proxy alike.
 *
 * The HTTP API lives on a DIFFERENT port (`server.apiPort`). When the hive
 * serves the GUI it injects the real port into the page (see
 * `hive/server.py::create_gui_app`):
 *
 *     <script>window.__PI_HIVE_CONFIG__ = {"apiPort": 3001}</script>
 *
 * A manual override (`window.__PI_HIVE_API_PORT__`) is honored as well. The
 * trailing default matches `hive/config.py::ServerConfig.apiPort` and only
 * applies in the rare case the page is NOT served by the hive (vite dev /
 * static preview without the injected config).
 *
 * Host override: set `window.__PI_HIVE_HOST__` (e.g. from a build inline
 * script) before the bundle loads to force a different hive host when the GUI
 * is served from elsewhere.
 */

declare global {
  interface Window {
    /** Optional override for the host the hive is reached at. */
    __PI_HIVE_HOST__?: string;
    /** Injected by the hive (server.py create_gui_app): real API port. */
    __PI_HIVE_CONFIG__?: { apiPort?: number };
    /** Manual override for the API port (takes precedence over the default). */
    __PI_HIVE_API_PORT__?: number | string;
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

function resolveApiPort(): number {
  if (typeof window !== "undefined") {
    const injected = window.__PI_HIVE_CONFIG__?.apiPort;
    if (injected) return injected;
    const manual = window.__PI_HIVE_API_PORT__;
    if (manual) return Number(manual);
  }
  // Only reached when the page was not served by the hive (dev / static
  // preview). Matches ServerConfig.apiPort so dev and prod behave alike.
  return 3001;
}

const wsScheme =
  typeof window !== "undefined" && window.location.protocol === "https:"
    ? "wss"
    : "ws";

/** HTTP API base (Port 2), e.g. `http://<host>:<apiPort>`. */
export const API_BASE = `http://${resolveHiveHost()}:${resolveApiPort()}`;

/**
 * GUI WebSocket URL. The hive serves this socket on the same origin (same
 * host AND port) that served this page, so derive it from `window.location`
 * instead of hardcoding the port number.
 */
export const GUI_WS_URL =
  typeof window !== "undefined"
    ? `${wsScheme}://${window.location.host}/ws`
    : `${wsScheme}://${resolveHiveHost()}:3000/ws`;
