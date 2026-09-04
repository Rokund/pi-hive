import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath, URL } from "node:url";

// Dev server proxies to the hive GUI transport (Port 1). The hive serves no
// static assets today, so the GUI is served by Vite and talks to the hive over
// a direct WebSocket at ws://<host>:<guiPort>/ws. The GUI port is configurable
// via hive.config.json `server.guiPort`; override the dev target with
// PI_HIVE_GUI_WS_PORT to match a non-default port (default 3000).
const GUI_WS_PORT = process.env.PI_HIVE_GUI_WS_PORT ?? "3000";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./src", import.meta.url)),
    },
  },
  server: {
    port: 5173,
    proxy: {
      // If the hive later exposes static files, proxy them here.
      "/ws": {
        target: `ws://127.0.0.1:${GUI_WS_PORT}`,
        ws: true,
      },
    },
  },
});
