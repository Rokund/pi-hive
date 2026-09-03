import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath, URL } from "node:url";

// Dev server proxies to the hive GUI transport (Port 1). The hive serves no
// static assets today, so the GUI is served by Vite and talks to the hive over
// a direct WebSocket at ws://localhost:3000/ws.
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
        target: "ws://127.0.0.1:3000",
        ws: true,
      },
    },
  },
});
