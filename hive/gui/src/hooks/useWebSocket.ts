import { useEffect, useRef, useState } from "react";
import { GUI_WS_URL } from "../lib/apiBase";
import type { ConnectionStatus, HivePushMessage, SubscribeMessage } from "../types";

export interface UseWebSocketOptions {
  /** Message handler invoked for every parsed inbound message. */
  onMessage: (msg: HivePushMessage) => void;
  /** Reconnect delay in ms after an unexpected close (default 3000). */
  reconnectDelay?: number;
  /** Max consecutive reconnect attempts (default Infinity). */
  maxReconnects?: number;
}

export interface UseWebSocketResult {
  status: ConnectionStatus;
  /** Last error message, if any. */
  error: string | null;
  /** Manually close the socket. */
  close: () => void;
}

/**
 * Connects to the hive GUI WebSocket (Port 1) and surfaces incoming
 * `hive:*` messages. Sends `{"type":"subscribe"}` immediately on connect so the
 * hive starts pushing the tree and events. Reconnects automatically on an
 * unexpected close.
 */
export function useWebSocket({
  onMessage,
  reconnectDelay = 3000,
  maxReconnects = Infinity,
}: UseWebSocketOptions): UseWebSocketResult {
  const [status, setStatus] = useState<ConnectionStatus>("connecting");
  const [error, setError] = useState<string | null>(null);

  // Keep the latest handler without re-binding on every render.
  const onMessageRef = useRef(onMessage);
  onMessageRef.current = onMessage;

  const closeRef = useRef<() => void>(() => {});

  useEffect(() => {
    let socket: WebSocket | null = null;
    let closedByUs = false;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let attempts = 0;
    let disposed = false;

    const cleanupTimer = () => {
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
    };

    const connect = () => {
      if (disposed) return;
      setStatus("connecting");
      setError(null);

      try {
        socket = new WebSocket(GUI_WS_URL);
      } catch (err) {
        setStatus("error");
        setError(err instanceof Error ? err.message : String(err));
        scheduleReconnect();
        return;
      }

      socket.onopen = () => {
        attempts = 0;
        setStatus("open");
        const subscribe: SubscribeMessage = { type: "subscribe" };
        socket?.send(JSON.stringify(subscribe));
      };

      socket.onmessage = (ev) => {
        try {
          const data = JSON.parse(ev.data as string) as HivePushMessage;
          onMessageRef.current(data);
        } catch {
          // Ignore malformed frames; keep the connection alive.
        }
      };

      socket.onerror = () => {
        setError("WebSocket error");
      };

      socket.onclose = () => {
        if (closedByUs || disposed) {
          setStatus("closed");
          return;
        }
        setStatus("closed");
        scheduleReconnect();
      };
    };

    const scheduleReconnect = () => {
      if (closedByUs || disposed) return;
      attempts += 1;
      if (attempts > maxReconnects) {
        setStatus("error");
        setError(`Reconnect failed after ${maxReconnects} attempts`);
        return;
      }
      reconnectTimer = setTimeout(connect, reconnectDelay);
    };

    closeRef.current = () => {
      closedByUs = true;
      cleanupTimer();
      socket?.close();
    };

    connect();

    return () => {
      disposed = true;
      closedByUs = true;
      cleanupTimer();
      socket?.close();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return {
    status,
    error,
    close: () => closeRef.current(),
  };
}
