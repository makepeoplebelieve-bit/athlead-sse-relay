import express from "express";
import Redis from "ioredis";

// ATHLEAD SSE Relay — Pub/Sub primary, watchdog fallback.
//
// Primary path: computeGroups (triggered by MQTT bridge or watchdog) publishes
// to Redis Pub/Sub → relay fans out to all SSE clients.
// Watchdog: if no Pub/Sub message is received for > SILENCE_THRESHOLD_MS, the
// relay starts a compute timer as fallback. When Pub/Sub resumes, the watchdog
// stops automatically. This eliminates redundant computeGroups calls when
// Pub/Sub is active (e.g. MQTT bridge triggers computeGroups), while
// guaranteeing data flow when Pub/Sub is silent or misconfigured.

const app = express();
app.use(express.json({ limit: "5mb" }));

const REDIS_URL = process.env.REDIS_URL;
const BASE44_API_URL =
  process.env.BASE44_API_URL || "https://athlead-live-track.base44.app";
const PORT = process.env.PORT || 3001;

// ── In-memory state ──────────────────────────────────────────
const clients = new Map();        // eventId → Set<res>
const lastMessage = new Map();     // eventId → last JSON string
const lastPubSubTs = new Map();    // eventId → last Pub/Sub message timestamp (ms)
const subscribed = new Set();      // eventId → subscribed to Pub/Sub?
const computeTimers = new Map();   // eventId → interval handle (watchdog compute)
const watchdogTimers = new Map();  // eventId → interval handle (silence monitor)
const computeInFlight = new Set(); // eventId → bool (prevent overlap)
const COMPUTE_INTERVAL_MS = 3000;
const SILENCE_THRESHOLD_MS = 5000; // start watchdog after 5s of Pub/Sub silence

// ── Event loop lag monitor ───────────────────────────────────
// Detects when the synchronous fan-out (forwardToClients) blocks too long.
// Logs a warning when lag > 100ms — a sign that chunked writes are needed.
let lastTick = Date.now();
setInterval(() => {
  const lag = Date.now() - lastTick - 100;
  if (lag > 100) {
    const connCount = [...clients.values()].reduce((s, set) => s + set.size, 0);
    console.warn(`[event-loop-lag] ${lag}ms blocked — ${connCount} SSE clients connected`);
  }
  lastTick = Date.now();
}, 100);

// ── Redis Pub/Sub subscriber (1 per relay instance) ─────────
const sub = REDIS_URL ? new Redis(REDIS_URL) : null;
if (sub) sub.on("error", (e) => console.error("Redis subscriber error:", e.message));

if (sub) {
  sub.on("message", (channel, msg) => {
    const eventId = channel.replace("live:", "");
    lastMessage.set(eventId, msg);
    lastPubSubTs.set(eventId, Date.now());
    forwardToClients(eventId, msg);
    // Pub/Sub active — stop watchdog compute timer if running
    stopWatchdog(eventId);
  });
}

function ensureSubscribed(eventId) {
  if (!sub || subscribed.has(eventId)) return;
  subscribed.add(eventId);
  sub.subscribe(`live:${eventId}`, (err) => {
    if (err) console.error(`Subscribe error for ${eventId}:`, err.message);
  });
}

function maybeUnsubscribe(eventId) {
  const set = clients.get(eventId);
  if (!set || set.size === 0) {
    if (sub) {
      subscribed.delete(eventId);
      sub.unsubscribe(`live:${eventId}`);
    }
    clients.delete(eventId);
    lastMessage.delete(eventId);
    lastPubSubTs.delete(eventId);
    stopWatchdogMonitor(eventId);
    computeInFlight.delete(eventId);
  }
}

// ── Forward a message string to all SSE clients for an event ──
function forwardToClients(eventId, msg) {
  const set = clients.get(eventId);
  if (!set || set.size === 0) return;
  const payload = `data: ${msg}\n\n`;
  for (const res of set) {
    if (!res.writableEnded) {
      try { res.write(payload); } catch (_e) {}
    }
  }
}

// ── Watchdog compute timer (fallback when Pub/Sub is silent) ──
function startWatchdog(eventId) {
  if (computeTimers.has(eventId)) return;

  const trigger = async () => {
    if (computeInFlight.has(eventId)) return;
    computeInFlight.add(eventId);
    try {
      const r = await fetch(`${BASE44_API_URL}/functions/computeGroups`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ event_id: eventId }),
      });
      if (!r.ok) return;
      const data = await r.json();
      const msg = JSON.stringify(data);
      lastMessage.set(eventId, msg);
      // Direct forward — reliable even if Pub/Sub is broken.
      // computeGroups also publishes to Pub/Sub; if that path works,
      // the relay will receive it and stop this watchdog.
      forwardToClients(eventId, msg);
    } catch (_e) {
      // silent — next tick will retry
    } finally {
      computeInFlight.delete(eventId);
    }
  };

  trigger();
  computeTimers.set(eventId, setInterval(trigger, COMPUTE_INTERVAL_MS));
}

function stopWatchdog(eventId) {
  if (computeTimers.has(eventId)) {
    clearInterval(computeTimers.get(eventId));
    computeTimers.delete(eventId);
  }
}

// ── Silence monitor — starts watchdog when Pub/Sub goes quiet ──
function ensureWatchdogMonitor(eventId) {
  if (watchdogTimers.has(eventId)) return;

  const check = () => {
    const last = lastPubSubTs.get(eventId) || 0;
    if (last === 0 || Date.now() - last > SILENCE_THRESHOLD_MS) {
      // Pub/Sub silent — start watchdog compute timer
      startWatchdog(eventId);
    }
  };

  // Check immediately and every 2s
  check();
  watchdogTimers.set(eventId, setInterval(check, 2000));
}

function stopWatchdogMonitor(eventId) {
  if (watchdogTimers.has(eventId)) {
    clearInterval(watchdogTimers.get(eventId));
    watchdogTimers.delete(eventId);
  }
  stopWatchdog(eventId);
}

// ── SSE endpoint for browsers ────────────────────────────────
app.get("/sse/:eventId", async (req, res) => {
  const { eventId } = req.params;

  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
    "Access-Control-Allow-Origin": "*",
    "X-Accel-Buffering": "no",
  });

  // 1. Send cached message immediately if available
  const cached = lastMessage.get(eventId);
  if (cached) {
    res.write(`data: ${cached}\n\n`);
  } else {
    // First client for this event — fetch initial state from computeGroups
    try {
      const r = await fetch(`${BASE44_API_URL}/functions/computeGroups`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ event_id: eventId }),
      });
      if (r.ok) {
        const data = await r.json();
        const msg = JSON.stringify(data);
        lastMessage.set(eventId, msg);
        res.write(`data: ${msg}\n\n`);
      }
    } catch (_e) {}
  }

  // 2. Register client + subscribe to Pub/Sub + start watchdog monitor
  if (!clients.has(eventId)) clients.set(eventId, new Set());
  const isFirstClient = clients.get(eventId).size === 0;
  clients.get(eventId).add(res);
  ensureSubscribed(eventId);
  if (isFirstClient) ensureWatchdogMonitor(eventId);

  // 3. Heartbeat every 15s
  const hb = setInterval(() => {
    if (!res.writableEnded) {
      try { res.write(`: heartbeat\n\n`); } catch (_e) {}
    }
  }, 15000);

  // 4. Cleanup on disconnect
  req.on("close", () => {
    clearInterval(hb);
    const set = clients.get(eventId);
    if (set) {
      set.delete(res);
      maybeUnsubscribe(eventId);
    }
  });
});

// ── Health endpoint ──────────────────────────────────────────
app.get("/health", (req, res) => {
  const connCount = [...clients.values()].reduce((s, set) => s + set.size, 0);
  res.json({
    status: "ok",
    connections: connCount,
    events: clients.size,
    active_watchdogs: computeTimers.size,
    pubsub_connected: sub ? sub.status === "ready" : false,
    capacity: connCount < 3000,
  });
});

app.listen(PORT, () => {
  console.log(`ATHLEAD SSE relay running on port ${PORT}`);
});
