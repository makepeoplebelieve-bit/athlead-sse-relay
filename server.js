import express from "express";
import Redis from "ioredis";

// ATHLEAD SSE Relay — Pub/Sub primary, watchdog fallback.
//
// Primary path: computeGroups (triggered by MQTT bridge or watchdog) publishes
// to Redis Pub/Sub → relay fans out to all SSE clients.
// The MQTT bridge is the primary compute trigger (debounced 3s per event).
// The relay watchdog is a fallback: it runs every 3s but skips when Pub/Sub
// is active (bridge working). It keeps running after the last client leaves
// so computation stays continuous without viewers, and auto-stops after 60s
// of no activity.

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
const computeInFlight = new Set(); // eventId → bool (prevent overlap)
const staleCounts = new Map();     // eventId → consecutive zero-active ticks
const STALE_TICK_THRESHOLD = 20;   // 20 × 3s = 60s of no activity → auto-stop
const COMPUTE_INTERVAL_MS = 3000;

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
    // Don't stop the watchdog — it self-skips when Pub/Sub is active (bridge
    // triggering computeGroups). This keeps a steady 3s cadence instead of
    // the 5s restart cycle that happened when we stopped on every Pub/Sub.
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
    clients.delete(eventId);
    // Watchdog keeps running — computation continues without SSE clients,
    // keeping rideridx + GPS trail continuous. The watchdog auto-stops after
    // 60s of no activity and cleans up Pub/Sub + cached state itself.
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

// ── Watchdog compute timer ──────────────────────────────────
// Runs every 3s. Skips when Pub/Sub is active (bridge triggers computeGroups).
// Auto-stops after 60s of no activity (0 active riders) to prevent leaks.
// Keeps running after the last SSE client disconnects so computation
// continues without viewers — rideridx + GPS trail stay continuous.
function ensureWatchdog(eventId) {
  if (computeTimers.has(eventId)) return;

  const trigger = async () => {
    // Skip when Pub/Sub is active — the bridge is triggering computeGroups,
    // so the watchdog is not needed. Keeps a 3s cadence (bridge debounce)
    // instead of the old 5s restart cycle.
    const lastPub = lastPubSubTs.get(eventId) || 0;
    if (lastPub > 0 && Date.now() - lastPub < COMPUTE_INTERVAL_MS) return;
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
      forwardToClients(eventId, msg);

      // Auto-stop after 60s of no activity (no riders, no finished)
      if (data.total_active === 0 && data.total_finished === 0) {
        const count = (staleCounts.get(eventId) || 0) + 1;
        staleCounts.set(eventId, count);
        if (count >= STALE_TICK_THRESHOLD) stopWatchdog(eventId);
      } else {
        staleCounts.delete(eventId);
      }
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
  staleCounts.delete(eventId);
  // Clean up Pub/Sub subscription + cached state when watchdog stops
  if (sub) {
    subscribed.delete(eventId);
    sub.unsubscribe(`live:${eventId}`);
  }
  clients.delete(eventId);
  lastMessage.delete(eventId);
  lastPubSubTs.delete(eventId);
  computeInFlight.delete(eventId);
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
  if (isFirstClient) ensureWatchdog(eventId);

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
