import express from "express";
import Redis from "ioredis";

// ATHLEAD SSE Relay — Redis Pub/Sub architecture with direct-response fallback.
//
// Primary path: computeGroups publishes to Redis Pub/Sub → relay fans out.
// Fallback path: the relay's compute timer calls computeGroups and forwards
// the HTTP response directly, so data flows even if Pub/Sub is misconfigured
// (e.g. relay Redis ≠ Base44 Redis). Both paths are active simultaneously;
// duplicates are harmless (client overwrites with latest).

const app = express();
app.use(express.json({ limit: "5mb" }));

const REDIS_URL = process.env.REDIS_URL;
const BASE44_API_URL =
  process.env.BASE44_API_URL || "https://athlead-live-track.base44.app";
const PORT = process.env.PORT || 3001;

// ── In-memory state ──────────────────────────────────────────
const clients = new Map();        // eventId → Set<res>
const lastMessage = new Map();     // eventId → last JSON string
const subscribed = new Set();      // eventId → subscribed to Pub/Sub?
const computeTimers = new Map();   // eventId → interval handle
const computeInFlight = new Set(); // eventId → bool (prevent overlap)
const COMPUTE_INTERVAL_MS = 2000;

// ── Redis Pub/Sub subscriber (1 per relay instance) ─────────
const sub = REDIS_URL ? new Redis(REDIS_URL) : null;
if (sub) sub.on("error", (e) => console.error("Redis subscriber error:", e.message));

if (sub) {
  sub.on("message", (channel, msg) => {
    const eventId = channel.replace("live:", "");
    lastMessage.set(eventId, msg);
    forwardToClients(eventId, msg);
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
    if (computeTimers.has(eventId)) {
      clearInterval(computeTimers.get(eventId));
      computeTimers.delete(eventId);
    }
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

// ── Periodic computeGroups trigger ────────────────────────────
// Calls computeGroups every 2s and forwards the response directly
// (reliable fallback). computeGroups also publishes to Pub/Sub for
// cross-instance fan-out.
function ensureComputeTimer(eventId) {
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
      // Direct forward — reliable even if Pub/Sub is broken
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

  // 2. Register client + subscribe to Pub/Sub + start compute timer
  if (!clients.has(eventId)) clients.set(eventId, new Set());
  const isFirstClient = clients.get(eventId).size === 0;
  clients.get(eventId).add(res);
  ensureSubscribed(eventId);
  if (isFirstClient) ensureComputeTimer(eventId);

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
    active_compute_timers: computeTimers.size,
    pubsub_connected: sub ? sub.status === "ready" : false,
    capacity: connCount < 3000,
  });
});

app.listen(PORT, () => {
  console.log(`ATHLEAD SSE relay running on port ${PORT}`);
});
