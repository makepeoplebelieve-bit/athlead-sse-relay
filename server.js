import express from "express";
import Redis from "ioredis";

const app = express();
app.use(express.json({ limit: "5mb" }));

const REDIS_URL = process.env.REDIS_URL;
const BASE44_API_URL = process.env.BASE44_API_URL || "https://athlead-live-track.base44.app";
const PORT = process.env.PORT || 3001;

if (!REDIS_URL) {
  console.error("FATAL: REDIS_URL environment variable is required");
  process.exit(1);
}

// ── In-memory state ──────────────────────────────────────────
// eventId → Set<res> — connected SSE clients per event
const clients = new Map();
// eventId → last published message string (for instant initial state)
const lastMessage = new Map();
// eventId → subscribed to Redis Pub/Sub?
const subscribed = new Set();

// ── Redis Pub/Sub subscriber (1 per relay instance) ─────────
const sub = new Redis(REDIS_URL);
sub.on("error", (e) => console.error("Redis subscriber error:", e.message));

sub.on("message", (channel, msg) => {
  const eventId = channel.replace("live:", "");
  lastMessage.set(eventId, msg);

  const set = clients.get(eventId);
  if (!set || set.size === 0) return;

  const payload = `data: ${msg}\n\n`;
  for (const res of set) {
    if (!res.writableEnded) {
      try { res.write(payload); } catch (_e) {}
    }
  }
});

function ensureSubscribed(eventId) {
  if (subscribed.has(eventId)) return;
  subscribed.add(eventId);
  sub.subscribe(`live:${eventId}`, (err) => {
    if (err) console.error(`Subscribe error for ${eventId}:`, err.message);
  });
}

function maybeUnsubscribe(eventId) {
  const set = clients.get(eventId);
  if (!set || set.size === 0) {
    subscribed.delete(eventId);
    sub.unsubscribe(`live:${eventId}`);
    lastMessage.delete(eventId);
    clients.delete(eventId);
  }
}

// ── SSE endpoint for browsers ────────────────────────────────
app.get("/sse/:eventId", async (req, res) => {
  const { eventId } = req.params;

  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
    "Access-Control-Allow-Origin": "*",
    "X-Accel-Buffering": "no", // disable Nginx/Proxy buffering
  });

  // 1. Send cached message immediately if available (no Base44 fetch needed)
  const cached = lastMessage.get(eventId);
  if (cached) {
    res.write(`data: ${cached}\n\n`);
  } else {
    // First client for this event on this instance — fetch initial state
    try {
      const r = await fetch(`${BASE44_API_URL}/functions/getCachedGroups`, {
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

  // 2. Register client + subscribe to Redis Pub/Sub
  if (!clients.has(eventId)) clients.set(eventId, new Set());
  clients.get(eventId).add(res);
  ensureSubscribed(eventId);

  // 3. Heartbeat every 15s (keeps proxies alive, detects dead connections)
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

// ── Health endpoint (for Fly.io auto-scaler + monitoring) ─────
app.get("/health", (req, res) => {
  const connCount = [...clients.values()].reduce((s, set) => s + set.size, 0);
  res.json({
    status: "ok",
    connections: connCount,
    events: clients.size,
    capacity: connCount < 3000,
  });
});

app.listen(PORT, () => {
  console.log(`ATHLEAD SSE relay running on port ${PORT}`);
});
