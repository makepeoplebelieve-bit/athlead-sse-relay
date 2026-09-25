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
const clients = new Map();
const lastMessage = new Map();
const subscribed = new Set();
const computeTimers = new Map();
const COMPUTE_INTERVAL_MS = 2000;

// ── Redis Pub/Sub subscriber ─────────────────────────────────
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
    if (computeTimers.has(eventId)) {
      clearInterval(computeTimers.get(eventId));
      computeTimers.delete(eventId);
    }
  }
}

// ── Periodic computeGroups trigger ────────────────────────────
function ensureComputeTimer(eventId) {
  if (computeTimers.has(eventId)) return;
  const trigger = async () => {
    try {
      await fetch(`${BASE44_API_URL}/functions/computeGroups`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ event_id: eventId }),
      });
    } catch (_e) {}
  };
  trigger();
  computeTimers.set(eventId, setInterval(trigger, COMPUTE_INTERVAL_MS));
}

// ── SSE endpoint ─────────────────────────────────────────────
app.get("/sse/:eventId", async (req, res) => {
  const { eventId } = req.params;

  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
    "Access-Control-Allow-Origin": "*",
    "X-Accel-Buffering": "no",
  });

  const cached = lastMessage.get(eventId);
  if (cached) {
    res.write(`data: ${cached}\n\n`);
  } else {
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

  if (!clients.has(eventId)) clients.set(eventId, new Set());
  const isFirstClient = clients.get(eventId).size === 0;
  clients.get(eventId).add(res);
  ensureSubscribed(eventId);
  if (isFirstClient) ensureComputeTimer(eventId);

  const hb = setInterval(() => {
    if (!res.writableEnded) {
      try { res.write(`: heartbeat\n\n`); } catch (_e) {}
    }
  }, 15000);

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
    capacity: connCount < 3000,
  });
});

app.listen(PORT, () => {
  console.log(`ATHLEAD SSE relay running on port ${PORT}`);
});
