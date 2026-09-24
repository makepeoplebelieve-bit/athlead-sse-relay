# ATHLEAD SSE Relay

External Node.js service that fans out live race data from Redis Pub/Sub to
browser SSE connections. Built for 10k+ concurrent viewers.

## Architecture

```
Base44 computeGroups → Redis PUBLISH live:{eventId}
                              ↓
                    SSE Relay (this service)
                              ↓
                    N browser EventSource connections
```

The relay is stateless and dumb — it subscribes to Redis Pub/Sub channels and
fans out messages to connected SSE clients. All compute stays on Base44.

## Deploy on Fly.io

### 1. Install flyctl

```bash
curl -L https://fly.io/install.sh | sh
```

### 2. Login & create app

```bash
flyctl auth login
cd relay
flyctl launch --no-deploy  # creates the app, uses fly.toml
```

### 3. Set environment variables

```bash
# The Redis Cloud URL (same as Base44 REDIS_URL secret)
flyctl secrets set REDIS_URL="rediss://default:YOUR_PASSWORD@YOUR_REGION.redis.cloud.redislabs.com:PORT"

# Base44 API URL (published app)
flyctl secrets set BASE44_API_URL="https://athlead-live-track.base44.app"
```

### 4. Deploy

```bash
flyctl deploy
```

### 5. Verify

```bash
# Health check
curl https://athlead-live-relay.fly.dev/health
# Should return: {"status":"ok","connections":0,"events":0,"capacity":true}

# SSE test (replace with a real event ID)
curl -N https://athlead-live-relay.fly.dev/sse/YOUR_EVENT_ID
```

### 6. Configure in ATHLEAD admin

Go to **Admin → Instellingen → Live data levering**:
- Set mode to **SSE Relay (schaalbaar)**
- Set SSE Relay URL to `https://athlead-live-relay.fly.dev`
- Save

The frontend will now use SSE instead of polling. To switch back, set mode
to **Polling (huidig)** and save — no code changes needed.

## Auto-scaling

The `fly.toml` config:
- **Min 2 machines** (redundancy, always-on)
- **Auto-start** on traffic
- **Soft limit 2500 connections** per machine → Fly scales up
- **Health check** every 10s on `/health`

To scale manually:
```bash
flyctl scale count 4  # 4 machines
```

## Monitoring

```bash
# Live connection count
watch -n5 'curl -s https://athlead-live-relay.fly.dev/health | jq'

# Fly.io dashboard
flyctl dashboard
```

## Local development

```bash
cd relay
npm install
REDIS_URL="rediss://..." BASE44_API_URL="https://athlead-live-track.base44.app" node server.js
```

## How it works

1. Browser opens `EventSource('/sse/:eventId')` on the relay
2. Relay sends cached last message immediately (if available)
3. Otherwise, relay fetches initial state from Base44 `getCachedGroups`
4. Relay subscribes to Redis `live:{eventId}` Pub/Sub channel
5. Base44 `computeGroups` publishes results every 3-5s
6. Relay fans out to all connected clients for that event
7. Heartbeat every 15s keeps connections alive
8. On disconnect, client is removed and subscription cleaned up

## Fallback

If the relay is down, the frontend automatically falls back to polling
`computeGroups` directly (after 5 SSE failures). No manual intervention needed.
