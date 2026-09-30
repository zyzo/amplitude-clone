# Minimal analytics system

## Goal

Receive a stream of `click` and `view` events and provide an admin dashboard with:

- Real-time activity and click/view counters.
- Historical analytics filtered by datetime and event type.
- Counts grouped by minute, hour, or day.

Assume modest traffic, one backend instance, and live updates within a few seconds.

## Architecture

```text
Event producers -- HTTP batches --> Backend API --> PostgreSQL
Admin dashboard -- analytics queries --> Backend API
Admin dashboard <-- live SSE updates -- Backend API
```

Use one backend, one PostgreSQL database, and one dashboard. Query raw events initially. No message broker, cache, or separate analytics service.

## Event schema and unique IDs

| Field | Type | Purpose |
| --- | --- | --- |
| `id` | UUID, primary key | Deduplicate retries |
| `occurred_at` | timestamptz | Producer timestamp |
| `received_at` | timestamptz | Server timestamp, assigned on insertion |
| `type` | constrained text | `click` or `view` |
| `target_id` | text | Page or element identifier |

Generate a random UUID v4 **at the producer when the event occurs**. In browser JavaScript, use `crypto.randomUUID()`. Other producers should use their language's standard UUID v4 generator backed by secure randomness.

Generate once and retain the same ID when retrying. Each separate click or view gets a new ID. Do not derive IDs from timestamps alone. UUID collisions are negligibly likely; PostgreSQL's primary key enforces uniqueness.
∆
Index `occurred_at` and `(type, occurred_at)`. Store timestamps in UTC; use UTC for grouping in the initial dashboard and label it clearly.

## HTTP batching and ingestion

`POST /events` accepts an `events` array, including a single-event array:

```json
{
  "events": [
    {
      "id": "d1fb87a6-47e0-4ef8-aec2-3470188ea784",
      "occurred_at": "2026-09-30T10:00:00Z",
      "type": "click",
      "target_id": "signup-button"
    }
  ]
}
```

- Flush when 20 events accumulate or 1 second passes after the first queued event, whichever comes first.
- Validate the entire batch; reject malformed batches with a clear 4xx response. Limit requests to 100 events and a bounded payload size.
- Insert valid batches in one transaction, ignoring existing IDs with `ON CONFLICT (id) DO NOTHING`.
- Acknowledge only after commit. Return inserted and duplicate counts; duplicates are successful delivery.
- Retry network errors, 429s, and 5xx responses with exponential backoff and jitter, preserving event IDs. Do not blindly retry validation errors.

Batching reduces HTTP and database overhead, at the cost of up to roughly one second of collection delay. An in-memory producer buffer can lose unsent events if the page or process exits; durable producer buffering is outside the initial scope.

## Dashboard modes

### Real-time

`GET /events/stream` provides Server-Sent Events (SSE). Publish only newly inserted events after commit; duplicates must not increase counters.

Show recent activity and click/view counters for a clearly labeled window, such as today in UTC. Initialize from the database. On connection or reconnection, establish the stream and refresh the database snapshot; buffer live updates during refresh and reconcile by event ID to avoid overlap or gaps. Refresh counters from the database periodically.

SSE is a live notification channel, not a durable log. Database refreshes recover from missed notifications. Keep the initial deployment to one backend process so all subscriptions see ingestion activity.

### Analytics

`GET /analytics?from=...&to=...&interval=hour&type=click`

- Require `from` and `to`; use the half-open range `from <= occurred_at < to`.
- Allow `interval=minute|hour|day` and optional `type=click|view`; omit type for both.
- Return chronological bucket timestamps, click/view counts, and totals. Fill empty buckets with zeros.
- Validate the interval against an allowlist and bound the number of returned buckets.
- Group by event occurrence time; late-arriving events can update past buckets.

## Admin access

Use a single server-side environment secret, `API_TOKEN`. No user accounts, roles, or login service for now.

The admin enters the token in the dashboard; keep it in memory and send `Authorization: Bearer <token>` on protected requests. Protect analytics, snapshot/recent-event endpoints, and the SSE stream. Use HTTPS; never embed the token in frontend code, URLs, or logs.

Native browser `EventSource` cannot set the authorization header. Use a fetch-based SSE client that supports headers and reconnection.

`API_TOKEN` is an admin credential. Do not distribute it to public browser event producers. Apply validation and rate limits to public ingestion; confirm whether producers are browsers or trusted servers before choosing any separate ingestion credential.

## Implementation acceptance checks

- Sending a batch twice stores and counts each event once.
- Invalid event types and malformed batches are rejected.
- Datetime boundaries, type filters, grouping, and empty buckets behave correctly.
- Committed events appear live; reconnecting restores database-backed state without double counting.
- Missing or incorrect tokens cannot access any admin data endpoint or stream.

Add precomputed aggregates only when historical queries become slow. Add a durable ingestion queue only when bursts exceed database capacity. Multiple backend processes will require shared live-update distribution.
