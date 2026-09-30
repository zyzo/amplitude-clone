# Minimal analytics API

TypeScript/Node.js API for accepting click and view events and querying historical counts from PostgreSQL. The API runs as one process. Event producers choose a UUID v4 when an event occurs and reuse it on retries.

## Start with Docker

Requires Docker Compose. Copy `.env.example` to `.env`, then replace both passwords with long random values. The admin `API_TOKEN` must be at least 32 characters. Start the database, migration job, and API:

```sh
cp .env.example .env
docker compose up --build -d
docker compose ps
```

The API listens on `http://127.0.0.1:3000` by default. `DB_PORT` and `API_PORT` in `.env` change the host ports. PostgreSQL data persists in the Compose `postgres_data` volume. Re-run `docker compose run --rm migrate` after adding migrations. `docker compose down` stops services without removing the data volume; `docker compose down -v` deletes the volume and its events.

For development with the API watcher running inside Docker, use the separate development stack:

```sh
docker compose -f compose.dev.yaml up --build
```

The API is available at `http://127.0.0.1:3001`; source and migration changes are mounted into the container. Development PostgreSQL data persists in `dev_postgres_data`. Stop this stack with `docker compose -f compose.dev.yaml down`.

For local Node.js development, use Node.js 22 or newer and a PostgreSQL 17 database:

```sh
npm ci
export DATABASE_URL='postgres://analytics:password@localhost:54329/analytics'
export API_TOKEN='a-long-random-token-with-at-least-32-characters'
npm run build
npm run migrate
npm start
```

`npm run dev` runs the TypeScript server with file watching. Set `INGESTION_RATE_LIMIT` to change the initial per-IP request limit (default 120 per minute). Do not enable proxy address trust unless the deployment has a controlled reverse proxy. Terminate TLS at that proxy for remote use.

## API

The OpenAPI document is available at `GET /openapi.json`. All errors use `{ "error": { "code", "message", "details"? }, "request_id" }`.

`POST /events` accepts 1–100 events in a JSON `events` array and a body up to 128 KiB. All fields are required, unknown fields are rejected, and `received_at` is assigned by PostgreSQL. IDs must be UUID v4, timestamps must include a timezone, `type` is `click` or `view`, and `target_id` is 1–512 characters. A valid batch is inserted in one transaction. On success, `inserted + duplicates` equals the number submitted. Duplicate IDs keep their original stored content. This public endpoint is rate limited per client IP.

```sh
curl -sS -X POST http://127.0.0.1:3000/events \
  -H 'Content-Type: application/json' \
  -d '{"events":[{"id":"d1fb87a6-47e0-4ef8-aec2-3470188ea784","occurred_at":"2026-09-30T10:00:00Z","type":"click","target_id":"signup-button"}]}'
```

`GET /analytics` requires `Authorization: Bearer <API_TOKEN>`. Supply `from`, `to`, and `interval=minute|hour|day`; `type=click|view` is optional. The occurrence-time range is `[from,to)`. UTC-aligned buckets include empty buckets with zeros. Results are limited to 10,000 buckets and queries have a five-second database timeout. A late event may change a past bucket. Counts are JSON numbers; as with JavaScript numbers, very large counts beyond the safe integer range are unsupported.

```sh
curl -sS 'http://127.0.0.1:3000/analytics?from=2026-09-30T09%3A00%3A00Z&to=2026-09-30T12%3A00%3A00Z&interval=hour' \
  -H "Authorization: Bearer $API_TOKEN"
```

`GET /health/live` checks the process. `GET /health/ready` checks the database and required migration. Both are public.

## Verification

Run the PostgreSQL integration tests in Docker with a dedicated temporary database:

```sh
docker compose -f compose.test.yaml run --build --rm integration-test
```

The test database lives in a temporary filesystem and is separate from the deployment database. It is discarded when its container is removed. The test runner uses the same Docker build stage as the application build, including development dependencies. For local Node.js testing instead, set `TEST_DATABASE_URL` to a disposable PostgreSQL database whose name ends in `_test`; tests truncate its `events` table.
