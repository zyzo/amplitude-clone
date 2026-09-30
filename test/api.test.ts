import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import type { AddressInfo } from 'node:net';
import { after, before, test } from 'node:test';
import type { FastifyInstance } from 'fastify';
import { buildApp } from '../src/app.js';
import { createPool, type DatabasePool } from '../src/db.js';
import { migrate } from '../src/migrate.js';
import { handleEvent, type EventQueue, type InputEvent, type StreamEvent } from '../src/events.js';

const databaseUrl = process.env.TEST_DATABASE_URL;
const token = 'test-admin-token-at-least-thirty-two-chars';
let pool: DatabasePool;
let app: FastifyInstance;

function event(overrides: Record<string, unknown> = {}) {
  return { id: randomUUID(), occurred_at: '2026-09-30T10:00:00Z', type: 'click', target_id: 'signup', ...overrides };
}

class TestEventQueue implements EventQueue {
  private handler?: (event: StreamEvent) => Promise<void>;
  async start(handler: (event: StreamEvent) => Promise<void>) { this.handler = handler; }
  async enqueue(events: InputEvent[]) {
    if (!this.handler) throw new Error('Queue has not started');
    for (const event of events) {
      const handled = await handleEvent(pool, event);
      if (handled.inserted) await this.handler(handled.event);
    }
  }
  async close() {}
}

function config(ingestionRateLimit = 10000) {
  return {
    databaseUrl: databaseUrl!, apiToken: token, host: '127.0.0.1', port: 0, ingestionRateLimit,
    kafkaBrokers: ['localhost:9092'], kafkaTopic: 'analytics.events',
    kafkaProcessedTopic: 'analytics.events.processed', kafkaWorkerGroup: 'test-workers',
    kafkaNotificationGroup: 'test-notifications',
  };
}

async function post(events: unknown[]) {
  return app.inject({ method: 'POST', url: '/events', payload: { events } });
}

async function analytics(query: string, bearer = token) {
  return app.inject({ method: 'GET', url: `/analytics?${query}`, headers: { authorization: `Bearer ${bearer}` } });
}

if (!databaseUrl) {
  test('PostgreSQL integration tests require TEST_DATABASE_URL', { skip: true }, () => {});
} else {
  if (!new URL(databaseUrl).pathname.endsWith('_test')) {
    throw new Error('TEST_DATABASE_URL database name must end in _test');
  }

  before(async () => {
    pool = createPool(databaseUrl);
    await migrate(pool);
    app = await buildApp(config(), pool, new TestEventQueue());
    await app.ready();
  });
  after(async () => {
    await app?.close();
    await pool?.end();
  });

  test('valid batch is idempotent, including repeated IDs and concurrent retries', async () => {
    await pool.query('TRUNCATE events');
    const first = event();
    const second = event({ type: 'view' });
    const initial = await post([first, second, first]);
    assert.equal(initial.statusCode, 202);
    assert.deepEqual(initial.json(), { accepted: 3 });
    const repeats = await Promise.all([post([first, second]), post([first, second])]);
    for (const repeat of repeats) {
      assert.equal(repeat.statusCode, 202);
      assert.deepEqual(repeat.json(), { accepted: 2 });
    }
    const stored = await pool.query('SELECT id, received_at FROM events');
    assert.equal(stored.rowCount, 2);
    assert.ok(stored.rows.every((row) => row.received_at instanceof Date));
  });

  test('validation rejects an entire malformed batch and unknown fields', async () => {
    await pool.query('TRUNCATE events');
    const valid = event();
    const invalid = event({ type: 'purchase' });
    const result = await post([valid, invalid]);
    assert.equal(result.statusCode, 400);
    assert.equal(result.json().error.code, 'VALIDATION_ERROR');
    assert.equal(result.json().error.details[0].path, '/events/1/type');
    const unknown = await post([event({ received_at: '2026-09-30T10:00:00Z' })]);
    assert.equal(unknown.statusCode, 400);
    const badDate = await post([event({ occurred_at: '2026-02-30T10:00:00Z' })]);
    assert.equal(badDate.statusCode, 400);
    const badId = await post([event({ id: 'd1fb87a6-47e0-1ef8-aec2-3470188ea784' })]);
    assert.equal(badId.statusCode, 400);
    assert.equal(badId.json().error.code, 'VALIDATION_ERROR');
    const count = await pool.query('SELECT count(*)::int AS count FROM events');
    assert.equal(count.rows[0].count, 0);
  });

  test('database errors roll back event processing', async () => {
    await pool.query('TRUNCATE events');
    await pool.query("ALTER TABLE events ADD CONSTRAINT test_reject_target CHECK (target_id <> 'reject-me')");
    try {
      await assert.rejects(handleEvent(pool, event({ target_id: 'reject-me' }) as InputEvent));
      const count = await pool.query('SELECT count(*)::int AS count FROM events');
      assert.equal(count.rows[0].count, 0);
    } finally {
      await pool.query('ALTER TABLE events DROP CONSTRAINT test_reject_target');
    }
  });

  test('admin snapshot is protected and initializes current UTC-day activity', async () => {
    await pool.query('TRUNCATE events');
    const current = event({ occurred_at: new Date(Date.now() - 1_000).toISOString(), type: 'view' });
    assert.deepEqual((await post([current])).json(), { accepted: 1 });
    const unauthorized = await app.inject('/events/snapshot');
    assert.equal(unauthorized.statusCode, 401);
    const wrongToken = await app.inject({ method: 'GET', url: '/events/snapshot', headers: { authorization: 'Bearer wrong-token' } });
    assert.equal(wrongToken.statusCode, 401);
    const snapshot = await app.inject({ method: 'GET', url: '/events/snapshot', headers: { authorization: `Bearer ${token}` } });
    assert.equal(snapshot.statusCode, 200);
    assert.deepEqual(snapshot.json().totals, { clicks: 0, views: 1, total: 1 });
    assert.equal(snapshot.json().recent[0].id, current.id);
  });

  test('authenticated SSE publishes only newly committed events', async () => {
    await pool.query('TRUNCATE events');
    const streamingApp = await buildApp(config(), pool, new TestEventQueue());
    const abort = new AbortController();
    try {
      await streamingApp.listen({ host: '127.0.0.1', port: 0 });
      const address = streamingApp.server.address() as AddressInfo;
      const baseUrl = `http://127.0.0.1:${address.port}`;
      assert.equal((await streamingApp.inject('/events/stream')).statusCode, 401);
      assert.equal((await streamingApp.inject({ method: 'GET', url: '/events/stream', headers: { authorization: 'Bearer wrong-token' } })).statusCode, 401);
      const response = await fetch(`${baseUrl}/events/stream`, {
        headers: { authorization: `Bearer ${token}`, accept: 'text/event-stream', origin: 'http://localhost:5173' }, signal: abort.signal,
      });
      assert.equal(response.status, 200);
      assert.equal(response.headers.get('access-control-allow-origin'), 'http://localhost:5173');
      const reader = response.body!.getReader();
      const initial = await reader.read();
      assert.match(new TextDecoder().decode(initial.value), /connected/);
      const submitted = event({ occurred_at: new Date().toISOString() });
      const first = await streamingApp.inject({ method: 'POST', url: '/events', payload: { events: [submitted] } });
      assert.equal(first.statusCode, 202);
      assert.deepEqual(first.json(), { accepted: 1 });
      const liveFrame = new TextDecoder().decode((await reader.read()).value);
      assert.match(liveFrame, new RegExp(submitted.id));
      assert.match(liveFrame, /signup/);
      const retry = await streamingApp.inject({ method: 'POST', url: '/events', payload: { events: [submitted] } });
      assert.equal(retry.statusCode, 202);
      assert.deepEqual(retry.json(), { accepted: 1 });
      const duplicateFrame = await Promise.race([
        reader.read().then(({ value }) => new TextDecoder().decode(value)),
        new Promise<string>((resolve) => setTimeout(() => resolve('no event'), 40)),
      ]);
      assert.equal(duplicateFrame, 'no event');
      const snapshot = await app.inject({ method: 'GET', url: '/events/snapshot', headers: { authorization: `Bearer ${token}` } });
      assert.deepEqual(snapshot.json().totals, { clicks: 1, views: 0, total: 1 });
    } finally {
      abort.abort();
      await streamingApp.close();
    }
  });

  test('analytics applies half-open UTC ranges, filters and zero-filled buckets', async () => {
    await pool.query('TRUNCATE events');
    const inserted = await post([
      event({ occurred_at: '2026-09-30T09:59:59.999Z', type: 'click' }),
      event({ occurred_at: '2026-09-30T10:00:00Z', type: 'click' }),
      event({ occurred_at: '2026-09-30T10:30:00+00:00', type: 'view' }),
      event({ occurred_at: '2026-09-30T12:00:00Z', type: 'view' }),
    ]);
    assert.equal(inserted.statusCode, 202);
    const result = await analytics('from=2026-09-30T10%3A00%3A00Z&to=2026-09-30T12%3A00%3A00Z&interval=hour');
    assert.equal(result.statusCode, 200);
    assert.deepEqual(result.json().buckets, [
      { start: '2026-09-30T10:00:00.000Z', clicks: 1, views: 1, total: 2 },
      { start: '2026-09-30T11:00:00.000Z', clicks: 0, views: 0, total: 0 },
    ]);
    assert.deepEqual(result.json().totals, { clicks: 1, views: 1, total: 2 });
    const filtered = await analytics('from=2026-09-30T10%3A00%3A00Z&to=2026-09-30T12%3A00%3A00Z&interval=hour&type=click');
    assert.equal(filtered.statusCode, 200);
    assert.deepEqual(filtered.json().totals, { clicks: 1, views: 0, total: 1 });
    const partial = await analytics('from=2026-09-30T10%3A15%3A00Z&to=2026-09-30T10%3A45%3A00Z&interval=hour');
    assert.deepEqual(partial.json().totals, { clicks: 0, views: 1, total: 1 });
  });

  test('analytics rejects missing token, invalid ranges and excess buckets', async () => {
    const query = 'from=2026-09-30T10%3A00%3A00Z&to=2026-09-30T11%3A00%3A00Z&interval=minute';
    const missing = await app.inject({ method: 'GET', url: `/analytics?${query}` });
    assert.equal(missing.statusCode, 401);
    assert.equal((await analytics(query, 'wrong')).statusCode, 401);
    const reversed = await analytics('from=2026-09-30T11%3A00%3A00Z&to=2026-09-30T10%3A00%3A00Z&interval=minute');
    assert.equal(reversed.statusCode, 400);
    const excess = await analytics('from=2026-01-01T00%3A00%3A00Z&to=2026-02-01T00%3A00%3A00Z&interval=minute');
    assert.equal(excess.statusCode, 400);
    assert.equal(excess.json().error.code, 'TOO_MANY_BUCKETS');
  });

  test('minute and day buckets align to UTC when producers use offsets', async () => {
    await pool.query('TRUNCATE events');
    const inserted = await post([event({ occurred_at: '2026-10-01T06:59:30+07:00', type: 'view' })]);
    assert.equal(inserted.statusCode, 202);
    const minute = await analytics('from=2026-09-30T23%3A59%3A00Z&to=2026-10-01T00%3A01%3A00Z&interval=minute');
    assert.equal(minute.statusCode, 200);
    assert.deepEqual(minute.json().buckets.map((bucket: { start: string; total: number }) => [bucket.start, bucket.total]), [
      ['2026-09-30T23:59:00.000Z', 1], ['2026-10-01T00:00:00.000Z', 0],
    ]);
    const day = await analytics('from=2026-09-30T00%3A00%3A00Z&to=2026-10-02T00%3A00%3A00Z&interval=day');
    assert.equal(day.statusCode, 200);
    assert.deepEqual(day.json().buckets.map((bucket: { start: string; total: number }) => [bucket.start, bucket.total]), [
      ['2026-09-30T00:00:00.000Z', 1], ['2026-10-01T00:00:00.000Z', 0],
    ]);
  });

  test('ingestion enforces its per-IP request limit', async () => {
    const limited = await buildApp(config(1), pool, new TestEventQueue());
    try {
      await limited.ready();
      const first = await limited.inject({ method: 'POST', url: '/events', payload: { events: [event()] } });
      assert.equal(first.statusCode, 202);
      const second = await limited.inject({ method: 'POST', url: '/events', payload: { events: [event()] } });
      assert.equal(second.statusCode, 429);
      assert.equal(second.json().error.code, 'RATE_LIMITED');
    } finally {
      await limited.close();
    }
  });

  test('dashboard CORS preflight permits the configured local origin and bearer header', async () => {
    const allowed = await app.inject({
      method: 'OPTIONS', url: '/analytics',
      headers: {
        origin: 'http://localhost:5173',
        'access-control-request-method': 'GET',
        'access-control-request-headers': 'authorization',
      },
    });
    assert.equal(allowed.statusCode, 204);
    assert.equal(allowed.headers['access-control-allow-origin'], 'http://localhost:5173');
    assert.match(String(allowed.headers['access-control-allow-headers']), /authorization/i);

    const rejected = await app.inject({
      method: 'OPTIONS', url: '/analytics',
      headers: { origin: 'https://untrusted.example', 'access-control-request-method': 'GET' },
    });
    assert.equal(rejected.headers['access-control-allow-origin'], undefined);
  });

  test('health checks and OpenAPI document are available', async () => {
    assert.equal((await app.inject('/health/live')).statusCode, 200);
    assert.equal((await app.inject('/health/ready')).statusCode, 200);
    const spec = await app.inject('/openapi.json');
    assert.equal(spec.statusCode, 200);
    assert.ok(spec.json().paths['/events']);
    assert.ok(spec.json().paths['/events/snapshot']);
    assert.ok(spec.json().paths['/events/stream']);
    assert.ok(spec.json().paths['/analytics']);
  });
}
