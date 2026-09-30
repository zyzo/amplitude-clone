import assert from 'node:assert/strict';
import { test } from 'node:test';
import type { LightMyRequestResponse } from 'fastify';
import { adminHeaders, createTestApp, event, TestEventQueue } from './helpers.js';

const range = { from: '2026-09-30T10:00:00Z', to: '2026-09-30T11:00:00Z', interval: 'minute' };

function assertJson(response: { statusCode: number; headers: Record<string, unknown> }, status: number) {
  assert.equal(response.statusCode, status);
  assert.equal(response.headers['content-type'], 'application/json; charset=utf-8');
}

function assertError(response: LightMyRequestResponse, status: number, code: string) {
  assertJson(response, status);
  const body = response.json();
  assert.equal(body.error.code, code);
  assert.equal(typeof body.error.message, 'string');
  assert.equal(typeof body.request_id, 'string');
  assert.ok(body.request_id.length > 0);
}

// These tests exercise the HTTP contract without connecting to PostgreSQL or Kafka.
test('ingestion accepts and forwards a valid batch without admin authentication', async (t) => {
  const queue = new TestEventQueue();
  const app = await createTestApp(t, { queue });
  const events = [event(), event({ type: 'view' })];
  const response = await app.inject({ method: 'POST', url: '/events', payload: { events } });
  assertJson(response, 202);
  assert.deepEqual(response.json(), { accepted: 2 });
  assert.deepEqual(queue.batches, [events]);
});

const invalidBatches = [
  { name: 'missing events', payload: {}, code: 'VALIDATION_ERROR' },
  { name: 'empty batch', payload: { events: [] }, code: 'VALIDATION_ERROR' },
  { name: 'more than 100 events', payload: { events: Array.from({ length: 101 }, () => event()) }, code: 'VALIDATION_ERROR' },
  { name: 'unknown body fields', payload: { events: [event()], extra: true }, code: 'VALIDATION_ERROR' },
  { name: 'unknown event fields', payload: { events: [event({ received_at: '2026-09-30T10:00:00Z' })] }, code: 'VALIDATION_ERROR' },
  { name: 'invalid event type in a mixed batch', payload: { events: [event(), event({ type: 'purchase' })] }, code: 'VALIDATION_ERROR' },
  { name: 'impossible date', payload: { events: [event({ occurred_at: '2026-02-30T10:00:00Z' })] }, code: 'VALIDATION_ERROR' },
  { name: 'timestamp without timezone', payload: { events: [event({ occurred_at: '2026-09-30T10:00:00' })] }, code: 'VALIDATION_ERROR' },
  { name: 'non-v4 UUID', payload: { events: [event({ id: 'd1fb87a6-47e0-1ef8-aec2-3470188ea784' })] }, code: 'VALIDATION_ERROR' },
  { name: 'empty target', payload: { events: [event({ target_id: '' })] }, code: 'VALIDATION_ERROR' },
  { name: 'blank target', payload: { events: [event({ target_id: '  ' })] }, code: 'INVALID_TARGET' },
  { name: 'overlong target', payload: { events: [event({ target_id: 'x'.repeat(513) })] }, code: 'VALIDATION_ERROR' },
];
for (const { name, payload, code } of invalidBatches) {
  test(`ingestion rejects ${name} before enqueueing`, async (t) => {
    const queue = new TestEventQueue();
    const app = await createTestApp(t, { queue });
    const response = await app.inject({ method: 'POST', url: '/events', payload });
    assertError(response, 400, code);
    assert.deepEqual(queue.batches, []);
    if (name === 'invalid event type in a mixed batch') {
      assert.equal(response.json().error.details[0].path, '/events/1/type');
    }
  });
}

test('ingestion accepts the maximum batch size', async (t) => {
  const queue = new TestEventQueue();
  const app = await createTestApp(t, { queue });
  const events = Array.from({ length: 100 }, () => event());
  const response = await app.inject({ method: 'POST', url: '/events', payload: { events } });
  assertJson(response, 202);
  assert.deepEqual(response.json(), { accepted: 100 });
  assert.deepEqual(queue.batches, [events]);
});

test('ingestion rejects malformed JSON and oversized payloads', async (t) => {
  const queue = new TestEventQueue();
  const app = await createTestApp(t, { queue });
  const headers = { 'content-type': 'application/json' };
  assertError(await app.inject({ method: 'POST', url: '/events', headers, payload: '{' }), 400, 'BAD_REQUEST');
  assertError(await app.inject({ method: 'POST', url: '/events', payload: { events: [event({ target_id: 'x'.repeat(128 * 1024) })] } }), 413, 'PAYLOAD_TOO_LARGE');
  assert.deepEqual(queue.batches, []);
});

test('queue failures return a retryable error without exposing internal details', async (t) => {
  const queue = new TestEventQueue();
  t.mock.method(queue, 'enqueue', async () => { throw new Error('private broker connection details'); });
  const app = await createTestApp(t, { queue });
  const response = await app.inject({ method: 'POST', url: '/events', payload: { events: [event()] } });
  assertError(response, 503, 'EVENT_QUEUE_UNAVAILABLE');
  assert.doesNotMatch(response.body, /private broker/);
});

for (const url of ['/analytics', '/events/snapshot', '/events/stream']) {
  for (const authorization of [undefined, 'Bearer wrong-token', 'Basic invalid', 'Bearer ']) {
    test(`${url} rejects ${authorization === undefined ? 'missing credentials' : JSON.stringify(authorization)}`, async (t) => {
      const app = await createTestApp(t);
      const response = await app.inject({
        method: 'GET', url, ...(url === '/analytics' ? { query: range } : {}),
        headers: authorization === undefined ? {} : { authorization },
      });
      assertError(response, 401, 'UNAUTHORIZED');
    });
  }
}

for (const { name, query, code } of [
  { name: 'missing query fields', query: {}, code: 'VALIDATION_ERROR' },
  { name: 'unknown interval', query: { ...range, interval: 'week' }, code: 'VALIDATION_ERROR' },
  { name: 'unknown type', query: { ...range, type: 'purchase' }, code: 'VALIDATION_ERROR' },
  { name: 'unknown query field', query: { ...range, extra: 'true' }, code: 'VALIDATION_ERROR' },
  { name: 'impossible timestamp', query: { ...range, from: '2026-02-30T10:00:00Z' }, code: 'INVALID_TIMESTAMP' },
  { name: 'reversed range', query: { ...range, from: range.to, to: range.from }, code: 'INVALID_RANGE' },
  { name: 'equal endpoints', query: { ...range, to: range.from }, code: 'INVALID_RANGE' },
  { name: 'excess buckets', query: { ...range, from: '2026-01-01T00:00:00Z', to: '2026-02-01T00:00:00Z' }, code: 'TOO_MANY_BUCKETS' },
]) {
  test(`analytics rejects ${name}`, async (t) => {
    const app = await createTestApp(t);
    assertError(await app.inject({ method: 'GET', url: '/analytics', query, headers: adminHeaders }), 400, code);
  });
}

test('ingestion rate limits are per IP and isolated between app instances', async (t) => {
  const app = await createTestApp(t, { config: { ingestionRateLimit: 1 } });
  const request = { method: 'POST' as const, url: '/events', payload: { events: [event()] }, remoteAddress: '192.0.2.1' };
  assertJson(await app.inject(request), 202);
  const limited = await app.inject(request);
  assertError(limited, 429, 'RATE_LIMITED');
  assert.ok(limited.headers['retry-after']);
  assertJson(await app.inject({ ...request, remoteAddress: '192.0.2.2' }), 202);
  const fresh = await createTestApp(t, { config: { ingestionRateLimit: 1 } });
  assertJson(await fresh.inject(request), 202);
});

test('CORS permits the configured origin and bearer header, but not untrusted origins', async (t) => {
  const origin = 'https://dashboard.example';
  const app = await createTestApp(t, { config: { dashboardOrigin: origin } });
  const headers = { origin, 'access-control-request-method': 'GET', 'access-control-request-headers': 'authorization' };
  const allowed = await app.inject({ method: 'OPTIONS', url: '/analytics', headers });
  assert.equal(allowed.statusCode, 204);
  assert.equal(allowed.headers['access-control-allow-origin'], origin);
  assert.match(String(allowed.headers['access-control-allow-headers']), /authorization/i);
  for (const rejectedOrigin of ['https://untrusted.example', 'http://localhost:5173']) {
    const rejected = await app.inject({ method: 'OPTIONS', url: '/analytics', headers: { ...headers, origin: rejectedOrigin } });
    // A string origin is emitted as a fixed allow-origin value; browsers reject
    // the response when it doesn't match their request origin.
    assert.equal(rejected.headers['access-control-allow-origin'], origin);
    assert.notEqual(rejected.headers['access-control-allow-origin'], rejectedOrigin);
  }
});

test('default CORS allows local dashboards without reflecting untrusted origins', async (t) => {
  const app = await createTestApp(t);
  for (const origin of ['http://localhost:5173', 'http://127.0.0.1:5173', 'https://untrusted.example']) {
    const response = await app.inject({
      method: 'OPTIONS', url: '/analytics',
      headers: { origin, 'access-control-request-method': 'GET', 'access-control-request-headers': 'authorization' },
    });
    assert.equal(response.statusCode, 204);
    assert.equal(response.headers['access-control-allow-origin'], origin === 'https://untrusted.example' ? undefined : origin);
  }
});

test('liveness and OpenAPI are available without external services', async (t) => {
  const app = await createTestApp(t);
  const live = await app.inject('/health/live');
  assertJson(live, 200);
  assert.deepEqual(live.json(), { status: 'ok' });
  const spec = await app.inject('/openapi.json');
  assertJson(spec, 200);
  for (const path of ['/events', '/events/snapshot', '/events/stream', '/analytics']) {
    assert.ok(spec.json().paths[path], `${path} is documented`);
  }
  assert.deepEqual(spec.json().paths['/analytics'].get.security, [{ adminToken: [] }]);
});

test('closing the app closes its event queue', async (t) => {
  const queue = new TestEventQueue();
  const app = await createTestApp(t, { queue });
  await app.inject('/health/live');
  assert.equal(queue.closed, false);
  await app.close();
  assert.equal(queue.closed, true);
});
