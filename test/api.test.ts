import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import { after, before, describe, test, type TestContext } from 'node:test';
import type { FastifyInstance } from 'fastify';
import { createPool, type DatabasePool } from '../src/db.js';
import { migrate } from '../src/migrate.js';
import { handleEvent, type InputEvent } from '../src/events.js';
import { adminHeaders, createTestApp, event, TestEventQueue } from './helpers.js';

const databaseUrl = process.env.TEST_DATABASE_URL;
if (databaseUrl && !new URL(databaseUrl).pathname.endsWith('_test')) {
  throw new Error('TEST_DATABASE_URL database name must end in _test');
}

// Serial execution is intentional: these tests truncate the same disposable database.
describe('PostgreSQL integration', {
  skip: databaseUrl ? false : 'requires TEST_DATABASE_URL', concurrency: false,
}, () => {
  let pool: DatabasePool;
  let app: FastifyInstance;

  after(async () => { await pool?.end(); });
  before(async () => {
    pool = createPool(databaseUrl!);
    await migrate(pool);
  });
  async function setup(t: TestContext) {
    await pool.query('TRUNCATE events');
    app = await createTestApp(t, { pool, queue: new TestEventQueue(pool), config: { databaseUrl: databaseUrl! } });
  }

  function post(events: unknown[]) {
    return app.inject({ method: 'POST', url: '/events', payload: { events } });
  }
  function analytics(query: Record<string, string>) {
    return app.inject({ method: 'GET', url: '/analytics', query, headers: adminHeaders });
  }

  test('valid batch is idempotent, including repeated IDs and concurrent retries', async (t) => {
    await setup(t);
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
    assert.deepEqual(new Set(stored.rows.map((row) => row.id)), new Set([first.id, second.id]));
    assert.ok(stored.rows.every((row) => row.received_at instanceof Date));
  });

  test('validation rejects an entire malformed batch without persisting valid entries', async (t) => {
    await setup(t);
    const result = await post([event(), event({ type: 'purchase' })]);
    assert.equal(result.statusCode, 400);
    assert.equal(result.json().error.code, 'VALIDATION_ERROR');
    assert.equal(result.json().error.details[0].path, '/events/1/type');
    const count = await pool.query('SELECT count(*)::int AS count FROM events');
    assert.equal(count.rows[0].count, 0);
  });

  test('database errors roll back event processing and release the connection', async (t) => {
    await setup(t);
    await pool.query("ALTER TABLE events ADD CONSTRAINT test_reject_target CHECK (target_id <> 'reject-me')");
    t.after(() => pool.query('ALTER TABLE events DROP CONSTRAINT test_reject_target').then(() => {}));
    await assert.rejects(handleEvent(pool, event({ target_id: 'reject-me' }) as InputEvent), { code: '23514' });
    const count = await pool.query('SELECT count(*)::int AS count FROM events');
    assert.equal(count.rows[0].count, 0);
    const recovered = await handleEvent(pool, event() as InputEvent);
    assert.equal(recovered.inserted, true);
    assert.equal(pool.idleCount, pool.totalCount);
  });

  test('admin snapshot initializes current UTC-day activity', async (t) => {
    await setup(t);
    // Use the database clock rather than assuming the API and database clocks match.
    const clock = await pool.query<{ occurred_at: string }>('SELECT now()::text AS occurred_at');
    const current = event({ occurred_at: new Date(clock.rows[0]!.occurred_at).toISOString(), type: 'view' });
    const accepted = await post([current]);
    assert.equal(accepted.statusCode, 202);
    const snapshot = await app.inject({ method: 'GET', url: '/events/snapshot', headers: adminHeaders });
    assert.equal(snapshot.statusCode, 200);
    assert.equal(snapshot.headers['content-type'], 'application/json; charset=utf-8');
    assert.deepEqual(snapshot.json().totals, { clicks: 0, views: 1, total: 1 });
    assert.equal(snapshot.json().recent[0].id, current.id);
    assert.match(snapshot.json().from, /T00:00:00\.000Z$/);
  });

  test('authenticated SSE publishes only newly committed events in order', { timeout: 10000 }, async (t) => {
    await pool.query('TRUNCATE events');
    // SSE needs a real socket; injection cannot complete an open-ended response.
    const abort = new AbortController();
    t.after(() => abort.abort());
    const streamingApp = await createTestApp(t, { pool, queue: new TestEventQueue(pool) });
    const streamPost = (submitted: unknown) => streamingApp.inject({ method: 'POST', url: '/events', payload: { events: [submitted] } });
    await streamingApp.listen({ host: '127.0.0.1', port: 0 });
    const address = streamingApp.server.address() as AddressInfo;
    const response = await fetch(`http://127.0.0.1:${address.port}/events/stream`, {
      headers: { ...adminHeaders, accept: 'text/event-stream', origin: 'http://localhost:5173' }, signal: abort.signal,
    });
    assert.equal(response.status, 200);
    assert.equal(response.headers.get('content-type'), 'text/event-stream; charset=utf-8');
    assert.equal(response.headers.get('access-control-allow-origin'), 'http://localhost:5173');
    assert.ok(response.body);
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffered = '';
    async function frame(): Promise<string> {
      while (!buffered.includes('\n\n')) {
        const chunk = await reader.read();
        assert.equal(chunk.done, false, 'stream stays open until test cleanup');
        buffered += decoder.decode(chunk.value, { stream: true });
      }
      const boundary = buffered.indexOf('\n\n');
      const result = buffered.slice(0, boundary);
      buffered = buffered.slice(boundary + 2);
      return result;
    }
    try {
      assert.match(await frame(), /connected/);
      const submitted = event();
      assert.equal((await streamPost(submitted)).statusCode, 202);
      const firstFrame = await frame();
      assert.match(firstFrame, new RegExp(`^id: ${submitted.id}\nevent: event\ndata: `));
      const published = JSON.parse(firstFrame.split('\ndata: ')[1]!);
      assert.equal(published.id, submitted.id);
      assert.equal(published.target_id, submitted.target_id);
      const committed = await pool.query('SELECT id FROM events WHERE id = $1', [published.id]);
      assert.equal(committed.rowCount, 1);

      assert.equal((await streamPost(submitted)).statusCode, 202);
      const next = event({ type: 'view' });
      assert.equal((await streamPost(next)).statusCode, 202);
      // A subsequent event is a barrier: any duplicate would appear before it.
      assert.match(await frame(), new RegExp(`^id: ${next.id}\n`));
    } finally {
      abort.abort();
      await reader.cancel().catch(() => {});
      reader.releaseLock();
    }
  });

  test('analytics applies half-open UTC ranges, filters and zero-filled buckets', async (t) => {
    await setup(t);
    const inserted = await post([
      event({ occurred_at: '2026-09-30T09:59:59.999Z', type: 'click' }),
      event({ occurred_at: '2026-09-30T10:00:00Z', type: 'click' }),
      event({ occurred_at: '2026-09-30T10:30:00+00:00', type: 'view' }),
      event({ occurred_at: '2026-09-30T12:00:00Z', type: 'view' }),
    ]);
    assert.equal(inserted.statusCode, 202);
    const range = { from: '2026-09-30T10:00:00Z', to: '2026-09-30T12:00:00Z', interval: 'hour' };
    const result = await analytics(range);
    assert.equal(result.statusCode, 200);
    assert.deepEqual(result.json().buckets, [
      { start: '2026-09-30T10:00:00.000Z', clicks: 1, views: 1, total: 2 },
      { start: '2026-09-30T11:00:00.000Z', clicks: 0, views: 0, total: 0 },
    ]);
    assert.deepEqual(result.json().totals, { clicks: 1, views: 1, total: 2 });
    const filtered = await analytics({ ...range, type: 'click' });
    assert.equal(filtered.statusCode, 200);
    assert.deepEqual(filtered.json().totals, { clicks: 1, views: 0, total: 1 });
    const partial = await analytics({ ...range, from: '2026-09-30T10:15:00Z', to: '2026-09-30T10:45:00Z' });
    assert.equal(partial.statusCode, 200);
    assert.deepEqual(partial.json().totals, { clicks: 0, views: 1, total: 1 });
  });

  test('minute and day buckets align to UTC when producers use offsets', async (t) => {
    await setup(t);
    assert.equal((await post([event({ occurred_at: '2026-10-01T06:59:30+07:00', type: 'view' })])).statusCode, 202);
    const minute = await analytics({ from: '2026-09-30T23:59:00Z', to: '2026-10-01T00:01:00Z', interval: 'minute' });
    assert.equal(minute.statusCode, 200);
    assert.deepEqual(minute.json().buckets.map((bucket: { start: string; total: number }) => [bucket.start, bucket.total]), [
      ['2026-09-30T23:59:00.000Z', 1], ['2026-10-01T00:00:00.000Z', 0],
    ]);
    const day = await analytics({ from: '2026-09-30T00:00:00Z', to: '2026-10-02T00:00:00Z', interval: 'day' });
    assert.equal(day.statusCode, 200);
    assert.deepEqual(day.json().buckets.map((bucket: { start: string; total: number }) => [bucket.start, bucket.total]), [
      ['2026-09-30T00:00:00.000Z', 1], ['2026-10-01T00:00:00.000Z', 0],
    ]);
  });

  test('readiness reports missing migrations and recovers when they are restored', async (t) => {
    await setup(t);
    const ready = await app.inject('/health/ready');
    assert.equal(ready.statusCode, 200);
    assert.deepEqual(ready.json(), { status: 'ok' });
    await pool.query("UPDATE schema_migrations SET version = 'test-hidden' WHERE version = '001_events.sql'");
    t.after(() => pool.query("UPDATE schema_migrations SET version = '001_events.sql' WHERE version = 'test-hidden'").then(() => {}));
    const unavailable = await app.inject('/health/ready');
    assert.equal(unavailable.statusCode, 503);
    assert.equal(unavailable.json().error.code, 'NOT_READY');
    assert.equal(typeof unavailable.json().request_id, 'string');
    assert.deepEqual((await app.inject('/health/live')).json(), { status: 'ok' });
    await pool.query("UPDATE schema_migrations SET version = '001_events.sql' WHERE version = 'test-hidden'");
    assert.equal((await app.inject('/health/ready')).statusCode, 200);
  });
});
