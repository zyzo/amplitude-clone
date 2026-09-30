import assert from 'node:assert/strict';
import { test } from 'node:test';
import { setTimeout as sleep } from 'node:timers/promises';
import { EventBatcher, EventBufferFullError, loadBatcherOptions, type BatchMessage } from '../src/event-batcher.js';
import type { InputEvent } from '../src/events.js';

function event(id = 'one', target = 'signup'): InputEvent {
  return { id, occurred_at: '2026-09-30T10:00:00Z', type: 'click', target_id: target };
}
function deferred() {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
async function until(check: () => boolean) {
  for (let i = 0; i < 200; i++) { if (check()) return; await sleep(2); }
  assert.fail('Timed out waiting for test condition');
}

// Approximate record accounting intentionally includes conservative framing overhead.
const messageBytes = (input: InputEvent) => Buffer.byteLength(input.id) + Buffer.byteLength(JSON.stringify(input)) + 128;

test('timer coalesces requests and resolves only after the Kafka acknowledgment', async () => {
  const ack = deferred();
  const sent: BatchMessage[][] = [];
  const batcher = new EventBatcher(async (messages) => { sent.push(messages); await ack.promise; }, { flushIntervalMs: 10 });
  let accepted = 0;
  const requests = [batcher.enqueue([event('one')]), batcher.enqueue([event('two'), event('three')])];
  for (const request of requests) void request.then(() => accepted++);
  await until(() => sent.length === 1);
  assert.deepEqual(sent[0]!.map((message) => message.key), ['one', 'two', 'three']);
  assert.deepEqual(JSON.parse(sent[0]![0]!.value), event('one'));
  assert.equal(accepted, 0);
  assert.equal(batcher.getStats().inFlightEvents, 3);
  ack.resolve();
  await Promise.all(requests);
  assert.equal(accepted, 2);
  assert.equal(batcher.getStats().bufferedEvents, 0);
  await batcher.close();
});

test('event limit flushes immediately without waiting for the interval', async () => {
  const sent: BatchMessage[][] = [];
  const batcher = new EventBatcher(async (messages) => { sent.push(messages); }, { maxBatchEvents: 3, flushIntervalMs: 10_000 });
  const one = batcher.enqueue([event('one')]);
  const two = batcher.enqueue([event('two'), event('three')]);
  assert.equal(sent.length, 1);
  await Promise.all([one, two]);
  await batcher.close();
});

test('whole requests remain together and sends never overlap', async () => {
  const firstAck = deferred();
  const sent: BatchMessage[][] = [];
  const batcher = new EventBatcher(async (messages) => {
    sent.push(messages);
    if (sent.length === 1) await firstAck.promise;
  }, { maxBatchEvents: 3, flushIntervalMs: 10_000 });
  const requests = [batcher.enqueue([event('a'), event('b')]), batcher.enqueue([event('c'), event('d')])];
  assert.deepEqual(sent[0]!.map((message) => message.key), ['a', 'b']);
  const closing = batcher.close();
  assert.equal(sent.length, 1);
  firstAck.resolve();
  await Promise.all([...requests, closing]);
  assert.deepEqual(sent[1]!.map((message) => message.key), ['c', 'd']);
});

test('byte limit includes UTF-8 keys, payload, and overhead and bounds every send', async () => {
  const input = event('a', '日本語');
  const size = messageBytes(input);
  const sent: BatchMessage[][] = [];
  const batcher = new EventBatcher(async (messages) => { sent.push(messages); }, { maxBatchBytes: size, flushIntervalMs: 10_000 });
  const requests = [batcher.enqueue([input]), batcher.enqueue([event('b', '日本語')])];
  await Promise.all([...requests, batcher.close()]);
  assert.deepEqual(sent.map((messages) => messages.length), [1, 1]);
  assert.equal(batcher.getStats().lastBatchBytes, size);
});

test('in-flight events count against capacity and capacity is released after acknowledgment', async () => {
  const ack = deferred();
  const batcher = new EventBatcher(() => ack.promise, { maxBatchEvents: 2, maxBufferedEvents: 3 });
  const first = batcher.enqueue([event('a'), event('b')]);
  const second = batcher.enqueue([event('c')]);
  await assert.rejects(batcher.enqueue([event('d')]), EventBufferFullError);
  assert.equal(batcher.getStats().bufferedEvents, 3);
  assert.equal(batcher.getStats().queuedEvents, 1);
  ack.resolve();
  await Promise.all([first, second]);
  await batcher.enqueue([event('d')]);
  assert.equal(batcher.getStats().requestsRejected, 1);
  await batcher.close();
});

test('in-flight bytes count against capacity', async () => {
  const ack = deferred();
  const size = messageBytes(event('a'));
  const batcher = new EventBatcher(() => ack.promise, { maxBatchBytes: size, maxBufferedBytes: size * 2 });
  const requests = [batcher.enqueue([event('a')]), batcher.enqueue([event('b')])];
  await assert.rejects(batcher.enqueue([event('c')]), EventBufferFullError);
  ack.resolve();
  await Promise.all([...requests, batcher.close()]);
  assert.equal(batcher.getStats().bufferedBytes, 0);
});

test('oversized requests are rejected atomically before any publication', async () => {
  const sent: BatchMessage[][] = [];
  const batcher = new EventBatcher(async (messages) => { sent.push(messages); }, { maxBatchEvents: 1, maxBatchBytes: messageBytes(event()) });
  await assert.rejects(batcher.enqueue([event('a'), event('b')]), EventBufferFullError);
  await assert.rejects(batcher.enqueue([event('a', 'x'.repeat(1000))]), EventBufferFullError);
  assert.equal(sent.length, 0);
  assert.equal(batcher.getStats().bufferedEvents, 0);
  await batcher.close();
});

test('send rejection rejects every request in its batch and later batches still succeed', async () => {
  const ack = deferred();
  let sends = 0;
  const batcher = new EventBatcher(async () => { if (++sends === 1) await ack.promise; }, { maxBatchEvents: 2 });
  const one = batcher.enqueue([event('a')]);
  const two = batcher.enqueue([event('b')]);
  const failures = [assert.rejects(one, /broker unavailable/), assert.rejects(two, /broker unavailable/)];
  const three = batcher.enqueue([event('c')]);
  ack.reject(new Error('broker unavailable'));
  await Promise.all([...failures, three, batcher.close()]);
  assert.equal(sends, 2);
  assert.equal(batcher.getStats().batchesFailed, 1);
  assert.equal(batcher.getStats().eventsPublished, 1);
  assert.equal(batcher.getStats().bufferedBytes, 0);
});

test('synchronous send errors reject requests without leaking capacity', async () => {
  const batcher = new EventBatcher(() => { throw new Error('sync failure'); }, { maxBatchEvents: 1 });
  await assert.rejects(batcher.enqueue([event()]), /sync failure/);
  assert.equal(batcher.getStats().bufferedEvents, 0);
  await batcher.close();
});

test('payloads are snapshotted before delayed publication', async () => {
  const sent: BatchMessage[][] = [];
  const batcher = new EventBatcher(async (messages) => { sent.push(messages); }, { flushIntervalMs: 10_000 });
  const input = event();
  const publication = batcher.enqueue([input]);
  input.target_id = 'mutated';
  await Promise.all([publication, batcher.close()]);
  assert.equal(JSON.parse(sent[0]![0]!.value).target_id, 'signup');
});

test('close flushes a partial batch immediately, waits for acknowledgment, and is idempotent', async () => {
  const ack = deferred();
  let sends = 0;
  const batcher = new EventBatcher(async () => { sends++; await ack.promise; }, { flushIntervalMs: 10_000 });
  const publication = batcher.enqueue([event()]);
  const closing = batcher.close();
  assert.equal(closing, batcher.close());
  assert.equal(sends, 1);
  let closed = false;
  void closing.then(() => { closed = true; });
  await sleep(5);
  assert.equal(closed, false);
  await assert.rejects(batcher.enqueue([event('late')]), /closing/);
  ack.resolve();
  await Promise.all([publication, closing]);
  assert.equal(closed, true);
});

test('shutdown deadline rejects queued and in-flight requests; late acknowledgment cannot accept them', async () => {
  const ack = deferred();
  let sends = 0;
  const batcher = new EventBatcher(async () => { sends++; await ack.promise; }, { maxBatchEvents: 1, shutdownTimeoutMs: 20 });
  const one = batcher.enqueue([event('one')]);
  const two = batcher.enqueue([event('two')]);
  const failures = [assert.rejects(one, /shutdown deadline/), assert.rejects(two, /shutdown deadline/)];
  await assert.rejects(batcher.close(), /shutdown deadline/);
  await Promise.all(failures);
  assert.equal(batcher.getStats().bufferedBytes, 0);
  ack.resolve();
  await sleep(10);
  assert.equal(sends, 1);
  assert.equal(batcher.getStats().eventsPublished, 0);
  assert.equal(batcher.getStats().bufferedEvents, 0);
});

test('empty batches do not publish and idle close completes', async () => {
  let sends = 0;
  const batcher = new EventBatcher(async () => { sends++; });
  await batcher.enqueue([]);
  await batcher.close();
  assert.equal(sends, 0);
  await assert.rejects(batcher.enqueue([]), /closing/);
});

test('configuration has safe defaults and validates numeric values and capacity relationships', () => {
  assert.equal(loadBatcherOptions({}).flushIntervalMs, 2);
  assert.equal(loadBatcherOptions({ INGESTION_BATCH_MAX_EVENTS: '200' }).maxBatchEvents, 200);
  for (const bad of ['0', '-1', '1.5', 'NaN', '', '2147483648']) {
    assert.throws(() => loadBatcherOptions({ INGESTION_BATCH_FLUSH_MS: bad }), /positive integer/);
  }
  assert.throws(() => loadBatcherOptions({ INGESTION_BUFFER_MAX_EVENTS: '1' }), /buffer limits/);
  assert.throws(() => loadBatcherOptions({ INGESTION_BUFFER_MAX_BYTES: '1' }), /buffer limits/);
  assert.throws(() => new EventBatcher(async () => {}, { flushIntervalMs: 0 }), /positive integer/);
});
