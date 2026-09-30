import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { test } from 'node:test';
import { setTimeout as sleep } from 'node:timers/promises';
import { Kafka, logLevel } from 'kafkajs';
import { KafkaEventQueue } from '../src/kafka.js';
import type { InputEvent } from '../src/events.js';

const brokers = process.env.TEST_KAFKA_BROKERS?.split(',');

test('real Kafka acknowledges coalesced requests and preserves every keyed event', { skip: !brokers, timeout: 30_000 }, async () => {
  const topic = `analytics.batching-test.${randomUUID()}`;
  const processedTopic = `${topic}.processed`;
  const kafka = new Kafka({ brokers: brokers!, clientId: 'batching-integration-test', logLevel: logLevel.ERROR });
  const admin = kafka.admin();
  const reader = kafka.consumer({ groupId: `${topic}.reader` });
  const queue = new KafkaEventQueue(brokers!, topic, processedTopic, `${topic}.notifications`, { flushIntervalMs: 10 });
  const received = new Map<string, InputEvent>();
  const events: InputEvent[] = Array.from({ length: 50 }, () => ({
    id: randomUUID(), occurred_at: new Date().toISOString(), type: 'click', target_id: 'batching-integration-test',
  }));
  try {
    await admin.connect();
    await queue.start(async () => {});
    await reader.connect();
    await reader.subscribe({ topic, fromBeginning: true });
    await reader.run({ eachMessage: async ({ message }) => {
      assert.ok(message.value);
      const input = JSON.parse(message.value.toString()) as InputEvent;
      assert.equal(message.key?.toString(), input.id);
      received.set(input.id, input);
    } });
    // Mix single-event requests and one ten-event HTTP-style request.
    await Promise.all([
      ...events.slice(0, 40).map((input) => queue.enqueue([input])),
      queue.enqueue(events.slice(40)),
    ]);
    const ends = await admin.fetchTopicOffsets(topic);
    assert.equal(ends.reduce((total, partition) => total + Number(partition.high), 0), 50);
    assert.equal(queue.getIngestionStats().eventsPublished, 50);
    assert.ok(queue.getIngestionStats().batchesSucceeded < 41, 'requests should share Kafka publications');
    for (let attempt = 0; attempt < 200 && received.size < events.length; attempt++) await sleep(20);
    assert.equal(received.size, events.length);
    for (const input of events) assert.deepEqual(received.get(input.id), input);
  } finally {
    await reader.disconnect();
    await queue.close();
    await admin.deleteTopics({ topics: [topic, processedTopic] });
    await admin.disconnect();
  }
});
