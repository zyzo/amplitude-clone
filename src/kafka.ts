import { randomUUID } from 'node:crypto';
import { Kafka, type Admin, type Consumer, type Producer } from 'kafkajs';
import type { DatabasePool } from './db.js';
import { handleEvent, type EventQueue, type InputEvent, type StreamEvent } from './events.js';

async function ensureTopics(admin: Admin, topics: string[]): Promise<void> {
  await admin.connect();
  try {
    const existing = new Set(await admin.listTopics());
    const missing = topics.filter((topic) => !existing.has(topic));
    if (missing.length > 0) {
      await admin.createTopics({
        waitForLeaders: true,
        topics: missing.map((topic) => ({ topic, numPartitions: 3, replicationFactor: 1 })),
      });
    }
  } finally {
    await admin.disconnect();
  }
}

export class KafkaEventQueue implements EventQueue {
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private readonly notifier: Consumer;
  private running = false;

  constructor(
    brokers: string[],
    private readonly inputTopic: string,
    processedTopic: string,
    notificationGroup: string,
  ) {
    this.kafka = new Kafka({ clientId: 'analytics-api', brokers });
    this.producer = this.kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
    this.notifier = this.kafka.consumer({ groupId: `${notificationGroup}-${randomUUID()}` });
    this.processedTopic = processedTopic;
  }

  private readonly processedTopic: string;

  async start(handler: (event: StreamEvent) => Promise<void>): Promise<void> {
    await ensureTopics(this.kafka.admin(), [this.inputTopic, this.processedTopic]);
    try {
      await this.producer.connect();
      await this.notifier.connect();
      await this.notifier.subscribe({ topic: this.processedTopic, fromBeginning: false });
      this.running = true;
      await this.notifier.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
          if (!message.value) throw new Error('Kafka processed-event message has no value');
          await handler(JSON.parse(message.value.toString()) as StreamEvent);
          await this.notifier.commitOffsets([{
            topic,
            partition,
            offset: (BigInt(message.offset) + 1n).toString(),
          }]);
        },
      });
    } catch (error) {
      await Promise.allSettled([this.notifier.disconnect(), this.producer.disconnect()]);
      this.running = false;
      throw error;
    }
  }

  async enqueue(events: InputEvent[]): Promise<void> {
    await this.producer.send({
      topic: this.inputTopic,
      acks: -1,
      messages: events.map((event) => ({ key: event.id, value: JSON.stringify(event) })),
    });
  }

  async close(): Promise<void> {
    if (this.running) await this.notifier.stop();
    await Promise.all([this.notifier.disconnect(), this.producer.disconnect()]);
    this.running = false;
  }
}

export async function startKafkaEventWorker(
  pool: DatabasePool,
  brokers: string[],
  inputTopic: string,
  processedTopic: string,
  groupId: string,
): Promise<() => Promise<void>> {
  const kafka = new Kafka({ clientId: 'analytics-event-worker', brokers });
  const admin = kafka.admin();
  await ensureTopics(admin, [inputTopic, processedTopic]);
  const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
  const consumer = kafka.consumer({ groupId });

  try {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: inputTopic, fromBeginning: false });
    await consumer.run({
      autoCommit: false,
      partitionsConsumedConcurrently: 3,
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) throw new Error('Kafka event message has no value');
        const event = JSON.parse(message.value.toString()) as InputEvent;
        const handled = await handleEvent(pool, event);
        await producer.send({
          topic: processedTopic,
          acks: -1,
          messages: [{ key: handled.event.id, value: JSON.stringify(handled.event) }],
        });
        await consumer.commitOffsets([{
          topic,
          partition,
          offset: (BigInt(message.offset) + 1n).toString(),
        }]);
      },
    });
  } catch (error) {
    await Promise.allSettled([consumer.disconnect(), producer.disconnect()]);
    throw error;
  }

  return async () => {
    await consumer.stop();
    await Promise.all([consumer.disconnect(), producer.disconnect()]);
  };
}
