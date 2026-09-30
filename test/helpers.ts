import { randomUUID } from 'node:crypto';
import type { TestContext } from 'node:test';
import type { FastifyInstance } from 'fastify';
import { buildApp } from '../src/app.js';
import type { Config } from '../src/config.js';
import { createPool, type DatabasePool } from '../src/db.js';
import { handleEvent, type EventQueue, type InputEvent, type StreamEvent } from '../src/events.js';

export const token = 'test-admin-token-at-least-thirty-two-chars';
export const adminHeaders = { authorization: `Bearer ${token}` };

export function event(overrides: Record<string, unknown> = {}) {
  return { id: randomUUID(), occurred_at: '2026-09-30T10:00:00Z', type: 'click', target_id: 'signup', ...overrides };
}

// The queue is a test double; persistence tests still use real PostgreSQL.
export class TestEventQueue implements EventQueue {
  private handler?: (event: StreamEvent) => Promise<void>;
  readonly batches: InputEvent[][] = [];
  closed = false;

  constructor(private readonly pool?: DatabasePool) {}

  async start(handler: (event: StreamEvent) => Promise<void>) { this.handler = handler; }
  async enqueue(events: InputEvent[]) {
    if (!this.handler) throw new Error('Queue has not started');
    this.batches.push(events);
    if (this.pool) {
      for (const event of events) {
        const handled = await handleEvent(this.pool, event);
        if (handled.inserted) await this.handler(handled.event);
      }
    }
  }
  async close() { this.closed = true; }
}

export function config(overrides: Partial<Config> = {}): Config {
  return {
    databaseUrl: 'postgres://localhost/analytics_test', apiToken: token, host: '127.0.0.1', port: 0,
    ingestionRateLimit: 10000, kafkaBrokers: ['localhost:9092'], kafkaTopic: 'analytics.events',
    kafkaProcessedTopic: 'analytics.events.processed', kafkaWorkerGroup: 'test-workers',
    kafkaNotificationGroup: 'test-notifications', ...overrides,
  };
}

export async function createTestApp(t: TestContext, options: {
  pool?: DatabasePool;
  queue?: EventQueue;
  config?: Partial<Config>;
} = {}) {
  const appConfig = config(options.config);
  const pool = options.pool ?? createPool(appConfig.databaseUrl);
  const queue = options.queue ?? new TestEventQueue();
  let app: FastifyInstance | undefined;
  t.after(async () => {
    try {
      await app?.close();
    } finally {
      if (!options.pool) await pool.end();
    }
  });
  app = await buildApp(appConfig, pool, queue);
  // inject() boots Fastify's plugins, so HTTP tests don't need ready() or listen().
  return app;
}
