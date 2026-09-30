import { performance } from 'node:perf_hooks';
import type { InputEvent } from './events.js';

export interface BatchMessage { key: string; value: string }
export interface EventBatcherOptions {
  flushIntervalMs: number;
  maxBatchEvents: number;
  maxBatchBytes: number;
  maxBufferedEvents: number;
  maxBufferedBytes: number;
  shutdownTimeoutMs: number;
}

export const defaultBatcherOptions: Readonly<EventBatcherOptions> = {
  flushIntervalMs: 2,
  maxBatchEvents: 100,
  maxBatchBytes: 256 * 1024,
  maxBufferedEvents: 10_000,
  maxBufferedBytes: 16 * 1024 * 1024,
  shutdownTimeoutMs: 10_000,
};

export function loadBatcherOptions(env: NodeJS.ProcessEnv = process.env): EventBatcherOptions {
  const names: Record<keyof EventBatcherOptions, string> = {
    flushIntervalMs: 'INGESTION_BATCH_FLUSH_MS',
    maxBatchEvents: 'INGESTION_BATCH_MAX_EVENTS',
    maxBatchBytes: 'INGESTION_BATCH_MAX_BYTES',
    maxBufferedEvents: 'INGESTION_BUFFER_MAX_EVENTS',
    maxBufferedBytes: 'INGESTION_BUFFER_MAX_BYTES',
    shutdownTimeoutMs: 'INGESTION_SHUTDOWN_TIMEOUT_MS',
  };
  const options = { ...defaultBatcherOptions };
  for (const key of Object.keys(names) as (keyof EventBatcherOptions)[]) {
    const value = Number(env[names[key]] ?? options[key]);
    if (!Number.isSafeInteger(value) || value < 1 || value > 2_147_483_647) {
      throw new Error(`${names[key]} must be a positive integer no larger than 2147483647`);
    }
    options[key] = value;
  }
  validateOptions(options);
  return options;
}

function validateOptions(options: EventBatcherOptions): void {
  for (const [key, value] of Object.entries(options)) {
    if (!Number.isSafeInteger(value) || value < 1 || value > 2_147_483_647) {
      throw new Error(`${key} must be a positive integer no larger than 2147483647`);
    }
  }
  if (options.maxBufferedEvents < options.maxBatchEvents || options.maxBufferedBytes < options.maxBatchBytes) {
    throw new Error('Ingestion buffer limits must be at least as large as batch limits');
  }
}

export class EventBufferFullError extends Error {
  constructor() { super('Event publication buffer capacity exceeded'); }
}

interface PendingRequest {
  messages: BatchMessage[];
  bytes: number;
  enqueuedAt: number;
  settled: boolean;
  resolve: () => void;
  reject: (error: unknown) => void;
}

/** Batches whole HTTP requests; a request succeeds only after its Kafka send succeeds. */
export class EventBatcher {
  private readonly options: EventBatcherOptions;
  private pending: PendingRequest[] = [];
  private inFlight: PendingRequest[] = [];
  private flushTimer: ReturnType<typeof setTimeout> | undefined;
  private shutdownTimer: ReturnType<typeof setTimeout> | undefined;
  private closePromise: Promise<void> | undefined;
  private finishClose: (() => void) | undefined;
  private accepting = true;
  private expired = false;
  private queuedEvents = 0;
  private queuedBytes = 0;
  private bufferedEvents = 0;
  private bufferedBytes = 0;
  private readonly metrics = {
    batchesSucceeded: 0, batchesFailed: 0, eventsPublished: 0, requestsRejected: 0,
    lastBatchEvents: 0, lastBatchBytes: 0, lastQueueWaitMs: 0, lastPublishDurationMs: 0,
  };

  constructor(
    private readonly send: (messages: BatchMessage[]) => Promise<unknown>,
    options: Partial<EventBatcherOptions> = {},
  ) {
    this.options = { ...defaultBatcherOptions, ...options };
    validateOptions(this.options);
  }

  getStats() {
    return {
      ...this.metrics, queuedEvents: this.queuedEvents, queuedBytes: this.queuedBytes,
      bufferedEvents: this.bufferedEvents, bufferedBytes: this.bufferedBytes,
      inFlightEvents: this.bufferedEvents - this.queuedEvents,
    };
  }

  enqueue(events: InputEvent[]): Promise<void> {
    if (!this.accepting) return Promise.reject(new Error('Event publication is closing'));
    if (events.length === 0) return Promise.resolve();
    if (events.length > this.options.maxBatchEvents || this.bufferedEvents + events.length > this.options.maxBufferedEvents) {
      this.metrics.requestsRejected++;
      return Promise.reject(new EventBufferFullError());
    }
    // Snapshot the payload so caller mutations cannot change a queued publication.
    const messages = events.map((event) => ({ key: event.id, value: JSON.stringify(event) }));
    // Conservatively include per-record framing/headers, not only the JSON payload.
    const bytes = messages.reduce((size, message) => size + Buffer.byteLength(message.key) + Buffer.byteLength(message.value) + 128, 0);
    if (bytes > this.options.maxBatchBytes || this.bufferedBytes + bytes > this.options.maxBufferedBytes) {
      this.metrics.requestsRejected++;
      return Promise.reject(new EventBufferFullError());
    }
    const promise = new Promise<void>((resolve, reject) => {
      this.pending.push({ messages, bytes, enqueuedAt: performance.now(), settled: false, resolve, reject });
    });
    this.queuedEvents += messages.length;
    this.queuedBytes += bytes;
    this.bufferedEvents += messages.length;
    this.bufferedBytes += bytes;
    this.schedule();
    return promise;
  }

  close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    this.accepting = false;
    this.closePromise = new Promise<void>((resolve, reject) => {
      this.finishClose = () => { clearTimeout(this.shutdownTimer); resolve(); };
      this.shutdownTimer = setTimeout(() => {
        this.expired = true;
        const error = new Error('Event publication shutdown deadline exceeded; retry with the same event IDs');
        for (const request of [...this.pending, ...this.inFlight]) this.settle(request, error);
        this.pending = [];
        this.queuedEvents = 0;
        this.queuedBytes = 0;
        clearTimeout(this.flushTimer);
        this.finishClose = undefined;
        reject(error);
      }, this.options.shutdownTimeoutMs);
    });
    this.schedule();
    return this.closePromise;
  }

  private schedule(): void {
    if (this.expired || this.inFlight.length > 0) return;
    clearTimeout(this.flushTimer);
    if (this.pending.length === 0) {
      this.finishClose?.();
      return;
    }
    const delay = this.pending[0]!.enqueuedAt + this.options.flushIntervalMs - performance.now();
    if (!this.accepting || this.queuedEvents >= this.options.maxBatchEvents || this.queuedBytes >= this.options.maxBatchBytes || delay <= 0) {
      void this.flush();
    } else {
      this.flushTimer = setTimeout(() => { void this.flush(); }, delay);
    }
  }

  private settle(request: PendingRequest, error?: unknown): void {
    if (request.settled) return;
    request.settled = true;
    this.bufferedEvents -= request.messages.length;
    this.bufferedBytes -= request.bytes;
    if (error !== undefined) request.reject(error);
    else request.resolve();
  }

  private async flush(): Promise<void> {
    if (this.expired || this.inFlight.length > 0 || this.pending.length === 0) return;
    clearTimeout(this.flushTimer);
    let count = 0, events = 0, bytes = 0;
    for (const request of this.pending) {
      if (events + request.messages.length > this.options.maxBatchEvents || bytes + request.bytes > this.options.maxBatchBytes) break;
      count++;
      events += request.messages.length;
      bytes += request.bytes;
    }
    const batch = this.pending.splice(0, count);
    this.inFlight = batch;
    this.queuedEvents -= events;
    this.queuedBytes -= bytes;
    const started = performance.now();
    const queueWaitMs = started - batch[0]!.enqueuedAt;
    try {
      await this.send(batch.flatMap((request) => request.messages));
      if (!this.expired) {
        this.metrics.batchesSucceeded++;
        this.metrics.eventsPublished += events;
        for (const request of batch) this.settle(request);
      }
    } catch (error) {
      if (!this.expired) {
        this.metrics.batchesFailed++;
        // A failed send may have reached Kafka: reject, never silently drop or report acceptance.
        for (const request of batch) this.settle(request, error instanceof Error ? error : new Error(String(error)));
      }
    } finally {
      this.metrics.lastBatchEvents = events;
      this.metrics.lastBatchBytes = bytes;
      this.metrics.lastQueueWaitMs = queueWaitMs;
      this.metrics.lastPublishDurationMs = performance.now() - started;
      this.inFlight = [];
      // Avoid recursive draining if a transport throws synchronously for many batches.
      queueMicrotask(() => this.schedule());
    }
  }
}
