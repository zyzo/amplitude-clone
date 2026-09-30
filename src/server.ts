import { buildApp } from './app.js';
import { loadConfig } from './config.js';
import { createPool } from './db.js';
import { KafkaEventQueue } from './kafka.js';
import { loadBatcherOptions } from './event-batcher.js';

const config = loadConfig();
const batching = loadBatcherOptions();
const pool = createPool(config.databaseUrl);
const eventQueue = new KafkaEventQueue(
  config.kafkaBrokers, config.kafkaTopic, config.kafkaProcessedTopic, config.kafkaNotificationGroup, batching,
);
const app = await buildApp(config, pool, eventQueue);

// Aggregate metrics rather than logging on every event or batch.
let previousBatches = 0;
let previousRejections = 0;
const metricsTimer = setInterval(() => {
  const stats = eventQueue.getIngestionStats();
  const batches = stats.batchesSucceeded + stats.batchesFailed;
  if (batches !== previousBatches || stats.bufferedEvents > 0 || stats.requestsRejected !== previousRejections) {
    app.log.info({ ingestion: stats }, 'Ingestion publication metrics');
  }
  previousBatches = batches;
  previousRejections = stats.requestsRejected;
}, 10_000);
metricsTimer.unref();
app.addHook('preClose', async () => {
  clearInterval(metricsTimer);
  app.log.info({ ingestion: eventQueue.getIngestionStats() }, 'Final ingestion publication metrics');
});

let shutdownPromise: Promise<void> | undefined;
function shutdown(): Promise<void> {
  shutdownPromise ??= (async () => {
    // Kafka disconnect can itself wait for outstanding network requests. Bound the
    // entire process shutdown as well as the batcher's client-facing drain deadline.
    const deadline = setTimeout(() => {
      app.log.error('Shutdown deadline exceeded');
      process.exit(1);
    }, Math.min(2_147_483_647, batching.shutdownTimeoutMs + 3_000));
    deadline.unref();
    try {
      await app.close();
      await pool.end();
    } catch (error) {
      app.log.error({ err: error }, 'Unable to shut down cleanly');
      process.exitCode = 1;
    } finally {
      clearTimeout(deadline);
    }
  })();
  return shutdownPromise;
}

process.once('SIGINT', () => { void shutdown(); });
process.once('SIGTERM', () => { void shutdown(); });

try {
  await app.listen({ host: config.host, port: config.port });
} catch (error) {
  app.log.error({ err: error }, 'Unable to start API');
  await shutdown();
  process.exitCode = 1;
}
