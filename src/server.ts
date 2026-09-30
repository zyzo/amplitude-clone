import { buildApp } from './app.js';
import { loadConfig } from './config.js';
import { createPool } from './db.js';
import { KafkaEventQueue } from './kafka.js';

const config = loadConfig();
const pool = createPool(config.databaseUrl);
const eventQueue = new KafkaEventQueue(
  config.kafkaBrokers, config.kafkaTopic, config.kafkaProcessedTopic, config.kafkaNotificationGroup,
);
const app = await buildApp(config, pool, eventQueue);

async function shutdown(): Promise<void> {
  await app.close();
  await pool.end();
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
