import { loadKafkaConfig } from './config.js';
import { createPool } from './db.js';
import { startKafkaEventWorker } from './kafka.js';

const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) throw new Error('DATABASE_URL is required');
const kafka = loadKafkaConfig();
const pool = createPool(databaseUrl);

let stopWorker: (() => Promise<void>) | undefined;
let shuttingDown = false;

async function shutdown(): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  try {
    await stopWorker?.();
  } finally {
    await pool.end();
  }
}

try {
  stopWorker = await startKafkaEventWorker(
    pool,
    kafka.kafkaBrokers,
    kafka.kafkaTopic,
    kafka.kafkaProcessedTopic,
    kafka.kafkaWorkerGroup,
  );
  console.info(`Kafka event worker consuming ${kafka.kafkaTopic}`);
  process.once('SIGINT', () => { void shutdown(); });
  process.once('SIGTERM', () => { void shutdown(); });
} catch (error) {
  console.error('Unable to start Kafka event worker:', error);
  await pool.end();
  process.exitCode = 1;
}
