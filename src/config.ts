export interface KafkaConfig {
  kafkaBrokers: string[];
  kafkaTopic: string;
  kafkaProcessedTopic: string;
  kafkaWorkerGroup: string;
  kafkaNotificationGroup: string;
}

export interface Config extends KafkaConfig {
  databaseUrl: string;
  apiToken: string;
  host: string;
  port: number;
  ingestionRateLimit: number;
  dashboardOrigin?: string;
}

export function loadKafkaConfig(env: NodeJS.ProcessEnv = process.env): KafkaConfig {
  const kafkaBrokers = (env.KAFKA_BROKERS ?? 'localhost:9092').split(',').map((broker) => broker.trim()).filter(Boolean);
  if (kafkaBrokers.length === 0) throw new Error('KAFKA_BROKERS must contain at least one broker');
  const kafkaTopic = env.KAFKA_TOPIC?.trim() || 'analytics.events';
  return {
    kafkaBrokers,
    kafkaTopic,
    kafkaProcessedTopic: env.KAFKA_PROCESSED_TOPIC?.trim() || `${kafkaTopic}.processed`,
    kafkaWorkerGroup: env.KAFKA_WORKER_GROUP?.trim() || env.KAFKA_CONSUMER_GROUP?.trim() || 'analytics-event-workers',
    kafkaNotificationGroup: env.KAFKA_NOTIFICATION_GROUP?.trim() || 'analytics-event-notifications',
  };
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const databaseUrl = env.DATABASE_URL;
  const apiToken = env.API_TOKEN;
  if (!databaseUrl) throw new Error('DATABASE_URL is required');
  if (!apiToken || apiToken.length < 32) throw new Error('API_TOKEN must contain at least 32 characters');
  const port = Number(env.PORT ?? '3002');
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('PORT must be a valid TCP port');
  const ingestionRateLimit = Number(env.INGESTION_RATE_LIMIT ?? '120');
  if (!Number.isInteger(ingestionRateLimit) || ingestionRateLimit < 1) {
    throw new Error('INGESTION_RATE_LIMIT must be a positive integer');
  }
  const dashboardOrigin = env.DASHBOARD_ORIGIN?.trim();
  if (dashboardOrigin) {
    let parsedOrigin: URL;
    try {
      parsedOrigin = new URL(dashboardOrigin);
    } catch {
      throw new Error('DASHBOARD_ORIGIN must be a valid origin');
    }
    if (!['http:', 'https:'].includes(parsedOrigin.protocol) || parsedOrigin.origin !== dashboardOrigin) {
      throw new Error('DASHBOARD_ORIGIN must be an HTTP(S) origin without a path');
    }
  }
  return {
    databaseUrl, apiToken, host: env.HOST ?? '0.0.0.0', port, ingestionRateLimit,
    ...loadKafkaConfig(env),
    ...(dashboardOrigin ? { dashboardOrigin } : {}),
  };
}
