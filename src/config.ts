export interface Config {
  databaseUrl: string;
  apiToken: string;
  host: string;
  port: number;
  ingestionRateLimit: number;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const databaseUrl = env.DATABASE_URL;
  const apiToken = env.API_TOKEN;
  if (!databaseUrl) throw new Error('DATABASE_URL is required');
  if (!apiToken || apiToken.length < 32) throw new Error('API_TOKEN must contain at least 32 characters');
  const port = Number(env.PORT ?? '3000');
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('PORT must be a valid TCP port');
  const ingestionRateLimit = Number(env.INGESTION_RATE_LIMIT ?? '120');
  if (!Number.isInteger(ingestionRateLimit) || ingestionRateLimit < 1) {
    throw new Error('INGESTION_RATE_LIMIT must be a positive integer');
  }
  return { databaseUrl, apiToken, host: env.HOST ?? '0.0.0.0', port, ingestionRateLimit };
}
