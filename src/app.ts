import { createHash, timingSafeEqual } from 'node:crypto';
import Fastify, { type FastifyError, type FastifyInstance, type FastifyRequest } from 'fastify';
import cors from '@fastify/cors';
import rateLimit from '@fastify/rate-limit';
import swagger from '@fastify/swagger';
import { z } from 'zod';
import type { DatabasePool } from './db.js';
import type { Config } from './config.js';

type EventType = 'click' | 'view';
interface InputEvent {
  id: string;
  occurred_at: string;
  type: EventType;
  target_id: string;
}
interface EventBody { events: InputEvent[] }
interface AnalyticsQuery { from: string; to: string; interval: 'minute' | 'hour' | 'day'; type?: EventType }

const timestampPattern = '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,3})?(?:Z|[+-]\\d{2}:\\d{2})$';
const timestampValue = z.iso.datetime({ offset: true }).refine((value) => {
  const match = /^(\d{4})-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.(\d+))?(?:Z|([+-])(\d{2}):(\d{2}))$/.exec(value);
  if (!match || Number(match[1]) < 1 || (match[2]?.length ?? 0) > 3) return false;
  const offsetHour = Number(match[4] ?? 0);
  const offsetMinute = Number(match[5] ?? 0);
  if (offsetHour > 14 || offsetMinute > 59 || (offsetHour === 14 && offsetMinute !== 0)) return false;
  const ms = Date.parse(value);
  if (!Number.isFinite(ms)) return false;
  const year = new Date(ms).getUTCFullYear();
  return year >= 1 && year <= 9999;
}, 'Timestamp must be a valid RFC 3339 date and time with a timezone');
const inputEventSchema = z.object({
  id: z.uuidv4(), occurred_at: timestampValue, type: z.enum(['click', 'view']),
  target_id: z.string().min(1).max(512),
}).strict();
const eventBodySchema = z.object({ events: z.array(inputEventSchema).min(1).max(100) }).strict();
const timestampSchema = { type: 'string', pattern: timestampPattern } as const;
const errorSchema = {
  type: 'object', required: ['error', 'request_id'], additionalProperties: false,
  properties: {
    error: { type: 'object', required: ['code', 'message'], additionalProperties: false, properties: {
      code: { type: 'string' }, message: { type: 'string' },
      details: { type: 'array', items: { type: 'object', required: ['path', 'message'], additionalProperties: false,
        properties: { path: { type: 'string' }, message: { type: 'string' } } } },
    } },
    request_id: { type: 'string' },
  },
} as const;

class HttpError extends Error {
  constructor(public statusCode: number, public code: string, message: string, public details?: { path: string; message: string }[]) { super(message); }
}

function parseTimestamp(value: string): number {
  const parsed = timestampValue.safeParse(value);
  if (!parsed.success) throw new HttpError(400, 'INVALID_TIMESTAMP', 'Timestamp must be a valid RFC 3339 date and time with a timezone');
  return Date.parse(parsed.data);
}

function zodDetails(error: z.ZodError, prefix: string): { path: string; message: string }[] {
  return error.issues.map((issue) => ({ path: `${prefix}/${issue.path.join('/')}`, message: issue.message }));
}

function authorized(request: FastifyRequest, token: string): boolean {
  const match = /^Bearer (.+)$/.exec(request.headers.authorization ?? '');
  if (!match?.[1]) return false;
  const expected = createHash('sha256').update(token).digest();
  const actual = createHash('sha256').update(match[1]).digest();
  return timingSafeEqual(expected, actual);
}

export async function buildApp(config: Config, pool: DatabasePool): Promise<FastifyInstance> {
  const app = Fastify({
    logger: { redact: ['req.headers.authorization', 'request.headers.authorization', 'headers.authorization'] },
    bodyLimit: 128 * 1024,
    ajv: { customOptions: { allErrors: true, removeAdditional: false, coerceTypes: false, useDefaults: false } },
  });
  await app.register(cors, {
    origin: config.dashboardOrigin ?? ['http://localhost:5173', 'http://127.0.0.1:5173'],
    methods: ['GET', 'OPTIONS'],
    allowedHeaders: ['Accept', 'Authorization'],
  });
  await app.register(swagger, {
    openapi: {
      info: { title: 'Minimal Analytics API', version: '0.1.0' },
      components: { securitySchemes: { adminToken: { type: 'http', scheme: 'bearer' } } },
    },
  });
  await app.register(rateLimit, { global: false, max: config.ingestionRateLimit, timeWindow: '1 minute' });

  app.setErrorHandler((error, request, reply) => {
    const fastifyError = error as FastifyError;
    const status = error instanceof HttpError ? error.statusCode : (fastifyError.statusCode ?? 500);
    const code = error instanceof HttpError ? error.code : fastifyError.validation ? 'VALIDATION_ERROR' :
      status === 429 ? 'RATE_LIMITED' : status === 413 ? 'PAYLOAD_TOO_LARGE' : status >= 500 ? 'INTERNAL_ERROR' : 'BAD_REQUEST';
    if (status >= 500) {
      const databaseCode = typeof error === 'object' && error !== null && 'code' in error && typeof error.code === 'string' ? error.code : undefined;
      request.log.error({ databaseCode, message: fastifyError.message }, 'Request failed');
    }
    const details = error instanceof HttpError ? error.details : fastifyError.validation?.map((item) => ({ path: item.instancePath, message: item.message ?? 'Invalid value' }));
    return reply.status(status).send({
      error: { code, message: status >= 500 ? 'Internal server error' : fastifyError.message, ...(details ? { details } : {}) },
      request_id: request.id,
    });
  });

  const requireAdmin = async (request: FastifyRequest): Promise<void> => {
    if (!authorized(request, config.apiToken)) throw new HttpError(401, 'UNAUTHORIZED', 'Valid bearer token required');
  };

  app.get('/health/live', {
    schema: { tags: ['health'], response: { 200: { type: 'object', properties: { status: { type: 'string' } } } } },
  }, async () => ({ status: 'ok' }));

  app.get('/health/ready', {
    schema: { tags: ['health'], response: { 200: { type: 'object', properties: { status: { type: 'string' } } }, 503: errorSchema } },
  }, async (request, reply) => {
    try {
      const result = await pool.query<{ version: string }>('SELECT version FROM schema_migrations WHERE version = $1', ['001_events.sql']);
      if (result.rowCount !== 1) throw new Error('Migration missing');
      return { status: 'ok' };
    } catch (error) {
      request.log.error({ message: error instanceof Error ? error.message : 'Unknown error' }, 'Readiness failed');
      return reply.status(503).send({ error: { code: 'NOT_READY', message: 'Database is not ready' }, request_id: request.id });
    }
  });

  app.post<{ Body: EventBody }>('/events', {
    config: { rateLimit: { max: config.ingestionRateLimit, timeWindow: '1 minute' } },
    schema: {
      tags: ['events'],
      body: {
        type: 'object', required: ['events'], additionalProperties: false,
        properties: { events: { type: 'array', minItems: 1, maxItems: 100, items: {
          type: 'object', required: ['id', 'occurred_at', 'type', 'target_id'], additionalProperties: false,
          properties: {
            id: { type: 'string' },
            occurred_at: timestampSchema,
            type: { type: 'string', enum: ['click', 'view'] },
            target_id: { type: 'string', minLength: 1, maxLength: 512 },
          },
        } } },
      },
      response: {
        200: { type: 'object', required: ['inserted', 'duplicates'], additionalProperties: false, properties: {
          inserted: { type: 'integer' }, duplicates: { type: 'integer' },
        } },
        400: errorSchema, 429: errorSchema,
      },
    },
  }, async (request) => {
    const parsedBody = eventBodySchema.safeParse(request.body);
    if (!parsedBody.success) throw new HttpError(400, 'VALIDATION_ERROR', 'Request validation failed', zodDetails(parsedBody.error, ''));
    const events = parsedBody.data.events;
    if (events.some((event) => event.target_id.trim().length === 0)) {
      throw new HttpError(400, 'INVALID_TARGET', 'target_id must not be blank');
    }
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      let inserted = 0;
      for (const event of events) {
        const result = await client.query(
          `INSERT INTO events (id, occurred_at, type, target_id)
           VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING RETURNING id`,
          [event.id, event.occurred_at, event.type, event.target_id],
        );
        inserted += result.rowCount ?? 0;
      }
      await client.query('COMMIT');
      return { inserted, duplicates: events.length - inserted };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  });

  app.get<{ Querystring: AnalyticsQuery }>('/analytics', {
    preHandler: requireAdmin,
    schema: {
      tags: ['analytics'], security: [{ adminToken: [] }],
      querystring: {
        type: 'object', required: ['from', 'to', 'interval'], additionalProperties: false,
        properties: {
          from: timestampSchema, to: timestampSchema,
          interval: { type: 'string', enum: ['minute', 'hour', 'day'] },
          type: { type: 'string', enum: ['click', 'view'] },
        },
      },
      response: {
        200: { type: 'object', required: ['from', 'to', 'interval', 'buckets', 'totals'], additionalProperties: false,
          properties: {
            from: { type: 'string' }, to: { type: 'string' }, interval: { type: 'string' },
            type: { type: 'string' },
            buckets: { type: 'array', items: { type: 'object', required: ['start', 'clicks', 'views', 'total'], additionalProperties: false,
              properties: { start: { type: 'string' }, clicks: { type: 'integer' }, views: { type: 'integer' }, total: { type: 'integer' } } } },
            totals: { type: 'object', required: ['clicks', 'views', 'total'], additionalProperties: false,
              properties: { clicks: { type: 'integer' }, views: { type: 'integer' }, total: { type: 'integer' } } },
          } },
        400: errorSchema, 401: errorSchema,
      },
    },
  }, async (request) => {
    const { from, to, interval, type } = request.query;
    const fromMs = parseTimestamp(from);
    const toMs = parseTimestamp(to);
    if (fromMs >= toMs) throw new HttpError(400, 'INVALID_RANGE', '`from` must be earlier than `to`');
    const stepMs = { minute: 60_000, hour: 3_600_000, day: 86_400_000 }[interval];
    const firstMs = Math.floor(fromMs / stepMs) * stepMs;
    const lastMs = Math.floor((toMs - 1) / stepMs) * stepMs;
    const count = (lastMs - firstMs) / stepMs + 1;
    if (count > 10_000) throw new HttpError(400, 'TOO_MANY_BUCKETS', 'Range exceeds 10,000 buckets');
    const sqlInterval = { minute: '1 minute', hour: '1 hour', day: '1 day' }[interval];
    const client = await pool.connect();
    try {
      await client.query('BEGIN READ ONLY');
      await client.query("SET LOCAL statement_timeout = '5000ms'");
      const result = await client.query<{ start: Date; clicks: string; views: string }>(
        `WITH counts AS (
           SELECT date_bin($3::interval, occurred_at, '1970-01-01T00:00:00Z'::timestamptz) AS start,
                  COUNT(*) FILTER (WHERE type = 'click') AS clicks,
                  COUNT(*) FILTER (WHERE type = 'view') AS views
           FROM events
           WHERE occurred_at >= $1::timestamptz AND occurred_at < $2::timestamptz
             AND ($4::text IS NULL OR type = $4)
           GROUP BY 1
         )
         SELECT series.start, COALESCE(counts.clicks, 0)::text AS clicks,
                COALESCE(counts.views, 0)::text AS views
         FROM generate_series($5::timestamptz, $6::timestamptz, $3::interval) AS series(start)
         LEFT JOIN counts USING (start)
         ORDER BY series.start`,
        [from, to, sqlInterval, type ?? null, new Date(firstMs).toISOString(), new Date(lastMs).toISOString()],
      );
      await client.query('COMMIT');
      const buckets = result.rows.map((row) => {
        const clicks = Number(row.clicks);
        const views = Number(row.views);
        return { start: row.start.toISOString(), clicks, views, total: clicks + views };
      });
      const totals = buckets.reduce((sum, bucket) => ({
        clicks: sum.clicks + bucket.clicks, views: sum.views + bucket.views, total: sum.total + bucket.total,
      }), { clicks: 0, views: 0, total: 0 });
      return { from: new Date(fromMs).toISOString(), to: new Date(toMs).toISOString(), interval, ...(type ? { type } : {}), buckets, totals };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  });

  app.get('/openapi.json', { schema: { hide: true } }, async () => app.swagger());
  return app;
}
