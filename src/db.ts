import pg from 'pg';

export function createPool(databaseUrl: string): pg.Pool {
  return new pg.Pool({ connectionString: databaseUrl, max: 10, connectionTimeoutMillis: 5000 });
}

export type DatabasePool = pg.Pool;
