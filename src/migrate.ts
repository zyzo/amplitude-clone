import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { createPool, type DatabasePool } from './db.js';
import { loadConfig } from './config.js';

export async function migrate(pool: DatabasePool, directory = path.join(process.cwd(), 'migrations')): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock(873648103)');
    await client.query(`CREATE TABLE IF NOT EXISTS schema_migrations (
      version text PRIMARY KEY,
      applied_at timestamptz NOT NULL DEFAULT now()
    )`);
    const files = (await readdir(directory)).filter((name) => /^\d+_[a-z0-9_-]+\.sql$/.test(name)).sort();
    if (files.length === 0) throw new Error('No SQL migrations found');
    const rows = await client.query<{ version: string }>('SELECT version FROM schema_migrations');
    const applied = new Set(rows.rows.map((row) => row.version));
    for (const file of files) {
      if (applied.has(file)) continue;
      const sql = await readFile(path.join(directory, file), 'utf8');
      await client.query(sql);
      await client.query('INSERT INTO schema_migrations (version) VALUES ($1)', [file]);
    }
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

if (process.argv[1] && import.meta.url === new URL(`file://${process.argv[1]}`).href) {
  const pool = createPool(loadConfig().databaseUrl);
  migrate(pool).then(
    async () => { await pool.end(); },
    async (error: unknown) => { console.error('Migration failed:', error); await pool.end(); process.exitCode = 1; },
  );
}
