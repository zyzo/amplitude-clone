import type { DatabasePool } from './db.js';

export type EventType = 'click' | 'view';
export interface InputEvent {
  id: string;
  occurred_at: string;
  type: EventType;
  target_id: string;
}
export interface StreamEvent extends InputEvent { received_at: string }

export interface EventQueue {
  enqueue(events: InputEvent[]): Promise<void>;
  start(handler: (event: StreamEvent) => Promise<void>): Promise<void>;
  close(): Promise<void>;
}

export async function handleEvent(pool: DatabasePool, event: InputEvent): Promise<{ event: StreamEvent; inserted: boolean }> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await client.query<{
      id: string;
      occurred_at: Date;
      type: EventType;
      target_id: string;
      received_at: Date;
    }>(
      `INSERT INTO events (id, occurred_at, type, target_id)
       VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING
       RETURNING id, occurred_at, type, target_id, received_at`,
      [event.id, event.occurred_at, event.type, event.target_id],
    );
    const inserted = result.rows[0];
    const row = inserted ?? (await client.query<{
      id: string;
      occurred_at: Date;
      type: EventType;
      target_id: string;
      received_at: Date;
    }>(
      'SELECT id, occurred_at, type, target_id, received_at FROM events WHERE id = $1', [event.id],
    )).rows[0];
    if (!row) throw new Error(`Event ${event.id} was not available after insert`);
    await client.query('COMMIT');
    return {
      event: {
        id: row.id,
        occurred_at: row.occurred_at.toISOString(),
        type: row.type,
        target_id: row.target_id,
        received_at: row.received_at.toISOString(),
      },
      inserted: Boolean(inserted),
    };
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}
