import { describe, expect, it, vi } from 'vitest';
import { ApiError, buildAnalyticsUrl, datetimeInputValue, fetchAnalytics, fetchSnapshot, inputValueToUtc, startEventStream, validateRange, type AnalyticsFilters } from './analytics';

const filters: AnalyticsFilters = {
  from: '2026-09-30T09:00:00Z',
  to: '2026-09-30T12:00:00Z',
  interval: 'hour',
  type: 'click',
};

const responseBody = {
  from: filters.from,
  to: filters.to,
  interval: filters.interval,
  type: 'click',
  buckets: [{ start: '2026-09-30T09:00:00.000Z', clicks: 2, views: 0, total: 2 }],
  totals: { clicks: 2, views: 0, total: 2 },
};

describe('analytics API helpers', () => {
  it('builds encoded query parameters and omits the all-events type filter', () => {
    const url = new URL(buildAnalyticsUrl(filters, 'https://analytics.example'));
    expect(url.pathname).toBe('/analytics');
    expect(Object.fromEntries(url.searchParams)).toEqual({
      from: filters.from,
      to: filters.to,
      interval: 'hour',
      type: 'click',
    });
    expect(buildAnalyticsUrl({ ...filters, type: 'all' })).not.toContain('type=');
  });

  it('sends the admin token in the Bearer header only', async () => {
    const fetcher = vi.fn().mockResolvedValue(new Response(JSON.stringify(responseBody), { status: 200 }));
    await fetchAnalytics(filters, 'secret-admin-token', fetcher);
    expect(fetcher).toHaveBeenCalledWith(expect.stringContaining('/analytics?'), {
      headers: { Authorization: 'Bearer secret-admin-token', Accept: 'application/json' },
    });
    expect(fetcher.mock.calls[0]?.[0]).not.toContain('secret-admin-token');
  });

  it('surfaces API validation messages and unauthorized responses', async () => {
    const fetcher = vi.fn().mockResolvedValue(new Response(JSON.stringify({ error: { code: 'INVALID_RANGE', message: 'Invalid date range' } }), { status: 400 }));
    await expect(fetchAnalytics(filters, 'token', fetcher)).rejects.toMatchObject({
      name: 'ApiError', status: 400, code: 'INVALID_RANGE', message: 'Invalid date range',
    });
    fetcher.mockResolvedValue(new Response(JSON.stringify({ error: { code: 'UNAUTHORIZED', message: 'Valid bearer token required' } }), { status: 401 }));
    await expect(fetchAnalytics(filters, 'wrong-token', fetcher)).rejects.toBeInstanceOf(ApiError);
  });

  it('fetches a protected live snapshot without exposing the token in its URL', async () => {
    const snapshot = { from: filters.from, to: filters.to, totals: { clicks: 1, views: 2, total: 3 }, recent: [] };
    const fetcher = vi.fn().mockResolvedValue(new Response(JSON.stringify(snapshot), { status: 200 }));
    await expect(fetchSnapshot('admin-secret', fetcher, 'https://analytics.example')).resolves.toEqual(snapshot);
    expect(fetcher).toHaveBeenCalledWith('https://analytics.example/events/snapshot', {
      headers: { Authorization: 'Bearer admin-secret', Accept: 'application/json' },
    });
  });

  it('uses authenticated fetch-based SSE and parses event frames', async () => {
    const event = { id: 'event-id', occurred_at: filters.from, received_at: filters.from, type: 'click', target_id: 'button' };
    const frame = `id: ${event.id}\nevent: event\ndata: ${JSON.stringify(event)}\n\n`;
    const stream = new ReadableStream<Uint8Array>({ start(controller) { controller.enqueue(new TextEncoder().encode(frame)); controller.close(); } });
    const fetcher = vi.fn().mockResolvedValue(new Response(stream, { status: 200, headers: { 'Content-Type': 'text/event-stream' } }));
    const onEvent = vi.fn();
    const stop = startEventStream('admin-secret', { onOpen: vi.fn(), onEvent, onError: vi.fn(), onConnecting: vi.fn() }, fetcher, 'https://analytics.example');
    await vi.waitFor(() => expect(onEvent).toHaveBeenCalledWith(event));
    stop();
    expect(fetcher).toHaveBeenCalledWith('https://analytics.example/events/stream', expect.objectContaining({
      headers: { Authorization: 'Bearer admin-secret', Accept: 'text/event-stream' },
    }));
  });

  it('validates half-open range selections and translates UTC datetime inputs', () => {
    expect(validateRange('', filters.to)).toBe('Choose both a start and end time.');
    expect(validateRange(filters.to, filters.from)).toContain('earlier');
    expect(validateRange('not a date', filters.to)).toContain('valid UTC');
    expect(validateRange(filters.from, filters.to)).toBeNull();
    expect(inputValueToUtc('2026-09-30T09:00')).toBe('2026-09-30T09:00:00Z');
    expect(datetimeInputValue(new Date('2026-09-30T09:00:00Z'))).toBe('2026-09-30T09:00');
  });
});
