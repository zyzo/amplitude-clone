import { describe, expect, it, vi } from 'vitest';
import { ApiError, buildAnalyticsUrl, datetimeInputValue, fetchAnalytics, inputValueToUtc, validateRange, type AnalyticsFilters } from './analytics';

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

  it('validates half-open range selections and translates UTC datetime inputs', () => {
    expect(validateRange('', filters.to)).toBe('Choose both a start and end time.');
    expect(validateRange(filters.to, filters.from)).toContain('earlier');
    expect(validateRange('not a date', filters.to)).toContain('valid UTC');
    expect(validateRange(filters.from, filters.to)).toBeNull();
    expect(inputValueToUtc('2026-09-30T09:00')).toBe('2026-09-30T09:00:00Z');
    expect(datetimeInputValue(new Date('2026-09-30T09:00:00Z'))).toBe('2026-09-30T09:00');
  });
});
