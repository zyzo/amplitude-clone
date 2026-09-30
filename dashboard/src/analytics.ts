export const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL ?? '').replace(/\/+$/, '');

export type EventType = 'click' | 'view';
export type EventFilter = EventType | 'all';
export type Interval = 'minute' | 'hour' | 'day';

export interface AnalyticsBucket {
  start: string;
  clicks: number;
  views: number;
  total: number;
}

export interface AnalyticsResponse {
  from: string;
  to: string;
  interval: Interval;
  type?: EventType;
  buckets: AnalyticsBucket[];
  totals: { clicks: number; views: number; total: number };
}

export interface AnalyticsFilters {
  from: string;
  to: string;
  interval: Interval;
  type: EventFilter;
}

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly code?: string,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

export function buildAnalyticsUrl(filters: AnalyticsFilters, baseUrl = ''): string {
  const params = new URLSearchParams({
    from: filters.from,
    to: filters.to,
    interval: filters.interval,
  });
  if (filters.type !== 'all') params.set('type', filters.type);
  return `${baseUrl}/analytics?${params.toString()}`;
}

export async function fetchAnalytics(
  filters: AnalyticsFilters,
  token: string,
  fetcher: typeof fetch = fetch,
  baseUrl = '',
): Promise<AnalyticsResponse> {
  const response = await fetcher(buildAnalyticsUrl(filters, baseUrl), {
    headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' },
  });

  if (!response.ok) {
    let message = `Request failed (${response.status})`;
    let code: string | undefined;
    try {
      const body = await response.json() as { error?: { message?: string; code?: string } };
      message = body.error?.message ?? message;
      code = body.error?.code;
    } catch {
      // The API may return a non-JSON response through a proxy.
    }
    throw new ApiError(message, response.status, code);
  }

  return await response.json() as AnalyticsResponse;
}

export function validateRange(from: string, to: string): string | null {
  if (!from || !to) return 'Choose both a start and end time.';
  const fromMs = Date.parse(from);
  const toMs = Date.parse(to);
  if (!Number.isFinite(fromMs) || !Number.isFinite(toMs)) return 'Enter valid UTC dates and times.';
  if (fromMs >= toMs) return 'The start time must be earlier than the end time.';
  return null;
}

export function datetimeInputValue(date: Date): string {
  return date.toISOString().slice(0, 16);
}

export function inputValueToUtc(value: string): string {
  return `${value}:00Z`;
}
