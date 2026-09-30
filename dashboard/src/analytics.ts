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

export interface RecentEvent {
  id: string;
  occurred_at: string;
  type: EventType;
  target_id: string;
}

export interface StreamEvent extends RecentEvent {
  received_at: string;
}

export interface SnapshotResponse {
  from: string;
  to: string;
  totals: { clicks: number; views: number; total: number };
  recent: RecentEvent[];
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

export async function fetchSnapshot(
  token: string,
  fetcher: typeof fetch = fetch,
  baseUrl = '',
): Promise<SnapshotResponse> {
  const response = await fetcher(`${baseUrl}/events/snapshot`, {
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
      // A proxy can return a non-JSON response.
    }
    throw new ApiError(message, response.status, code);
  }
  return await response.json() as SnapshotResponse;
}

export interface EventStreamHandlers {
  onOpen: () => void;
  onEvent: (event: StreamEvent) => void;
  onError: (error: Error) => void;
  onConnecting: () => void;
}

export function startEventStream(
  token: string,
  handlers: EventStreamHandlers,
  fetcher: typeof fetch = fetch,
  baseUrl = '',
): () => void {
  const controller = new AbortController();
  let stopped = false;
  let retryDelay = 1_000;
  let retryTimer: ReturnType<typeof setTimeout> | undefined;

  const connect = async () => {
    if (stopped) return;
    handlers.onConnecting();
    try {
      const response = await fetcher(`${baseUrl}/events/stream`, {
        headers: { Authorization: `Bearer ${token}`, Accept: 'text/event-stream' },
        signal: controller.signal,
      });
      if (!response.ok) {
        let message = `Stream request failed (${response.status})`;
        let code: string | undefined;
        try {
          const body = await response.json() as { error?: { message?: string; code?: string } };
          message = body.error?.message ?? message;
          code = body.error?.code;
        } catch { /* Ignore non-JSON proxy errors. */ }
        const error = new ApiError(message, response.status, code);
        if (response.status === 401) {
          handlers.onError(error);
          return;
        }
        throw error;
      }
      if (!response.body) throw new Error('The event stream response has no body.');
      retryDelay = 1_000;
      handlers.onOpen();
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      while (!stopped) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, '\n');
        let boundary = buffer.indexOf('\n\n');
        while (boundary >= 0) {
          const frame = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);
          const data = frame.split('\n').filter((line) => line.startsWith('data:')).map((line) => line.slice(5).trim()).join('\n');
          if (data) {
            try { handlers.onEvent(JSON.parse(data) as StreamEvent); } catch { /* Ignore malformed frames. */ }
          }
          boundary = buffer.indexOf('\n\n');
        }
      }
      if (!stopped) throw new Error('The event stream closed.');
    } catch (error) {
      if (stopped || (error instanceof Error && error.name === 'AbortError')) return;
      handlers.onError(error instanceof Error ? error : new Error('Event stream disconnected.'));
      retryTimer = setTimeout(() => { void connect(); }, retryDelay + Math.random() * retryDelay * 0.25);
      retryDelay = Math.min(retryDelay * 2, 30_000);
    }
  };

  void connect();
  return () => {
    stopped = true;
    controller.abort();
    if (retryTimer) clearTimeout(retryTimer);
  };
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
