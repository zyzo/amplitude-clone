import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import App from './App';
import { API_BASE_URL } from './analytics';

const liveSnapshot = {
  from: '2026-09-30T00:00:00.000Z', to: '2026-09-30T12:00:00.000Z',
  totals: { clicks: 0, views: 0, total: 0 }, recent: [],
};

function dashboardFetchMock(analyticsFetch: typeof fetch = () => Promise.resolve(successResponse())) {
  return vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    if (url.endsWith('/events/stream')) return Promise.resolve(new Response(': connected\n\n', { status: 200, headers: { 'Content-Type': 'text/event-stream' } }));
    if (url.endsWith('/events/snapshot')) return Promise.resolve(new Response(JSON.stringify(liveSnapshot), { status: 200 }));
    return analyticsFetch(input, init);
  });
}

function renderApp() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={queryClient}><App /></QueryClientProvider>);
}

function successResponse() {
  return new Response(JSON.stringify({
    from: '2026-09-30T09:00:00.000Z',
    to: '2026-09-30T12:00:00.000Z',
    interval: 'hour',
    buckets: [{ start: '2026-09-30T09:00:00.000Z', clicks: 12, views: 8, total: 20 }],
    totals: { clicks: 12, views: 8, total: 20 },
  }), { status: 200, headers: { 'Content-Type': 'application/json' } });
}

describe('dashboard', () => {
  it('keeps the token in memory and loads totals using UTC filters', async () => {
    const user = userEvent.setup();
    const fetchMock = dashboardFetchMock();
    vi.stubGlobal('fetch', fetchMock);
    renderApp();

    expect(screen.getByText(/never saved in this browser/i)).toBeTruthy();
    await user.type(screen.getByLabelText('Admin API token'), 'admin-secret');
    await user.click(screen.getByRole('button', { name: /connect/i }));
    await user.click(await screen.findByRole('button', { name: 'Historical analytics' }));
    expect(await screen.findByText('20')).toBeTruthy();
    const totals = within(screen.getByRole('region', { name: 'Event totals' }));
    expect(totals.getByText('12')).toBeTruthy();
    expect(totals.getByText('8')).toBeTruthy();
    const chart = screen.getByRole('img', { name: /grouped bar chart/i });
    expect(chart.querySelectorAll('.chart-click-bar')).toHaveLength(1);
    expect(chart.querySelectorAll('.chart-view-bar')).toHaveLength(1);
    const clickBar = chart.querySelector<SVGRectElement>('.chart-click-bar')!;
    const viewBar = chart.querySelector<SVGRectElement>('.chart-view-bar')!;
    const dateLabel = chart.querySelector<SVGTextElement>('.chart-x-label')!;
    const barGroupCenter = (Number(clickBar.getAttribute('x')) + Number(viewBar.getAttribute('x')) + Number(viewBar.getAttribute('width'))) / 2;
    expect(Number(dateLabel.getAttribute('x'))).toBeCloseTo(barGroupCenter);
    expect(dateLabel.getAttribute('text-anchor')).toBe('middle');
    expect(chart.textContent).toContain('12 clicks');
    expect(chart.textContent).toContain('8 views');
    expect(localStorage.length).toBe(0);

    const analyticsCall = fetchMock.mock.calls.find(([url]) => new URL(String(url), window.location.origin).pathname === '/analytics')!;
    const [url, options] = analyticsCall as [string, RequestInit];
    const request = new URL(url, window.location.origin);
    expect(request.pathname).toBe('/analytics');
    expect(request.origin).toBe(new URL(API_BASE_URL || window.location.origin).origin);
    expect(request.searchParams.get('interval')).toBe('hour');
    expect(request.searchParams.has('from')).toBe(true);
    expect(request.searchParams.has('to')).toBe(true);
    expect(options.headers).toMatchObject({ Authorization: 'Bearer admin-secret' });
    expect(url).not.toContain('admin-secret');

    await user.selectOptions(screen.getByLabelText('Event type'), 'click');
    await waitFor(() => expect(fetchMock.mock.calls.filter(([url]) => new URL(String(url), window.location.origin).pathname === '/analytics')).toHaveLength(2));
    const analyticsCalls = () => fetchMock.mock.calls.filter(([url]) => new URL(String(url), window.location.origin).pathname === '/analytics');
    expect(new URL(analyticsCalls()[1]?.[0] as string, window.location.origin).searchParams.get('type')).toBe('click');
    fireEvent.change(screen.getByLabelText(/from/i), { target: { value: '2026-09-30T08:15' } });
    await waitFor(() => expect(analyticsCalls()).toHaveLength(3));
    const rangeRequest = new URL(analyticsCalls()[2]?.[0] as string, window.location.origin);
    expect(rangeRequest.searchParams.get('from')).toBe('2026-09-30T08:15:00Z');
    await user.selectOptions(screen.getByLabelText('Group by'), 'day');
    await waitFor(() => expect(analyticsCalls()).toHaveLength(4));
    expect(new URL(analyticsCalls()[3]?.[0] as string, window.location.origin).searchParams.get('interval')).toBe('day');
  });

  it('prevents invalid ranges and presents an unauthorized state clearly', async () => {
    const user = userEvent.setup();
    const unauthorizedFetch = vi.fn().mockResolvedValue(new Response(JSON.stringify({ error: { code: 'UNAUTHORIZED', message: 'Valid bearer token required' } }), { status: 401 }));
    const fetchMock = dashboardFetchMock(unauthorizedFetch);
    vi.stubGlobal('fetch', fetchMock);
    renderApp();
    await user.type(screen.getByLabelText('Admin API token'), 'incorrect');
    await user.click(screen.getByRole('button', { name: /connect/i }));
    await user.click(await screen.findByRole('button', { name: 'Historical analytics' }));
    expect(await screen.findByRole('heading', { name: 'Access denied' })).toBeTruthy();
    expect(screen.getByText(/token was not accepted/i)).toBeTruthy();

    await user.click(screen.getByRole('button', { name: /enter token again/i }));
    await user.type(screen.getByLabelText('Admin API token'), 'valid-token');
    await user.click(screen.getByRole('button', { name: /connect/i }));
    await user.click(await screen.findByRole('button', { name: 'Historical analytics' }));
    await screen.findByRole('heading', { name: 'Access denied' });
    fireEvent.change(screen.getByLabelText(/from/i), { target: { value: '2026-09-30T12:00' } });
    fireEvent.change(screen.getByLabelText(/to/i), { target: { value: '2026-09-30T09:00' } });
    expect(await screen.findByText(/start time must be earlier/i)).toBeTruthy();
    expect(unauthorizedFetch).toHaveBeenCalledTimes(2);
  });

  it('shows a clear empty state when the selected range has no matching events', async () => {
    const user = userEvent.setup();
    const emptyResponse = successResponse();
    const body = await emptyResponse.json();
    body.buckets = [{ start: '2026-09-30T09:00:00.000Z', clicks: 0, views: 0, total: 0 }];
    body.totals = { clicks: 0, views: 0, total: 0 };
    vi.stubGlobal('fetch', dashboardFetchMock(() => Promise.resolve(new Response(JSON.stringify(body), { status: 200 }))));
    renderApp();
    await user.type(screen.getByLabelText('Admin API token'), 'admin-secret');
    await user.click(screen.getByRole('button', { name: /connect/i }));
    await user.click(await screen.findByRole('button', { name: 'Historical analytics' }));
    expect(await screen.findByRole('heading', { name: 'No activity in this range' })).toBeTruthy();
    expect(screen.getAllByText('0')).toHaveLength(3);
  });

  it('shows database-backed live counters and applies streamed committed events', async () => {
    const user = userEvent.setup();
    const event = {
      id: 'a7f18018-cfb2-4df8-a871-7530afad14b3', occurred_at: '2026-09-30T11:59:00.000Z',
      received_at: '2026-09-30T11:59:01.000Z', type: 'click', target_id: 'signup-button',
    };
    const fetchMock = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith('/events/stream')) {
        const frame = `id: ${event.id}\nevent: event\ndata: ${JSON.stringify(event)}\n\n`;
        const stream = new ReadableStream<Uint8Array>({ start(controller) { controller.enqueue(new TextEncoder().encode(frame)); controller.close(); } });
        return Promise.resolve(new Response(stream, { status: 200, headers: { 'Content-Type': 'text/event-stream' } }));
      }
      if (url.endsWith('/events/snapshot')) return Promise.resolve(new Response(JSON.stringify(liveSnapshot), { status: 200 }));
      return Promise.resolve(successResponse());
    });
    vi.stubGlobal('fetch', fetchMock);
    renderApp();
    await user.type(screen.getByLabelText('Admin API token'), 'admin-secret');
    await user.click(screen.getByRole('button', { name: /connect/i }));

    const totals = within(await screen.findByRole('region', { name: "Today's event totals" }));
    await waitFor(() => expect(totals.getAllByText('1')).toHaveLength(2));
    expect(totals.getAllByText('1')).toHaveLength(2);
    expect(await screen.findByText('signup-button')).toBeTruthy();
    expect(screen.getByText(/Live updates connected|Reconnecting to live updates/)).toBeTruthy();
    const streamCall = fetchMock.mock.calls.find(([url]) => String(url).endsWith('/events/stream'));
    expect(streamCall?.[1]?.headers).toMatchObject({ Authorization: 'Bearer admin-secret', Accept: 'text/event-stream' });
  });

  it('shows a loading state while the analytics request is pending', async () => {
    const user = userEvent.setup();
    vi.stubGlobal('fetch', dashboardFetchMock(() => new Promise<Response>(() => undefined)));
    renderApp();
    await user.type(screen.getByLabelText('Admin API token'), 'admin-secret');
    await user.click(screen.getByRole('button', { name: /connect/i }));
    await user.click(await screen.findByRole('button', { name: 'Historical analytics' }));
    expect(await screen.findByRole('status')).toBeTruthy();
  });
});
