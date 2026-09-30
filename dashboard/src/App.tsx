import { useMemo, useState, type FormEvent } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import {
  API_BASE_URL,
  ApiError,
  datetimeInputValue,
  fetchAnalytics,
  inputValueToUtc,
  validateRange,
  type AnalyticsBucket,
  type AnalyticsFilters,
  type EventFilter,
  type Interval,
} from './analytics';

const initialTo = new Date();
initialTo.setUTCSeconds(0, 0);
const initialFrom = new Date(initialTo.getTime() - 24 * 60 * 60 * 1000);

function randomSessionId(): string {
  return globalThis.crypto?.randomUUID?.() ?? `${Date.now()}-${Math.random()}`;
}

function formatNumber(value: number): string {
  return new Intl.NumberFormat('en-US').format(value);
}

function formatBucketTime(value: string, interval: Interval): string {
  const date = new Date(value);
  const options: Intl.DateTimeFormatOptions = interval === 'day'
    ? { month: 'short', day: 'numeric', timeZone: 'UTC' }
    : interval === 'minute'
      ? { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', timeZone: 'UTC', hourCycle: 'h23' }
      : { month: 'short', day: 'numeric', hour: '2-digit', timeZone: 'UTC', hourCycle: 'h23' };
  return new Intl.DateTimeFormat('en-US', options).format(date);
}

function SeriesChart({ buckets, interval }: { buckets: AnalyticsBucket[]; interval: Interval }) {
  const width = 760;
  const height = 260;
  const left = 42;
  const right = 36;
  const top = 14;
  const bottom = 44;
  const plotWidth = width - left - right;
  const plotHeight = height - top - bottom;
  const groupSize = Math.max(1, Math.ceil(buckets.length / 120));
  const groups = Array.from({ length: Math.ceil(buckets.length / groupSize) }, (_, index) => {
    const startIndex = index * groupSize;
    const items = buckets.slice(startIndex, startIndex + groupSize);
    return {
      start: items[0]!.start,
      end: items[items.length - 1]!.start,
      bucketCount: items.length,
      clicks: items.reduce((sum, bucket) => sum + bucket.clicks, 0),
      views: items.reduce((sum, bucket) => sum + bucket.views, 0),
    };
  });
  const max = Math.max(1, ...groups.map((group) => Math.max(group.clicks, group.views)));
  const groupWidth = plotWidth / Math.max(1, groups.length);
  const barGap = Math.min(4, groupWidth * 0.16);
  const barWidth = (groupWidth - barGap) / 2;
  const ticks = Array.from({ length: Math.min(5, groups.length) }, (_, index) => {
    const groupIndex = groups.length <= 1 ? 0 : Math.round(index * (groups.length - 1) / (Math.min(5, groups.length) - 1));
    return { group: groups[groupIndex], x: left + (groupIndex + 0.5) * groupWidth };
  });

  if (buckets.length === 0) {
    return <div className="chart-empty">No buckets are available for this time range.</div>;
  }

  return (
    <svg className="chart" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Grouped bar chart of clicks and views over time, UTC">
      {[0, 0.5, 1].map((fraction) => {
        const y = top + plotHeight * fraction;
        return <g key={fraction}>
          <line x1={left} x2={width - right} y1={y} y2={y} className="chart-grid" />
          <text x={left - 9} y={y + 4} textAnchor="end" className="chart-y-label">{formatNumber(Math.round(max * (1 - fraction)))}</text>
        </g>;
      })}
      {groups.map((group, index) => {
        const x = left + index * groupWidth;
        const clicksHeight = group.clicks / max * plotHeight;
        const viewsHeight = group.views / max * plotHeight;
        const period = group.bucketCount > 1
          ? `${formatBucketTime(group.start, interval)} – ${formatBucketTime(group.end, interval)} UTC · ${group.bucketCount} ${interval} buckets`
          : `${formatBucketTime(group.start, interval)} UTC`;
        return <g key={`${group.start}-${index}`}>
          <rect x={x} y={top + plotHeight - clicksHeight} width={barWidth} height={clicksHeight} rx="2" className="chart-bar chart-click-bar">
            <title>{period} · {formatNumber(group.clicks)} clicks</title>
          </rect>
          <rect x={x + barWidth + barGap} y={top + plotHeight - viewsHeight} width={barWidth} height={viewsHeight} rx="2" className="chart-bar chart-view-bar">
            <title>{period} · {formatNumber(group.views)} views</title>
          </rect>
        </g>;
      })}
      {ticks.map(({ group, x }, index) => group && <text key={`${group.start}-${index}`} x={x} y={height - 12} textAnchor="middle" className="chart-x-label">
        {formatBucketTime(group.start, interval)}
      </text>)}
    </svg>
  );
}

export default function App() {
  const queryClient = useQueryClient();
  const [tokenInput, setTokenInput] = useState('');
  const [token, setToken] = useState('');
  const [sessionId, setSessionId] = useState('');
  const [fromInput, setFromInput] = useState(datetimeInputValue(initialFrom));
  const [toInput, setToInput] = useState(datetimeInputValue(initialTo));
  const [type, setType] = useState<EventFilter>('all');
  const [interval, setInterval] = useState<Interval>('hour');
  const from = inputValueToUtc(fromInput);
  const to = inputValueToUtc(toInput);
  const rangeError = validateRange(from, to);
  const filters: AnalyticsFilters = { from, to, interval, type };

  const analytics = useQuery({
    queryKey: ['analytics', sessionId, from, to, interval, type],
    queryFn: () => fetchAnalytics(filters, token, fetch, API_BASE_URL),
    enabled: Boolean(token && !rangeError),
    staleTime: 30_000,
  });
  const error = analytics.error;
  const unauthorized = error instanceof ApiError && error.status === 401;

  const chartBuckets = useMemo(() => analytics.data?.buckets ?? [], [analytics.data]);

  function connect(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const nextToken = tokenInput.trim();
    if (!nextToken) return;
    setToken(nextToken);
    setTokenInput('');
    setSessionId(randomSessionId());
  }

  function disconnect() {
    queryClient.clear();
    setToken('');
    setSessionId('');
    setTokenInput('');
  }

  function applyPreset(hours: number) {
    const end = new Date();
    end.setUTCSeconds(0, 0);
    setToInput(datetimeInputValue(end));
    setFromInput(datetimeInputValue(new Date(end.getTime() - hours * 60 * 60 * 1000)));
  }

  const lastUpdated = analytics.dataUpdatedAt
    ? new Intl.DateTimeFormat('en-US', { hour: '2-digit', minute: '2-digit', timeZone: 'UTC', hourCycle: 'h23' }).format(analytics.dataUpdatedAt)
    : null;

  return (
    <main className="page-shell">
      <header className="topbar">
        <a className="brand" href="#dashboard" aria-label="Northstar Analytics dashboard">
          <span className="brand-mark" aria-hidden="true"><span /></span>
          <span>northstar<span className="brand-light"> / analytics</span></span>
        </a>
        <div className="topbar-right">
          <span className="system-status"><span className="status-dot" /> Historical data</span>
          {token && <button className="quiet-button" onClick={disconnect} type="button">Disconnect</button>}
        </div>
      </header>

      <div className="content" id="dashboard">
        <section className="intro-row">
          <div>
            <div className="eyebrow">ANALYTICS / OVERVIEW</div>
            <h1>Dashboard</h1>
            <p className="subtitle">Understand how your audience engages over time.</p>
          </div>
          <div className="range-badge"><span className="clock-icon" aria-hidden="true">◷</span> All times shown in UTC</div>
        </section>

        {!token ? (
          <section className="auth-card" aria-labelledby="auth-title">
            <div className="auth-icon" aria-hidden="true">⌘</div>
            <div className="auth-copy">
              <h2 id="auth-title">Connect to your analytics</h2>
              <p>Enter your admin API token to load historical event data. Your token stays in memory and is never saved in this browser.</p>
            </div>
            <form className="token-form" onSubmit={connect}>
              <label htmlFor="api-token">Admin API token</label>
              <div className="token-input-row">
                <input id="api-token" type="password" autoComplete="off" value={tokenInput} onChange={(event) => setTokenInput(event.target.value)} placeholder="Paste your API token" />
                <button className="primary-button" disabled={!tokenInput.trim()} type="submit">Connect <span aria-hidden="true">→</span></button>
              </div>
            </form>
          </section>
        ) : (
          <>
            <section className="filters-card" aria-label="Analytics filters">
              <div className="filters-heading">
                <div><h2>Explore events</h2><p>Choose a time range and breakdown for your data.</p></div>
                <div className="quick-ranges" aria-label="Quick date ranges">
                  <button type="button" onClick={() => applyPreset(24)}>24 hours</button>
                  <button type="button" onClick={() => applyPreset(24 * 7)}>7 days</button>
                </div>
              </div>
              <div className="filter-controls">
                <div className="field range-field">
                  <label htmlFor="from-time">From <span>UTC</span></label>
                  <input id="from-time" type="datetime-local" step="60" value={fromInput} onChange={(event) => setFromInput(event.target.value)} />
                </div>
                <div className="field range-field">
                  <label htmlFor="to-time">To <span>UTC · exclusive</span></label>
                  <input id="to-time" type="datetime-local" step="60" value={toInput} onChange={(event) => setToInput(event.target.value)} />
                </div>
                <div className="field select-field">
                  <label htmlFor="event-type">Event type</label>
                  <select id="event-type" value={type} onChange={(event) => setType(event.target.value as EventFilter)}>
                    <option value="all">All events</option><option value="click">Clicks</option><option value="view">Views</option>
                  </select>
                </div>
                <div className="field select-field">
                  <label htmlFor="interval">Group by</label>
                  <select id="interval" value={interval} onChange={(event) => setInterval(event.target.value as Interval)}>
                    <option value="minute">Minute</option><option value="hour">Hour</option><option value="day">Day</option>
                  </select>
                </div>
              </div>
              {rangeError && <p className="inline-error" role="alert">{rangeError}</p>}
            </section>

            {analytics.isPending && !rangeError && <section className="state-card" role="status"><span className="spinner" /> Loading analytics…</section>}
            {analytics.isError && !rangeError && <section className="error-card" role="alert">
              <div className="state-icon">!</div><div><h2>{unauthorized ? 'Access denied' : 'Could not load analytics'}</h2>
                <p>{unauthorized ? 'That token was not accepted. Check it and connect again.' : analytics.error?.message || 'Check the API connection and try again.'}</p>
                {unauthorized && <button className="text-button" onClick={disconnect} type="button">Enter token again</button>}
                {!unauthorized && <button className="text-button" onClick={() => void analytics.refetch()} type="button">Try again</button>}
              </div>
            </section>}

            {analytics.isSuccess && !rangeError && (
              <>
                <section className="metrics-grid" aria-label="Event totals">
                  <article className="metric-card total-card"><div className="metric-heading"><span>Total events</span><span className="metric-symbol total-symbol">Σ</span></div><div className="metric-value">{formatNumber(analytics.data.totals.total)}</div><div className="metric-foot">In selected time range</div></article>
                  <article className="metric-card"><div className="metric-heading"><span>Clicks</span><span className="metric-symbol clicks-symbol">↗</span></div><div className="metric-value">{formatNumber(analytics.data.totals.clicks)}</div><div className="metric-foot">Click events</div></article>
                  <article className="metric-card"><div className="metric-heading"><span>Views</span><span className="metric-symbol views-symbol">◉</span></div><div className="metric-value">{formatNumber(analytics.data.totals.views)}</div><div className="metric-foot">View events</div></article>
                </section>
                <section className="chart-card" aria-labelledby="chart-title">
                  <div className="chart-header"><div><h2 id="chart-title">Event volume</h2><p>Activity by {interval}, grouped in UTC</p></div>
                    {lastUpdated && <span className="updated-label">Updated {lastUpdated} UTC</span>}
                  </div>
                  {analytics.data.buckets.length === 0 || analytics.data.totals.total === 0 ? <div className="empty-state"><div className="empty-icon">⌁</div><h3>No activity in this range</h3><p>Try a wider date range or select a different event type.</p></div> : <>
                    <div className="chart-legend"><span><i className="legend-dot clicks-dot" /> Clicks</span><span><i className="legend-dot views-dot" /> Views</span></div>
                    <SeriesChart buckets={chartBuckets} interval={interval} />
                  </>}
                  <div className="chart-footnote">Buckets are UTC-aligned; the selected end time is exclusive.</div>
                </section>
              </>
            )}
          </>
        )}
        <footer className="page-footer"><span>Northstar Analytics</span><span>Historical data · no live stream</span></footer>
      </div>
    </main>
  );
}
