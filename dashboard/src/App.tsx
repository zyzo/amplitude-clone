import { useEffect, useMemo, useRef, useState, type FormEvent } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import {
  API_BASE_URL,
  ApiError,
  datetimeInputValue,
  fetchAnalytics,
  fetchSnapshot,
  inputValueToUtc,
  startEventStream,
  validateRange,
  type AnalyticsBucket,
  type AnalyticsFilters,
  type EventFilter,
  type Interval,
  type RecentEvent,
  type SnapshotResponse,
  type StreamEvent,
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

function LivePanel({ token, sessionId }: { token: string; sessionId: string }) {
  const [snapshot, setSnapshot] = useState<SnapshotResponse | null>(null);
  const [streamStatus, setStreamStatus] = useState<'connecting' | 'connected' | 'reconnecting' | 'error'>('connecting');
  const [error, setError] = useState<Error | null>(null);
  const [eventFilter, setEventFilter] = useState<EventFilter>('all');
  const [targetQuery, setTargetQuery] = useState('');
  const refreshing = useRef(false);
  const hasSnapshot = useRef(false);
  const refreshAgain = useRef(false);
  const buffered = useRef<StreamEvent[]>([]);
  const seenIds = useRef(new Set<string>());

  useEffect(() => {
    let active = true;
    const applyEvent = (event: StreamEvent) => {
      if (seenIds.current.has(event.id)) return;
      seenIds.current.add(event.id);
      const maxSeen = 2_000;
      if (seenIds.current.size > maxSeen) seenIds.current.delete(seenIds.current.values().next().value!);
      setSnapshot((current) => {
        if (!current) return current;
        const occurred = Date.parse(event.occurred_at);
        const eventDay = new Date(occurred).toISOString().slice(0, 10);
        if (occurred < Date.parse(current.from) || eventDay !== current.from.slice(0, 10)) return current;
        const click = event.type === 'click' ? 1 : 0;
        const view = event.type === 'view' ? 1 : 0;
        const recent: RecentEvent[] = [{ id: event.id, occurred_at: event.occurred_at, type: event.type, target_id: event.target_id },
          ...current.recent.filter((item) => item.id !== event.id)]
          .sort((left, right) => Date.parse(right.occurred_at) - Date.parse(left.occurred_at)).slice(0, 50);
        return { ...current, totals: {
          clicks: current.totals.clicks + click, views: current.totals.views + view, total: current.totals.total + 1,
        }, recent };
      });
    };

    const refresh = async () => {
      if (!active) return;
      if (refreshing.current) { refreshAgain.current = true; return; }
      refreshing.current = true;
      try {
        const next = await fetchSnapshot(token, fetch, API_BASE_URL);
        if (!active) return;
        const recentIds = new Set(next.recent.map((event) => event.id));
        hasSnapshot.current = true;
        seenIds.current = recentIds;
        const pending = buffered.current;
        buffered.current = [];
        setSnapshot(next);
        for (const event of pending) {
          if (recentIds.has(event.id)) continue;
          applyEvent(event);
        }
        setError(null);
      } catch (cause) {
        if (active) setError(cause instanceof Error ? cause : new Error('Could not load the live snapshot.'));
      } finally {
        refreshing.current = false;
        if (active && refreshAgain.current) {
          refreshAgain.current = false;
          void refresh();
        }
      }
    };

    const stop = startEventStream(token, {
      onOpen: () => {
        if (!active) return;
        setStreamStatus('connected');
        void refresh();
      },
      onConnecting: () => {
        if (active) setStreamStatus((status) => status === 'connected' ? 'reconnecting' : 'connecting');
      },
      onError: (cause) => {
        if (!active) return;
        setError(cause);
        setStreamStatus(cause instanceof ApiError && cause.status === 401 ? 'error' : 'reconnecting');
      },
      onEvent: (event) => {
        if (!active || seenIds.current.has(event.id)) return;
        if (refreshing.current || !hasSnapshot.current) buffered.current.push(event);
        else applyEvent(event);
      },
    }, fetch, API_BASE_URL);
    const poll = setInterval(() => { void refresh(); }, 30_000);
    return () => {
      active = false;
      stop();
      clearInterval(poll);
      buffered.current = [];
    };
  }, [token, sessionId]);

  const statusLabel = streamStatus === 'connected' ? 'Live updates connected'
    : streamStatus === 'reconnecting' ? 'Reconnecting to live updates'
      : streamStatus === 'error' ? 'Live updates unavailable' : 'Connecting to live updates';
  const unauthorized = error instanceof ApiError && error.status === 401;
  const displayTime = (value: string) => new Intl.DateTimeFormat('en-US', {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit',
    timeZone: 'UTC', hourCycle: 'h23',
  }).format(new Date(value));
  const filteredEvents = useMemo(() => (snapshot?.recent ?? []).filter((event) => {
    const matchesType = eventFilter === 'all' || event.type === eventFilter;
    const matchesTarget = event.target_id.toLocaleLowerCase().includes(targetQuery.trim().toLocaleLowerCase());
    return matchesType && matchesTarget;
  }), [snapshot?.recent, eventFilter, targetQuery]);
  const clickShare = snapshot && snapshot.totals.total > 0
    ? Math.round(snapshot.totals.clicks / snapshot.totals.total * 100) : 0;
  const viewShare = snapshot && snapshot.totals.total > 0
    ? Math.round(snapshot.totals.views / snapshot.totals.total * 100) : 0;

  return <>
    {error && <section className="error-card live-error" role="alert"><div className="state-icon">!</div><div>
      <h2>{unauthorized ? 'Access denied' : 'Live data temporarily unavailable'}</h2>
      <p>{unauthorized ? 'That token was not accepted. Disconnect and enter it again.' : error.message}</p>
    </div></section>}
    {!snapshot && !error && <section className="state-card" role="status"><span className="spinner" /> Loading today’s activity…</section>}
    {snapshot && <>
      <div className="live-workbench">
        <section className="signal-console" aria-label="Today's event totals">
          <div className="instrument-cap"><span className={`live-indicator ${streamStatus}`} /><span>COLLECTOR / 01</span><span className="collector-state">{streamStatus === 'connected' ? 'RUNNING' : streamStatus === 'error' ? 'OFFLINE' : 'SYNCING'}</span></div>
          <div className="signal-heading"><span>Today · UTC</span><span className="live-mark">LIVE</span></div>
          <div className="signal-total-label">Committed events</div>
          <div className="signal-total metric-value">{formatNumber(snapshot.totals.total)}</div>
          <div className="signal-caption">Database total · refreshed every 30 sec</div>
          <div className="signal-rule" />
          <div className="signal-mix-title"><span>Event composition</span><span>{clickShare}% clicks</span></div>
          <div className="signal-mix" role="img" aria-label={snapshot.totals.total > 0 ? `Event composition: ${clickShare}% clicks and ${viewShare}% views` : 'No events recorded yet'}>
            <span className="mix-clicks" style={{ width: `${clickShare}%` }} />
            <span className="mix-views" style={{ width: `${100 - clickShare}%` }} />
          </div>
          <div className="signal-split">
            <button type="button" aria-pressed={eventFilter === 'click'} onClick={() => setEventFilter(eventFilter === 'click' ? 'all' : 'click')}>
              <span className="split-label"><i className="legend-dot clicks-dot" /> Clicks</span><strong>{formatNumber(snapshot.totals.clicks)}</strong>
            </button>
            <button type="button" aria-pressed={eventFilter === 'view'} onClick={() => setEventFilter(eventFilter === 'view' ? 'all' : 'view')}>
              <span className="split-label"><i className="legend-dot views-dot" /> Views</span><strong>{formatNumber(snapshot.totals.views)}</strong>
            </button>
          </div>
          <div className="collector-foot"><span className={`live-indicator ${streamStatus}`} /><div><strong>{statusLabel}</strong><p>Events arrive within a few seconds.</p></div></div>
          {snapshot && <time className="snapshot-time" dateTime={snapshot.from}>Window opened {displayTime(snapshot.from)} UTC</time>}
        </section>

        <section className="trace-board" aria-labelledby="activity-title">
          <header className="trace-header"><div><div className="eyebrow">INGESTION TRACE</div><h2 id="activity-title">Recent events</h2><p>Latest committed targets, newest first.</p></div><span className="trace-count">{filteredEvents.length}<small> / {snapshot.recent.length}</small></span></header>
          <div className="trace-tools">
            <label className="target-search"><span className="search-glyph" aria-hidden="true">⌕</span><span className="sr-only">Filter targets</span><input type="search" value={targetQuery} onChange={(event) => setTargetQuery(event.target.value)} placeholder="Find a target…" /></label>
            <div className="trace-filter" role="group" aria-label="Filter recent events by type">
              {(['all', 'click', 'view'] as const).map((filter) => <button key={filter} type="button" aria-pressed={eventFilter === filter} onClick={() => setEventFilter(filter)}>{filter === 'all' ? 'All events' : filter === 'click' ? 'Clicks' : 'Views'}</button>)}
            </div>
          </div>
          {snapshot.recent.length === 0 ? <div className="trace-empty"><span className="empty-icon">⌁</span><h3>No events recorded today</h3><p>Committed events will appear here as they arrive.</p></div>
            : filteredEvents.length === 0 ? <div className="trace-empty compact-empty"><h3>No matching events</h3><p>Try another type or target.</p><button className="text-button" type="button" onClick={() => { setEventFilter('all'); setTargetQuery(''); }}>Clear filters</button></div>
              : <ol className="trace-list">{filteredEvents.map((event, index) => <li className={`trace-event ${event.type}`} key={event.id}>
                <span className="trace-node" aria-hidden="true"><span /></span>
                <span className={`activity-type ${event.type}`}>{event.type === 'click' ? 'Click' : 'View'}</span>
                <span className="trace-target"><span className="target-prefix">target /</span><strong title={event.target_id}>{event.target_id}</strong></span>
                <time dateTime={event.occurred_at}><span>{displayTime(event.occurred_at)}</span><small>UTC</small></time>
                <span className="trace-index">{String(index + 1).padStart(2, '0')}</span>
              </li>)}</ol>}
          <footer className="trace-footer"><span>Snapshot recovery every 30 sec</span><span>Window <time dateTime={snapshot.from}>{displayTime(snapshot.from)}</time> → now · UTC</span></footer>
        </section>
      </div>
    </>}
  </>;
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
  const [mode, setMode] = useState<'live' | 'history'>('live');
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
    enabled: Boolean(token && mode === 'history' && !rangeError),
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
    <main className="console-shell">
      <a className="skip-link" href="#dashboard">Skip to analytics</a>
      <aside className="side-rail">
        <a className="brand" href="#dashboard" aria-label="Northstar Analytics dashboard">
          <span className="brand-mark" aria-hidden="true"><span /></span>
          <span>northstar<span className="brand-light"> / telemetry</span></span>
        </a>
        {token ? <nav className="instrument-nav" aria-label="Analytics views">
          <span className="nav-caption">WORKSPACE</span>
          <button className={mode === 'live' ? 'selected' : ''} type="button" aria-pressed={mode === 'live'} onClick={() => setMode('live')}><span className="nav-icon live-nav-icon" aria-hidden="true">⌁</span><span>Live activity</span></button>
          <button className={mode === 'history' ? 'selected' : ''} type="button" aria-pressed={mode === 'history'} onClick={() => setMode('history')}><span className="nav-icon history-nav-icon" aria-hidden="true">▥</span><span>Historical analytics</span></button>
          <div className="nav-divider" />
          <span className="nav-caption">ACCESS</span>
          <div className="collector-chip"><span className="collector-token-icon" aria-hidden="true">⌘</span><div><strong>Admin token</strong><small>Held in this session</small></div></div>
        </nav> : <div className="rail-note"><span className="rail-note-icon">⌘</span><p>Connect your admin token to inspect the event stream.</p></div>}
        <div className="rail-bottom"><span className="rail-grid-mark" aria-hidden="true">01—24</span><span>UTC REFERENCE</span></div>
      </aside>

      <div className="workspace-area">
        <header className="workspace-topbar">
          <div className="breadcrumb"><span>Northstar</span><i>/</i><strong>{mode === 'live' ? 'Live stream' : 'Event analysis'}</strong></div>
          <div className="topbar-right"><span className="system-status"><span className="status-dot" />{mode === 'live' ? 'STREAM VIEW' : 'QUERY VIEW'}</span><span className="utc-chip">UTC</span>{token && <button className="quiet-button" onClick={disconnect} type="button">Disconnect</button>}</div>
        </header>

        <div className="content" id="dashboard">
          <section className="view-heading">
            <div><div className="eyebrow">EVENT OBSERVATORY <span> / </span> {mode === 'live' ? 'LIVE TRACE' : 'HISTORICAL QUERY'}</div>
              <h1>{mode === 'live' ? 'Live event stream' : 'Event analysis'}</h1>
              <p className="subtitle">{mode === 'live' ? 'Watch committed click and view events arrive at the collector.' : 'Inspect event volume across a UTC window.'}</p></div>
            {token && <div className="window-chip"><span aria-hidden="true">◷</span>{mode === 'live' ? 'TODAY · UTC' : 'CUSTOM WINDOW · UTC'}</div>}
          </section>

          {!token ? (
            <section className="connect-layout">
              <div className="connect-story"><div className="eyebrow">PRIVATE BY DEFAULT</div><h2>Bring the event stream into focus.</h2><p>Connect with an admin API token to inspect committed events and query historical click and view volume.</p><div className="connect-diagram" aria-hidden="true"><span>EMITTER</span><i>················</i><span>COLLECTOR</span></div></div>
              <section className="auth-card" aria-labelledby="auth-title">
                <div className="auth-icon" aria-hidden="true">⌘</div>
                <div className="auth-copy"><h2 id="auth-title">Connect to your analytics</h2><p>Your token stays in memory and is never saved in this browser.</p></div>
                <form className="token-form" onSubmit={connect}>
                  <label htmlFor="api-token">Admin API token</label>
                  <div className="token-input-row"><input id="api-token" type="password" autoComplete="off" value={tokenInput} onChange={(event) => setTokenInput(event.target.value)} placeholder="Paste your API token" /><button className="primary-button" disabled={!tokenInput.trim()} type="submit">Connect <span aria-hidden="true">→</span></button></div>
                </form>
              </section>
            </section>
          ) : mode === 'live' ? <LivePanel token={token} sessionId={sessionId} /> : (
            <div className="history-workbench">
              <aside className="query-inspector" aria-label="Analytics filters">
                <div className="inspector-heading"><span className="inspector-icon" aria-hidden="true">⌕</span><div><span className="nav-caption">QUERY BUILDER</span><h2>Refine window</h2></div></div>
                <div className="range-presets" aria-label="Quick date ranges"><button type="button" onClick={() => applyPreset(24)}>24 hours</button><button type="button" onClick={() => applyPreset(24 * 7)}>7 days</button></div>
                <div className="field range-field"><label htmlFor="from-time">From <span>UTC</span></label><input id="from-time" type="datetime-local" step="60" value={fromInput} onChange={(event) => setFromInput(event.target.value)} /></div>
                <div className="field range-field"><label htmlFor="to-time">To <span>UTC · exclusive</span></label><input id="to-time" type="datetime-local" step="60" value={toInput} onChange={(event) => setToInput(event.target.value)} /></div>
                <div className="field select-field"><label htmlFor="event-type">Event type</label><select id="event-type" value={type} onChange={(event) => setType(event.target.value as EventFilter)}><option value="all">All events</option><option value="click">Clicks</option><option value="view">Views</option></select></div>
                <div className="field select-field"><label htmlFor="interval">Group by</label><select id="interval" value={interval} onChange={(event) => setInterval(event.target.value as Interval)}><option value="minute">Minute</option><option value="hour">Hour</option><option value="day">Day</option></select></div>
                {rangeError && <p className="inline-error" role="alert">{rangeError}</p>}
                <div className="inspector-foot"><span className="inspector-led" /> Query times use UTC<br />End time is exclusive</div>
              </aside>

              <section className="analysis-main" aria-label="Historical event analysis">
                {analytics.isPending && !rangeError && <section className="state-card" role="status"><span className="spinner" /> Loading analytics…</section>}
                {analytics.isError && !rangeError && <section className="error-card" role="alert"><div className="state-icon">!</div><div><h2>{unauthorized ? 'Access denied' : 'Could not load analytics'}</h2><p>{unauthorized ? 'That token was not accepted. Check it and connect again.' : analytics.error?.message || 'Check the API connection and try again.'}</p>{unauthorized && <button className="text-button" onClick={disconnect} type="button">Enter token again</button>}{!unauthorized && <button className="text-button" onClick={() => void analytics.refetch()} type="button">Try again</button>}</div></section>}
                {analytics.isSuccess && !rangeError && <>
                  <section className="volume-readout" aria-label="Event totals">
                    <div className="volume-primary"><span className="nav-caption">TOTAL COMMITTED EVENTS</span><strong className="volume-total">{formatNumber(analytics.data.totals.total)}</strong><span className="volume-context">Selected window · {interval} buckets</span></div>
                    <div className="volume-composition"><div className="composition-bar" role="img" aria-label={`Event composition: ${analytics.data.totals.total > 0 ? Math.round(analytics.data.totals.clicks / analytics.data.totals.total * 100) : 0}% clicks and ${analytics.data.totals.total > 0 ? Math.round(analytics.data.totals.views / analytics.data.totals.total * 100) : 0}% views`}><span className="mix-clicks" style={{ width: `${analytics.data.totals.total > 0 ? analytics.data.totals.clicks / analytics.data.totals.total * 100 : 0}%` }} /><span className="mix-views" style={{ width: `${analytics.data.totals.total > 0 ? analytics.data.totals.views / analytics.data.totals.total * 100 : 0}%` }} /></div>
                      <div className="composition-values"><button type="button" aria-pressed={type === 'click'} onClick={() => setType(type === 'click' ? 'all' : 'click')}><span className="composition-label"><i className="legend-dot clicks-dot" /> Clicks</span><strong>{formatNumber(analytics.data.totals.clicks)}</strong></button><button type="button" aria-pressed={type === 'view'} onClick={() => setType(type === 'view' ? 'all' : 'view')}><span className="composition-label"><i className="legend-dot views-dot" /> Views</span><strong>{formatNumber(analytics.data.totals.views)}</strong></button></div>
                    </div>
                  </section>
                  <section className="chart-card" aria-labelledby="chart-title">
                    <div className="chart-header"><div><div className="eyebrow">DISTRIBUTION / UTC</div><h2 id="chart-title">Event volume</h2><p>Activity by {interval}, aligned to UTC boundaries.</p></div><span className="updated-label">{lastUpdated ? `Updated ${lastUpdated} UTC` : 'Waiting for response'}</span></div>
                    {analytics.data.buckets.length === 0 || analytics.data.totals.total === 0 ? <div className="empty-state"><div className="empty-icon">⌁</div><h3>No activity in this range</h3><p>Try a wider date range or select a different event type.</p></div> : <><div className="chart-legend"><span><i className="legend-dot clicks-dot" /> Clicks</span><span><i className="legend-dot views-dot" /> Views</span></div><div className="chart-scroll" role="region" aria-label="Scrollable event volume chart" tabIndex={0}><SeriesChart buckets={chartBuckets} interval={interval} /></div></>}
                    <div className="chart-footnote">Buckets are UTC-aligned; the selected end time is exclusive.</div>
                  </section>
                </>}
              </section>
            </div>
          )}
          <footer className="page-footer"><span>Northstar · Event Observatory</span><span>{mode === 'live' ? 'Committed events · today in UTC' : 'Historical query · UTC'}</span></footer>
        </div>
      </div>
    </main>
  );
}
