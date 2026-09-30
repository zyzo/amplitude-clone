#!/usr/bin/env node
import { randomUUID } from 'node:crypto';

const apiBaseUrl = (process.env.API_BASE_URL ?? 'http://127.0.0.1:3002').replace(/\/+$/, '');
const intervalMs = Number(process.env.EVENT_INTERVAL_MS ?? '2000');
const eventCount = Number(process.env.EVENT_COUNT ?? '0');

if (!Number.isFinite(intervalMs) || intervalMs < 100) {
  throw new Error('EVENT_INTERVAL_MS must be at least 100 milliseconds.');
}
if (!Number.isInteger(eventCount) || eventCount < 0) {
  throw new Error('EVENT_COUNT must be zero (unlimited) or a positive integer.');
}
const parsedApiUrl = new URL(apiBaseUrl);
if (!['http:', 'https:'].includes(parsedApiUrl.protocol)) {
  throw new Error('API_BASE_URL must use HTTP or HTTPS.');
}

let stopRequested = false;
let activeController;
let wakeWait;

function stop() {
  stopRequested = true;
  activeController?.abort();
  wakeWait?.();
}

process.once('SIGINT', stop);
process.once('SIGTERM', stop);

function wait(milliseconds) {
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      wakeWait = undefined;
      resolve();
    }, milliseconds);
    wakeWait = () => {
      clearTimeout(timer);
      wakeWait = undefined;
      resolve();
    };
  });
}

async function deliver(event) {
  let attempt = 0;
  while (!stopRequested) {
    activeController = new AbortController();
    try {
      const response = await fetch(`${apiBaseUrl}/events`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({ events: [event] }),
        signal: activeController.signal,
      });
      if (response.ok) {
        const result = await response.json();
        console.log(`${event.type} ${event.target_id}: inserted=${result.inserted}, duplicates=${result.duplicates}`);
        return true;
      }

      const message = await response.text();
      if (response.status !== 429 && response.status < 500) {
        throw new Error(`Ingestion rejected the event (${response.status}): ${message}`);
      }
      console.warn(`Ingestion returned ${response.status}; retrying the same event.`);
    } catch (error) {
      if (stopRequested) return false;
      if (error instanceof Error && error.message.startsWith('Ingestion rejected')) throw error;
      console.warn(`Could not reach the API (${error instanceof Error ? error.message : String(error)}); retrying the same event.`);
    } finally {
      activeController = undefined;
    }

    const delay = Math.min(1_000 * (2 ** attempt), 30_000);
    attempt = Math.min(attempt + 1, 5);
    await wait(Math.floor(delay * (0.75 + Math.random() * 0.5)));
  }
  return false;
}

console.log(`Sending test events to ${apiBaseUrl}/events every ${intervalMs} ms. Press Ctrl+C to stop.`);
let sent = 0;
while (!stopRequested && (eventCount === 0 || sent < eventCount)) {
  const type = sent % 2 === 0 ? 'click' : 'view';
  const event = {
    id: randomUUID(),
    occurred_at: new Date().toISOString(),
    type,
    target_id: type === 'click' ? 'demo-button' : 'demo-page',
  };
  if (!await deliver(event)) break;
  sent += 1;
  if (eventCount === 0 || sent < eventCount) await wait(intervalMs);
}
console.log(`Stopped after sending ${sent} test event${sent === 1 ? '' : 's'}.`);
