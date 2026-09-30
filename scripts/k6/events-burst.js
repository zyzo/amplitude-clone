import http from 'k6/http';
import { check } from 'k6';

const rate = Number(__ENV.RATE || '25');
const duration = __ENV.DURATION || '30s';
const apiUrl = __ENV.API_URL || 'http://api:3000';
const workerId = __ENV.WORKER_ID || '1';
const preAllocatedVUs = Number(__ENV.PREALLOCATED_VUS || Math.max(10, Math.ceil(rate * 0.1)));
const maxVUs = Number(__ENV.MAX_VUS || preAllocatedVUs * 2);

export const options = {
  scenarios: {
    burst: {
      executor: 'constant-arrival-rate',
      rate,
      timeUnit: '1s',
      duration,
      preAllocatedVUs,
      maxVUs,
    },
  },
  thresholds: {
    checks: ['rate>0.99'],
    http_req_failed: ['rate<0.01'],
    dropped_iterations: ['count==0'],
  },
};

function uuidV4() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (character) => {
    const random = Math.floor(Math.random() * 16);
    return (character === 'x' ? random : (random & 0x3) | 0x8).toString(16);
  });
}

export default function () {
  const event = {
    id: uuidV4(),
    occurred_at: new Date().toISOString(),
    type: Math.random() < 0.5 ? 'click' : 'view',
    target_id: `k6-burst-worker-${workerId}`,
  };
  const response = http.post(
    `${apiUrl}/events`,
    JSON.stringify({ events: [event] }),
    { headers: { 'Content-Type': 'application/json' }, tags: { endpoint: 'ingest' } },
  );
  check(response, { 'ingestion returned 202': (result) => result.status === 202 });
}
