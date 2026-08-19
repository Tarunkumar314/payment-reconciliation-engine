import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '30s', target: 50 }, // ramp up to 50 VUs over 30 seconds
    { duration: '1m', target: 50 },  // hold at 50 VUs for 1 minute
    { duration: '30s', target: 0 },  // ramp down to 0 VUs over 30 seconds
  ],
  thresholds: {
    http_req_duration: ['p(95)<2000'], // p95 response time under 2000ms
    http_req_failed: ['rate<0.05'],     // error rate under 5%
  },
};

export function setup() {
  const headers = { 'Content-Type': 'application/json' };
  
  const debitRes = http.post('http://localhost:8000/accounts', JSON.stringify({
    name: 'k6 Load Test Asset Account',
    account_type: 'ASSET',
    currency: 'USD',
  }), { headers });

  const creditRes = http.post('http://localhost:8000/accounts', JSON.stringify({
    name: 'k6 Load Test Revenue Account',
    account_type: 'REVENUE',
    currency: 'USD',
  }), { headers });

  return {
    debitAccountId: JSON.parse(debitRes.body).id,
    creditAccountId: JSON.parse(creditRes.body).id,
  };
}

export default function (data) {
  const url = 'http://localhost:8000/transactions';
  const payload = JSON.stringify({
    description: 'k6 load test transaction',
    entries: [
      {
        account_id: data.debitAccountId,
        entry_type: 'DEBIT',
        amount: '10.0000',
      },
      {
        account_id: data.creditAccountId,
        entry_type: 'CREDIT',
        amount: '10.0000',
      },
    ],
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(url, payload, params);

  check(res, {
    'status is 201': (r) => r.status === 201,
  });

  sleep(0.1);
}
