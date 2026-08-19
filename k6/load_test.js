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

const DEBIT_ACCOUNT_ID = 'df2fd8af-836f-4a11-a311-0f7d784f3431';
const CREDIT_ACCOUNT_ID = 'b1ed23fa-78d3-4db9-827b-de4d664a23d3';

export default function () {
  const url = 'http://localhost:8000/transactions';
  const payload = JSON.stringify({
    description: 'k6 load test transaction',
    entries: [
      {
        account_id: DEBIT_ACCOUNT_ID,
        entry_type: 'DEBIT',
        amount: '10.0000',
      },
      {
        account_id: CREDIT_ACCOUNT_ID,
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
