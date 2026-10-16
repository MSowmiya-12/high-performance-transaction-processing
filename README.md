# High-Performance Transaction Processing System

A TypeScript-based transaction processing API built with Express.js, SQLite, and Redis Streams. It accepts transactions asynchronously, tracks their processing status, supports client idempotency, and retries failed posting operations.

## 1. Overview

The system acts as a reliable intermediary between clients and a slow, unreliable mock posting service.

### Key features

- **Asynchronous processing:** Accepts transaction requests without waiting for the external posting service to finish.
- **Idempotency:** Uses an `Idempotency-Key` header to prevent duplicate transaction creation for repeated client requests.
- **Persistent transaction storage:** Uses SQLite to track transaction status and details.
- **Reliable queue publishing:** Uses a transactional outbox to coordinate database writes and queue publication.
- **Retry mechanism:** Schedules failed operations using Redis sorted sets and exponential backoff.
- **Worker coordination:** Uses Redis locks to prevent concurrent processing of the same transaction.
- **Recovery and monitoring:** Provides stale-transaction recovery and a health endpoint with queue and transaction metrics.

## 2. Technology Stack

| Component | Technology |
|---|---|
| Language | TypeScript |
| Runtime | Node.js |
| API framework | Express.js |
| Database | SQLite with better-sqlite3 |
| Queue and coordination | Redis Streams |
| Request validation | Zod |
| Testing | Vitest and Supertest |
| External service | Docker-based mock posting service |
| Load testing | autocannon |

## 3. Architecture

```text
Client
  |
  v
Express REST API
  |
  v
SQLite Transaction + Outbox
  |
  v
Outbox Publisher
  |
  v
Redis Stream
  |
  v
Transaction Worker
  |
  +--> Redis Lock
  |
  +--> Check posting service for existing transaction
  |
  +--> POST transaction when required
  |
  +--> Update SQLite status
  |
  +--> Schedule retries on failure
```

### Transaction flow

1. The client submits a transaction to `POST /api/transactions`.
2. The API validates the request and checks the idempotency key.
3. The transaction and its outbox entry are stored in SQLite.
4. The API returns an accepted response.
5. The outbox publisher publishes the transaction to Redis Streams.
6. A worker processes the transaction and checks whether it already exists in the posting service.
7. The worker posts the transaction when required and updates its status.
8. Failures are verified and retried using a delayed retry queue.

### Design decisions

**Transactional outbox:** The transaction record and its outbox entry are written within the same SQLite transaction. This reduces the risk of losing a queued operation when the application fails between database persistence and queue publication.

**Client idempotency:** A unique database index protects idempotency keys. The service also compares request hashes so that reusing a key with different transaction data returns a conflict.

**Redis Streams:** Consumer groups and message acknowledgements support asynchronous processing and pending-message recovery.

**Distributed locks:** Redis locks with ownership tokens help prevent multiple workers from processing the same transaction concurrently.

**Delayed retries:** Redis sorted sets schedule retries using exponential backoff. Repeated failures eventually move transactions to a failed state.

## 4. Prerequisites

Install the following before running the project:

- Node.js and npm
- Docker Desktop
- A running Redis instance
- The mock posting service

## 5. Setup and Installation

Clone the repository:

```bash
git clone https://github.com/MSowmiya-12/high-performance-transaction-processing.git
cd high-performance-transaction-processing
```

Install dependencies:

```bash
npm install
```

Create a `.env` file in the project root:

```env
PORT=3000
REDIS_URL=redis://localhost:6379
MOCK_SERVICE_URL=http://localhost:8080
```

Start Redis:

```bash
docker run -d --name transaction-redis -p 6379:6379 redis:7-alpine
```

Start the mock posting service:

```bash
docker run -d --name mock-posting-service -p 8080:8080 vinhopenfabric/mock-posting-service:latest
```

If the containers already exist, start them with `docker start transaction-redis mock-posting-service` rather than creating duplicate containers.

Build the project:

```bash
npm run build
```

Start the development server:

```bash
npm run dev
```

The API runs at `http://localhost:3000`.

## 6. API Endpoints

### Health check

`GET /api/health`

Example:

```bash
curl http://localhost:3000/api/health
```

Returns service status, Redis connectivity, stream and retry metrics, and transaction status counts.

### Submit a transaction

`POST /api/transactions`

Example:

```bash
curl -i -X POST http://localhost:3000/api/transactions \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: payment-001" \
  -d "{\"amount\":100.50,\"currency\":\"INR\",\"description\":\"Test payment\"}"
```

A newly accepted transaction returns HTTP `202 Accepted`.

### Retrieve a transaction

`GET /api/transactions/{id}`

Example:

```bash
curl http://localhost:3000/api/transactions/YOUR_TRANSACTION_ID
```

The response includes the transaction ID, amount, currency, description, status, submission time, completion time, and any recorded error.

### Idempotency behavior

Submitting the same request with the same idempotency key returns the existing transaction rather than creating another.

Reusing the key with different transaction data returns HTTP `409 Conflict`.

## 7. Transaction Statuses

| Status | Meaning |
|---|---|
| `pending` | Accepted and awaiting processing |
| `processing` | Being processed by a worker |
| `completed` | Posting service operation verified as successful |
| `failed` | Processing exhausted its retry policy or reached a terminal failure |

## 8. Testing

Run the automated test suite:

```bash
npm test
```

Build the project:

```bash
npm run build
```

Check production dependency advisories:

```bash
npm audit --omit=dev
```

### Verified results

The following checks have been completed during development:

- TypeScript build completed successfully.
- All 12 automated tests passed in the isolated test copy.
- API health checks succeeded for the transaction API and mock posting service.
- A transaction was accepted and subsequently reached `completed`.
- Repeating a request with the same idempotency key returned the same transaction ID.
- Reusing the key with different data returned HTTP `409 Conflict`.
- Transaction retrieval returned the expected completed record.
- `npm audit --omit=dev` reported zero vulnerabilities in production dependencies at the time of testing.

These results demonstrate functional correctness for the tested scenarios; they do not establish a guarantee of zero transaction loss or zero duplicates under every failure condition.

## 9. Performance Evaluation

The assignment specifies a transaction submission response time below 100 ms and throughput of 1,000+ transactions per second.

The asynchronous API design aims to return promptly without waiting for the mock posting service, whose operations can take several seconds.

Performance should be evaluated separately at two levels:

1. **API acceptance throughput:** How quickly the service validates and persists incoming requests.
2. **End-to-end processing throughput:** How quickly the workers complete transactions at the external posting service.

A high API acceptance rate does not necessarily mean that the workers can complete transactions at the same rate.

Before claiming the assignment's performance targets have been met, run a reproducible load test and record request rate, average latency, p95/p99 latency, error rate, queue growth, and completed transaction throughput.

## 10. Reliability Considerations and Limitations

- Redis persistence and recovery settings affect queue durability.
- The outbox publisher and worker must tolerate retries and duplicate delivery.
- A crash or ambiguous response during external posting can create duplicate-posting risk because the mock service does not support idempotency keys.
- Redis locks reduce concurrent processing but do not alone guarantee exactly-once external side effects.
- Worker concurrency and external posting latency constrain end-to-end throughput.
- Retry limits, stale-processing recovery, and monitoring should be tested under simulated service failures.
- Production deployment would benefit from graceful shutdown, stronger operational metrics, controlled worker concurrency, and additional failure-injection tests.

The system is designed to improve reliability through persistence, retries, deduplication checks, and recovery mechanisms, but exactly-once external processing cannot be guaranteed solely by the current client-side design.

## 11. Future Improvements

- Benchmark and tune worker concurrency.
- Add a circuit breaker for sustained posting-service failures.
- Add rate limiting and queue-capacity protection.
- Improve metrics and alerting.
- Expand integration and failure-injection tests.
- Improve graceful shutdown and recovery testing.
- Evaluate stronger external idempotency guarantees where the posting service permits them.

## 12. Repository

GitHub: https://github.com/MSowmiya-12/high-performance-transaction-processing

## 13. Demo Checklist

A short demonstration can show:

1. The API and Docker services starting.
2. The health endpoint reporting healthy status.
3. A transaction being accepted.
4. The transaction moving to `completed`.
5. Repeating the same idempotency key returning the same transaction.
6. Reusing the key with different data returning `409 Conflict`.
7. The automated test suite passing.