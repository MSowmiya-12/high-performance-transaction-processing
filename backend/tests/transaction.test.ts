import {
  scheduleRetry,
  recoverFailedTransactions,
  recoverStaleProcessingTransactions,
  acquireTransactionLock,
  releaseTransactionLock
} from "../src/workers/transactionWorker.js";
import { randomUUID } from "node:crypto";
import {
  beforeAll,
  beforeEach,
  afterAll,
  afterEach,
  describe,
  expect,
  it,
  vi
} from "vitest";
import request from "supertest";
import { app } from "../src/app.js";
import {
  initializeDatabase,
  db
} from "../src/database/db.js";
import {
  connectRedis,
  redisClient
} from "../src/queue/redis.js";

async function clearTestState(): Promise<void> {
  db.prepare("DELETE FROM outbox").run();
  db.prepare("DELETE FROM transactions").run();

  const retryKeys = await redisClient.keys("transactions:delayed-retries");
  if (retryKeys.length > 0) {
    await redisClient.del(retryKeys);
  }

  const lockKeys = await redisClient.keys("transactions:lock:*");
  if (lockKeys.length > 0) {
    await redisClient.del(lockKeys);
  }

  const recoveryKeys = await redisClient.keys("transactions:stale-recovery:*");
  if (recoveryKeys.length > 0) {
    await redisClient.del(recoveryKeys);
  }
}

beforeAll(async () => {
  initializeDatabase();
  await connectRedis();
  await clearTestState();
});

beforeEach(async () => {
  await clearTestState();
});

afterEach(async () => {
  await clearTestState();
});

afterAll(async () => {
  await clearTestState();
  db.close();

  if (redisClient.isOpen) {
    await redisClient.quit();
  }
});

describe("Transaction API", () => {
  it("returns healthy status", async () => {
    const response = await request(app).get("/api/health");

    expect(response.status).toBe(200);
    expect(response.body.status).toBe("ok");
    expect(response.body.redis).toBe("connected");
  });

  it("rejects an invalid transaction", async () => {
    const response = await request(app)
      .post("/api/transactions")
      .send({
        amount: -100,
        currency: "INR",
        description: "Invalid payment"
      });

    expect(response.status).toBe(400);
  });

  it("returns the same transaction for repeated idempotency keys", async () => {
    const key = `vitest-idempotency-${randomUUID()}`;

    const payload = {
      amount: 125,
      currency: "INR",
      description: "Automated idempotency test"
    };

    const first = await request(app)
      .post("/api/transactions")
      .set("Idempotency-Key", key)
      .send(payload);

    const second = await request(app)
      .post("/api/transactions")
      .set("Idempotency-Key", key)
      .send(payload);

    expect(first.status).toBe(202);
    expect(second.status).toBe(200);
    expect(second.body.transactionId).toBe(
      first.body.transactionId
    );
  });

  it("rejects reuse of a key with different data", async () => {
    const key = `vitest-conflict-${randomUUID()}`;

    const first = await request(app)
      .post("/api/transactions")
      .set("Idempotency-Key", key)
      .send({
        amount: 100,
        currency: "INR",
        description: "Original payment"
      });

    const second = await request(app)
      .post("/api/transactions")
      .set("Idempotency-Key", key)
      .send({
        amount: 200,
        currency: "INR",
        description: "Original payment"
      });

    expect(first.status).toBe(202);
    expect(second.status).toBe(409);
    expect(second.body.error).toContain(
      "different transaction data"
    );
  });

  it("retrieves an existing transaction by ID", async () => {
    const created = await request(app)
      .post("/api/transactions")
      .set("Idempotency-Key", `vitest-get-${randomUUID()}`)
      .send({
        amount: 175,
        currency: "INR",
        description: "Transaction retrieval test"
      });

    expect(created.status).toBe(202);

    const transactionId = created.body.transactionId;

    const response = await request(app)
      .get(`/api/transactions/${transactionId}`);

    expect(response.status).toBe(200);
    expect(response.body.transactionId).toBe(transactionId);
    expect(response.body.amount).toBe("175");
    expect(response.body.currency).toBe("INR");
  });

  it("returns 404 for a transaction that does not exist", async () => {
    const response = await request(app)
      .get(`/api/transactions/${randomUUID()}`);

    expect(response.status).toBe(404);
    expect(response.body.error).toBe("Transaction not found");
  });
  
it("rejects a malformed currency code", async () => {
  const response = await request(app)
    .post("/api/transactions")
    .send({
      amount: 100,
      currency: "INDIA",
      description: "Invalid currency test"
    });

  expect(response.status).toBe(400);
  expect(response.body.error).toBe("Invalid transaction data");
});

it("stores a scheduled retry in Redis", async () => {
  const transactionId = randomUUID();

  await redisClient.zRem(
    "transactions:delayed-retries",
    JSON.stringify({
      transactionId,
      attempts: "1"
    })
  );

  await scheduleRetry({
    transactionId,
    attempts: "0"
  });

  const retries = await redisClient.zRange(
    "transactions:delayed-retries",
    0,
    -1
  );

  expect(
    retries.some((entry) => {
      const job = JSON.parse(entry);
      return job.transactionId === transactionId;
    })
  ).toBe(true);

  await redisClient.zRem(
    "transactions:delayed-retries",
    retries.filter((entry) => {
      try {
        return JSON.parse(entry).transactionId === transactionId;
      } catch {
        return false;
      }
    })
  );
});

it("queues failed transactions for recovery", async () => {
  const transactionId = randomUUID();

  db.prepare(`
    INSERT INTO transactions (
      id, amount, currency, description, status, submitted_at,
      completed_at, error
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    transactionId,
    "250",
    "INR",
    "Recovery test",
    "failed",
    new Date().toISOString(),
    new Date().toISOString(),
    "Temporary service failure"
  );

  await recoverFailedTransactions();

  const retries = await redisClient.zRange(
    "transactions:delayed-retries",
    0,
    -1
  );

  expect(
    retries.some((entry) => {
      const job = JSON.parse(entry);
      return job.transactionId === transactionId;
    })
  ).toBe(true);

  await redisClient.zRem(
    "transactions:delayed-retries",
    retries.filter((entry) => {
      try {
        return JSON.parse(entry).transactionId === transactionId;
      } catch {
        return false;
      }
    })
  );

  db.prepare("DELETE FROM transactions WHERE id = ?").run(transactionId);
});

it("recovers stale processing transactions only when the redis lock is absent", async () => {
  const transactionId = randomUUID();

  db.prepare(`
    INSERT INTO transactions (
      id, amount, currency, description, status,
      submitted_at, processing_started_at, error
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    transactionId,
    "500",
    "INR",
    "Stale recovery test",
    "processing",
    new Date(Date.now() - 120_000).toISOString(),
    new Date(Date.now() - 120_000).toISOString(),
    null
  );

  const lockKey = `transactions:lock:${transactionId}`;
  const lockExistsBefore = await redisClient.exists(lockKey);
  expect(lockExistsBefore).toBe(0);

  const fetchSpy = vi.spyOn(globalThis, "fetch");
  fetchSpy.mockResolvedValueOnce(new Response(null, { status: 404 }));

  await recoverStaleProcessingTransactions();

  const retries = await redisClient.zRange(
    "transactions:delayed-retries",
    0,
    -1
  );

  expect(
    retries.some((entry) => {
      try {
        return JSON.parse(entry).transactionId === transactionId;
      } catch {
        return false;
      }
    })
  ).toBe(true);

  fetchSpy.mockRestore();

  await redisClient.zRem(
    "transactions:delayed-retries",
    retries.filter((entry) => {
      try {
        return JSON.parse(entry).transactionId === transactionId;
      } catch {
        return false;
      }
    })
  );

  db.prepare("DELETE FROM transactions WHERE id = ?").run(transactionId);
});

it("allows only one worker to acquire the same transaction lock", async () => {
  const transactionId = randomUUID();

  const firstLock = await acquireTransactionLock(transactionId);
  const secondLock = await acquireTransactionLock(transactionId);

  expect(firstLock).not.toBeNull();
  expect(secondLock).toBeNull();

  if (firstLock) {
    await releaseTransactionLock(transactionId, firstLock);
  }
});

it("does not release a lock owned by another worker", async () => {
  const transactionId = randomUUID();

  const firstLock = await acquireTransactionLock(transactionId);
  expect(firstLock).not.toBeNull();

  if (!firstLock) return;

  // A wrong token must not release the current owner's lock.
  await releaseTransactionLock(transactionId, "incorrect-token");

  const secondLock = await acquireTransactionLock(transactionId);
  expect(secondLock).toBeNull();

  await releaseTransactionLock(transactionId, firstLock);

  // After the owner releases it, another worker can acquire it.
  const thirdLock = await acquireTransactionLock(transactionId);
  expect(thirdLock).not.toBeNull();

  if (thirdLock) {
    await releaseTransactionLock(transactionId, thirdLock);
  }
});

});
