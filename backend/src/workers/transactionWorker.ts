
import { randomUUID } from "node:crypto";
import { db } from "../database/db.js";
import { redisClient } from "../queue/redis.js";

const STREAM = "transactions:stream";
const GROUP = "transaction-workers";
const CONSUMER = `worker-${process.pid}`;
const MOCK_URL =
  process.env.MOCK_SERVICE_URL || "http://localhost:8080";

const RETRY_SET = "transactions:delayed-retries";
const MAX_ATTEMPTS = 8;
const CLAIM_IDLE_MS = 60_000;
const BATCH_SIZE = 10;
const FAILED_RECOVERY_INTERVAL_MS = 15_000;
const FAILED_RECOVERY_BATCH_SIZE = 50;
const STALE_PROCESSING_THRESHOLD_MS = 60_000;
const STALE_RECOVERY_KEY = "transactions:stale-recovery";

const LOCK_PREFIX = "transactions:lock:";
const LOCK_TTL_SECONDS = 60;

type Job = {
  transactionId: string;
  attempts: string;
};

type StreamMessage = {
  id: string;
  message: Record<string, string>;
};

type TransactionRow = {
  id: string;
  amount: string;
  currency: string;
  description: string;
  metadata: string | null;
  submitted_at: string;
};

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export async function acquireTransactionLock(
  transactionId: string
): Promise<string | null> {
  const token = randomUUID();

  const result = await redisClient.set(
    `${LOCK_PREFIX}${transactionId}`,
    token,
    {
      NX: true,
      EX: LOCK_TTL_SECONDS
    }
  );

  return result === "OK" ? token : null;
}

export async function releaseTransactionLock(
  transactionId: string,
  token: string
): Promise<void> {
  const script = `
    if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("DEL", KEYS[1])
    end
    return 0
  `;

  await redisClient.eval(script, {
    keys: [`${LOCK_PREFIX}${transactionId}`],
    arguments: [token]
  });
}


async function existsAtPostingService(id: string): Promise<boolean> {
  const response = await fetch(
    `${MOCK_URL}/transactions/${encodeURIComponent(id)}`,
    { signal: AbortSignal.timeout(3000) }
  );

  if (response.status === 404) return false;

  if (!response.ok) {
    throw new Error(`Posting service GET returned ${response.status}`);
  }

  return true;
}

async function postTransaction(
  transaction: TransactionRow
): Promise<void> {
  const response = await fetch(`${MOCK_URL}/transactions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      id: transaction.id,
      amount: Number(transaction.amount),
      currency: transaction.currency,
      description: transaction.description,
      timestamp: transaction.submitted_at,
      ...(transaction.metadata
        ? { metadata: JSON.parse(transaction.metadata) }
        : {})
    }),
    signal: AbortSignal.timeout(15000)
  });

  if (!response.ok) {
    throw new Error(`Posting service POST returned ${response.status}`);
  }
}

function updateStatus(
  id: string,
  status: "processing" | "completed" | "failed",
  error: string | null = null
): void {
  const now = new Date().toISOString();
  const existing = db.prepare(`
    SELECT processing_started_at
    FROM transactions
    WHERE id = ?
  `).get(id) as { processing_started_at: string | null } | undefined;

  const processingStartedAt =
    status === "processing"
      ? existing?.processing_started_at ?? now
      : null;

  db.prepare(`
    UPDATE transactions
    SET status = ?,
        processing_started_at = ?,
        completed_at = ?,
        error = ?
    WHERE id = ?
  `).run(
    status,
    processingStartedAt,
    status === "completed" || status === "failed"
      ? now
      : null,
    error,
    id
  );
}


async function processTransaction(job: Job): Promise<void> {
  const transactionId = job.transactionId;

  // Acquire the lock before checking or changing transaction status.
  const token = await acquireTransactionLock(transactionId);

  if (!token) {
    throw new Error(
      `Transaction is already locked by another worker: ${transactionId}`
    );
  }

  // Renew the lock while this worker is processing the transaction.
let lockLost = false;
let renewalInProgress = false;

const heartbeat = setInterval(() => {
  if (renewalInProgress || lockLost) return;

  renewalInProgress = true;

  const script = `
    if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("EXPIRE", KEYS[1], ARGV[2])
    end
    return 0
  `;

  void redisClient.eval(script, {
    keys: [`${LOCK_PREFIX}${transactionId}`],
    arguments: [token, String(LOCK_TTL_SECONDS)]
  }).then((result) => {
    if (Number(result) !== 1) {
      lockLost = true;
      console.error("Transaction lock lost:", transactionId);
    }
  }).catch((error) => {
    lockLost = true;
    console.error(
      "Could not renew transaction lock:",
      transactionId,
      getErrorMessage(error)
    );
  }).finally(() => {
    renewalInProgress = false;
  });
}, 10_000);


  try {
    const transaction = db.prepare(`
      SELECT id, amount, currency, description, metadata, submitted_at
      FROM transactions
      WHERE id = ?
    `).get(transactionId) as TransactionRow | undefined;

    if (!transaction) {
      console.error("Transaction missing from SQLite:", transactionId);
      return;
    }

    const current = db.prepare(`
      SELECT status FROM transactions WHERE id = ?
    `).get(transactionId) as { status: string } | undefined;

    if (!current || current.status === "completed") return;

    updateStatus(transactionId, "processing");
    if (lockLost) {
  throw new Error(`Transaction lock lost: ${transactionId}`);
}
try { // Check whether a previous attempt already posted the transaction.
  if (await existsAtPostingService(transactionId)) {
    updateStatus(transactionId, "completed");
    return;
  }

  // Do not start a POST if this worker has lost its lock.
  if (lockLost) {
    throw new Error(`Transaction lock lost: ${transactionId}`);
  }

  await postTransaction(transaction);

  // Do not assume the lock is still valid after the network request.
  if (lockLost) {
    throw new Error(`Transaction lock lost: ${transactionId}`);
  }

  updateStatus(transactionId, "completed");
} catch (error) {

      // The POST may have succeeded remotely even if its response was lost.
      try {
        if (await existsAtPostingService(transactionId)) {
          updateStatus(transactionId, "completed");
          return;
        }
      } catch (verificationError) {
        console.error(
          "Could not verify posting result:",
          getErrorMessage(verificationError)
        );
      }

      throw error;
    }
  } finally {
    clearInterval(heartbeat);

    // Release only if this worker still owns the lock.
    await releaseTransactionLock(transactionId, token);
  }
}


export async function scheduleRetry(job: Job): Promise<void> {
  const attempts = Number(job.attempts) + 1;

  if (attempts >= MAX_ATTEMPTS) {
    updateStatus(
      job.transactionId,
      "failed",
      `Maximum retry attempts (${MAX_ATTEMPTS}) reached`
    );

    console.error("Transaction permanently failed:", job.transactionId);
    return;
  }

  const retryEntry = JSON.stringify({
    transactionId: job.transactionId,
    attempts: String(attempts)
  });

  const retries = await redisClient.zRange(RETRY_SET, 0, -1);

  for (const existing of retries) {
    try {
      const parsed = JSON.parse(existing) as Job;
      if (parsed.transactionId === job.transactionId) {
        await redisClient.zRem(RETRY_SET, existing);
      }
    } catch {
      // Ignore malformed entries so the retry queue remains usable.
    }
  }

  const backoffMs = Math.min(1000 * 2 ** attempts, 30000);
  const retryAt = Date.now() + backoffMs;

  await redisClient.zAdd(RETRY_SET, {
    score: retryAt,
    value: retryEntry
  });

  console.error(
    `Retry ${attempts} for ${job.transactionId} scheduled in ${backoffMs}ms`
  );
}

async function processDueRetries(): Promise<void> {
  const dueRetries = await redisClient.zRangeByScore(
    RETRY_SET,
    0,
    Date.now(),
    { LIMIT: { offset: 0, count: BATCH_SIZE } }
  );

  for (const retryData of dueRetries) {
    let job: Job;

    try {
      job = JSON.parse(retryData) as Job;

      if (!job.transactionId || job.attempts === undefined) {
        console.error("Invalid delayed retry:", retryData);
        continue;
      }
    } catch (error) {
      console.error("Could not parse retry:", getErrorMessage(error));
      continue;
    }

    // Atomically add the stream entry and remove the delayed retry.
    // This avoids losing the retry between these two operations.
    const script = `
      local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
      if not score or tonumber(score) > tonumber(ARGV[2]) then
        return 0
      end

      redis.call(
        'XADD', KEYS[2], '*',
        'transactionId', ARGV[3],
        'attempts', ARGV[4]
      )

      redis.call('ZREM', KEYS[1], ARGV[1])
      return 1
    `;

    try {
      const result = await redisClient.eval(script, {
        keys: [RETRY_SET, STREAM],
        arguments: [
          retryData,
          String(Date.now()),
          job.transactionId,
          job.attempts
        ]
      });

      if (Number(result) === 1) {
        console.log("Retry requeued:", job.transactionId);
      }
    } catch (error) {
      console.error(
        "Could not process due retry:",
        getErrorMessage(error)
      );
    }
  }
}

/**
 * Requeue old failed transactions at a controlled interval.
 * This lets them recover after the external service becomes available.
 */
export async function recoverFailedTransactions(): Promise<void> {
  const failedTransactions = db.prepare(`
    SELECT id
    FROM transactions
    WHERE status = 'failed'
      AND error = 'Temporary service failure'
    ORDER BY completed_at ASC
    LIMIT ?
  `).all(FAILED_RECOVERY_BATCH_SIZE) as Array<{ id: string }>;

  for (const transaction of failedTransactions) {
    try {
      const queued = await redisClient.zAdd(
        RETRY_SET,
        {
          score: Date.now(),
          value: JSON.stringify({
            transactionId: transaction.id,
            attempts: "0"
          })
        },
        { NX: true }
      );

      if (queued === 1) {
        console.log("Failed transaction queued for recovery:", transaction.id);
      }
    } catch (error) {
      console.error(
        "Could not queue failed transaction:",
        transaction.id,
        getErrorMessage(error)
      );
    }
  }
}

export async function recoverStaleProcessingTransactions(): Promise<void> {
  const staleTransactions = db.prepare(`
    SELECT id, processing_started_at
    FROM transactions
    WHERE status = 'processing'
      AND (
        processing_started_at IS NULL
        OR strftime('%s', processing_started_at) * 1000 <= ?
      )
    ORDER BY COALESCE(processing_started_at, submitted_at) ASC
    LIMIT ?
  `).all(
    Date.now() - STALE_PROCESSING_THRESHOLD_MS,
    FAILED_RECOVERY_BATCH_SIZE
  ) as Array<{
    id: string;
    processing_started_at: string | null;
  }>;

  for (const transaction of staleTransactions) {
    const lockKey = `${LOCK_PREFIX}${transaction.id}`;

    try {
      const lockExists = await redisClient.exists(lockKey);
      if (lockExists === 1) {
        continue;
      }

      const recoveryLock = await redisClient.set(
        `${STALE_RECOVERY_KEY}:${transaction.id}`,
        "1",
        {
          NX: true,
          EX: 300
        }
      );

      if (recoveryLock !== "OK") {
        continue;
      }

      const remoteExists = await existsAtPostingService(transaction.id);
      if (remoteExists) {
        updateStatus(transaction.id, "completed");
        console.log(
          "Recovered stale processing transaction after remote success:",
          transaction.id
        );
        continue;
      }

      await scheduleRetry({
        transactionId: transaction.id,
        attempts: "0"
      });

      console.warn(
        "Recovered stale processing transaction by scheduling retry:",
        transaction.id
      );
    } catch (error) {
      console.error(
        "Could not recover stale processing transaction:",
        transaction.id,
        getErrorMessage(error)
      );
    }
  }
}

async function handleMessage(message: StreamMessage): Promise<void> {
  const job: Job = {
    transactionId: String(message.message.transactionId ?? ""),
    attempts: String(message.message.attempts ?? "0")
  };

  if (!job.transactionId) {
    console.error("Discarding message without transactionId:", message.id);
    await redisClient.xAck(STREAM, GROUP, message.id);
    return;
  }

  try {
    await processTransaction(job);
    await redisClient.xAck(STREAM, GROUP, message.id);
  } catch (error) {
    console.error(
      `Processing failed for ${job.transactionId}:`,
      getErrorMessage(error)
    );

    try {
      await scheduleRetry(job);
      await redisClient.xAck(STREAM, GROUP, message.id);
    } catch (retryError) {
      // Leave the original message pending if retry scheduling fails.
      console.error(
        "Retry scheduling failed:",
        getErrorMessage(retryError)
      );
    }
  }
}

export async function startTransactionWorker(): Promise<void> {
  try {
    await redisClient.xGroupCreate(STREAM, GROUP, "0", {
      MKSTREAM: true
    });
  } catch (error) {
    if (
      !(error instanceof Error) ||
      !error.message.includes("BUSYGROUP")
    ) {
      throw error;
    }
  }

  console.log(`Transaction worker started: ${CONSUMER}`);

  let lastFailedRecovery = 0;

  while (true) {
    try {
      await processDueRetries();

      // Periodically retry transactions previously marked failed.
      if (
        Date.now() - lastFailedRecovery >=
        FAILED_RECOVERY_INTERVAL_MS
      ) {
        lastFailedRecovery = Date.now();
        await recoverFailedTransactions();
      }

      await recoverStaleProcessingTransactions();

      // Recover messages left pending by a crashed worker.
      const claimed = await redisClient.xAutoClaim(
        STREAM,
        GROUP,
        CONSUMER,
        CLAIM_IDLE_MS,
        "0-0",
        { COUNT: BATCH_SIZE }
      );

      for (const message of claimed.messages as StreamMessage[]) {
        await handleMessage(message);
      }

      const batches = await redisClient.xReadGroup(
        GROUP,
        CONSUMER,
        [{ key: STREAM, id: ">" }],
        { COUNT: BATCH_SIZE, BLOCK: 1000 }
      );

      if (!batches) continue;

      for (const batch of batches) {
        for (const message of batch.messages) {
          await handleMessage(message as StreamMessage);
        }
      }
    } catch (error) {
      console.error("Worker loop error:", getErrorMessage(error));
      await delay(1000);
    }
  }
}
