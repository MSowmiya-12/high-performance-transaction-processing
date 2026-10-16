
import { randomUUID, createHash } from "node:crypto";
import { db } from "../database/db.js";

type CreateTransactionInput = {
  amount: number;
  currency: string;
  description: string;
  metadata?: Record<string, unknown>;
};

type TransactionRow = {
  id: string;
  amount: string;
  currency: string;
  description: string;
  metadata: string | null;
  status: string;
  submitted_at: string;
  completed_at: string | null;
  error: string | null;
};

export class IdempotencyConflictError extends Error {
  constructor() {
    super("This Idempotency-Key was already used with different transaction data.");
    this.name = "IdempotencyConflictError";
  }
}

function calculateRequestHash(input: CreateTransactionInput): string {
  const normalizedInput = {
    amount: input.amount,
    currency: input.currency.toUpperCase(),
    description: input.description.trim(),
    metadata: input.metadata ?? null
  };

  return createHash("sha256")
    .update(JSON.stringify(normalizedInput))
    .digest("hex");
}

const findByIdempotencyKey = db.prepare(`
  SELECT * FROM transactions WHERE idempotency_key = ?
`);

const insertTransaction = db.prepare(`
  INSERT INTO transactions (
    id, amount, currency, description, metadata,
    status, submitted_at, idempotency_key, request_hash
  )
  VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)
`);

const insertOutbox = db.prepare(`
  INSERT INTO outbox (transaction_id, created_at)
  VALUES (?, ?)
`);

const createTransactionAtomically = db.transaction(
  (
    id: string,
    input: CreateTransactionInput,
    submittedAt: string,
    idempotencyKey: string | undefined,
    requestHash: string
  ) => {
    if (idempotencyKey) {
      const existing = findByIdempotencyKey.get(
        idempotencyKey
      ) as (TransactionRow & {
        idempotency_key: string;
        request_hash: string;
      }) | undefined;

      if (existing) {
        if (existing.request_hash !== requestHash) {
          throw new IdempotencyConflictError();
        }

        return {
          transaction: {
            transactionId: existing.id,
            status: existing.status,
            submittedAt: existing.submitted_at,
            completedAt: existing.completed_at,
            error: existing.error
          },
          duplicate: true
        };
      }
    }

    insertTransaction.run(
      id,
      String(input.amount),
      input.currency.toUpperCase(),
      input.description.trim(),
      input.metadata ? JSON.stringify(input.metadata) : null,
      submittedAt,
      idempotencyKey ?? null,
      requestHash
    );

    insertOutbox.run(id, submittedAt);

    return {
      transaction: {
        transactionId: id,
        status: "pending",
        submittedAt,
        completedAt: null,
        error: null
      },
      duplicate: false
    };
  }
);

export function submitTransaction(
  input: CreateTransactionInput,
  idempotencyKey?: string
) {
  const id = randomUUID();
  const submittedAt = new Date().toISOString();
  const requestHash = calculateRequestHash(input);

  try {
    return createTransactionAtomically(
      id,
      input,
      submittedAt,
      idempotencyKey,
      requestHash
    );
  } catch (error) {
    // Handle a simultaneous request that inserted the same key first.
    if (
      idempotencyKey &&
      error instanceof Error &&
      error.message.includes("UNIQUE constraint failed")
    ) {
      const existing = findByIdempotencyKey.get(
        idempotencyKey
      ) as (TransactionRow & {
        idempotency_key: string;
        request_hash: string;
      }) | undefined;

      if (existing) {
        if (existing.request_hash !== requestHash) {
          throw new IdempotencyConflictError();
        }

        return {
          transaction: {
            transactionId: existing.id,
            status: existing.status,
            submittedAt: existing.submitted_at,
            completedAt: existing.completed_at,
            error: existing.error
          },
          duplicate: true
        };
      }
    }

    throw error;
  }
}

export function getTransaction(id: string) {
  const row = db.prepare(`
    SELECT * FROM transactions WHERE id = ?
  `).get(id) as TransactionRow | undefined;

  if (!row) return null;

  return {
    transactionId: row.id,
    amount: row.amount,
    currency: row.currency,
    description: row.description,
    metadata: row.metadata ? JSON.parse(row.metadata) : null,
    status: row.status,
    submittedAt: row.submitted_at,
    completedAt: row.completed_at,
    error: row.error
  };
}
