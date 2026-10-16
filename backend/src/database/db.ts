import Database from "better-sqlite3";

export const db = new Database("transactions.db");

export function initializeDatabase(): void {
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");

  db.exec(`
    CREATE TABLE IF NOT EXISTS transactions (
      id TEXT PRIMARY KEY,
      amount TEXT NOT NULL,
      currency TEXT NOT NULL,
      description TEXT NOT NULL,
      metadata TEXT,
      status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
      submitted_at TEXT NOT NULL,
      processing_started_at TEXT,
      completed_at TEXT,
      error TEXT
    );

    CREATE TABLE IF NOT EXISTS outbox (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      transaction_id TEXT NOT NULL UNIQUE,
      published_at TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY (transaction_id)
        REFERENCES transactions(id)
    );
  `);

  const columns = db.prepare(
    "PRAGMA table_info(transactions)"
  ).all() as Array<{ name: string }>;

  if (!columns.some((column) => column.name === "processing_started_at")) {
    db.exec(
      "ALTER TABLE transactions ADD COLUMN processing_started_at TEXT"
    );
  }

  if (!columns.some((column) => column.name === "idempotency_key")) {
    db.exec(
      "ALTER TABLE transactions ADD COLUMN idempotency_key TEXT"
    );
  }

  if (!columns.some((column) => column.name === "request_hash")) {
    db.exec(
      "ALTER TABLE transactions ADD COLUMN request_hash TEXT"
    );
  }

  db.exec(`
    CREATE UNIQUE INDEX IF NOT EXISTS
    idx_transactions_idempotency_key
    ON transactions(idempotency_key)
    WHERE idempotency_key IS NOT NULL
  `);

  console.log("SQLite database initialized successfully!");
}