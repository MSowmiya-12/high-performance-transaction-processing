import { db } from "../database/db.js";
import { redisClient } from "./redis.js";

const STREAM = "transactions:stream";

export async function publishOutboxBatch(): Promise<void> {
  const jobs = db.prepare(`
    SELECT id, transaction_id
    FROM outbox
    WHERE published_at IS NULL
    ORDER BY id
    LIMIT 100
  `).all() as Array<{ id: number; transaction_id: string }>;

  for (const job of jobs) {
    await redisClient.xAdd(STREAM, "*", {
      transactionId: job.transaction_id,
      attempts: "0"
    });

    db.prepare(`
      UPDATE outbox
      SET published_at = ?
      WHERE id = ? AND published_at IS NULL
    `).run(new Date().toISOString(), job.id);
  }
}

export function startOutboxPublisher(): void {
  const publish = async () => {
    try {
      await publishOutboxBatch();
    } catch (error) {
      console.error("Outbox publisher error:", error);
    }
  };

  void publish();
  setInterval(() => void publish(), 1000);
}