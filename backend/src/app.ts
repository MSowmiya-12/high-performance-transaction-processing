
import express from "express";
import cors from "cors";
import transactionRoutes from "./routes/transactionRoutes.js";
import { redisClient } from "./queue/redis.js";
import { db } from "./database/db.js";

export const app = express();

app.use(cors());
app.use(express.json({ limit: "100kb" }));

// Health check endpoint

app.get("/api/health", async (_req, res) => {
  try {
    const redisConnected = redisClient.isReady;

    let streamLength: number | null = null;
    let pendingMessages: number | null = null;

    if (redisConnected) {
      try {
        streamLength = await redisClient.xLen("transactions:stream");
      } catch {
        streamLength = 0;
      }

      try {
        const pending = await redisClient.xPending(
          "transactions:stream",
          "transaction-workers"
        );
        pendingMessages = pending.pending;
      } catch {
        pendingMessages = 0;
      }
    }

    const delayedRetries = redisConnected
      ? await redisClient.zCard("transactions:delayed-retries")
      : null;

    const transactionCounts = db.prepare(`
      SELECT
        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
        SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) AS processing,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
        SUM(CASE WHEN status IN ('pending', 'processing') THEN 1 ELSE 0 END) AS uncompleted,
        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
        SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END) AS errors,
        SUM(CASE WHEN status = 'processing' AND (
          processing_started_at IS NULL OR strftime('%s', processing_started_at) * 1000 <= ?
        ) THEN 1 ELSE 0 END) AS stale_processing
      FROM transactions
    `).get(Date.now() - 60_000) as {
      pending: number | null;
      processing: number | null;
      failed: number | null;
      uncompleted: number | null;
      completed: number | null;
      errors: number | null;
      stale_processing: number | null;
    };

    return res.status(redisConnected ? 200 : 503).json({
      status: redisConnected ? "ok" : "degraded",
      service: "transaction-processing-api",
      redis: redisConnected ? "connected" : "disconnected",
      streamLength,
      pendingMessages,
      delayedRetries,
      pendingTransactions: transactionCounts.pending ?? 0,
      processingTransactions: transactionCounts.processing ?? 0,
      staleProcessingTransactions: transactionCounts.stale_processing ?? 0,
      failedTransactions: transactionCounts.failed ?? 0,
      uncompletedTransactions: transactionCounts.uncompleted ?? 0,
      completedTransactions: transactionCounts.completed ?? 0,
      transactionsWithErrors: transactionCounts.errors ?? 0,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error("Health check failed:", error);

    return res.status(503).json({
      status: "degraded",
      service: "transaction-processing-api",
      redis: redisClient.isReady ? "connected" : "disconnected",
      timestamp: new Date().toISOString()
    });
  }
});


// Transaction API routes
app.use("/api/transactions", transactionRoutes);
