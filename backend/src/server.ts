import "dotenv/config";
import { app } from "./app.js";
import { connectRedis } from "./queue/redis.js";
import { initializeDatabase } from "./database/db.js";
import { startOutboxPublisher } from "./queue/outboxPublisher.js";
import { startTransactionWorker } from "./workers/transactionWorker.js";

const PORT = Number(process.env.PORT) || 3000;

async function startServer(): Promise<void> {
  try {
    initializeDatabase();
    await connectRedis();

    startOutboxPublisher();
    void startTransactionWorker().catch((error) => {
      console.error("Worker stopped:", error);
    });

    app.listen(PORT, () => {
      console.log(`Transaction API running at http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error("Failed to start the server:", error);
    process.exit(1);
  }
}

startServer();