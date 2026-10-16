import { Router } from "express";
import { z } from "zod";
import {
  getTransaction,
  submitTransaction,
  IdempotencyConflictError
} from "../services/transactionService.js";

const router = Router();

const transactionSchema = z.object({
  amount: z.number().finite().positive(),
  currency: z.string().regex(/^[A-Za-z]{3}$/),
  description: z.string().trim().min(1).max(500),
  metadata: z.record(z.string(), z.unknown()).optional()
}).strict();

router.post("/", (req, res) => {
  const parsed = transactionSchema.safeParse(req.body);

  if (!parsed.success) {
    return res.status(400).json({
      error: "Invalid transaction data",
      details: parsed.error.issues
    });
  }

    const idempotencyKey = req.get("Idempotency-Key")?.trim();

  if (idempotencyKey && idempotencyKey.length > 200) {
    return res.status(400).json({
      error: "Idempotency-Key must not exceed 200 characters"
    });
  }

  try {
    const result = submitTransaction(
      {
        ...parsed.data,
        currency: parsed.data.currency.toUpperCase()
      },
      idempotencyKey || undefined
    );

    return res
      .status(result.duplicate ? 200 : 202)
      .json(result.transaction);
  } catch (error) {
    if (error instanceof IdempotencyConflictError) {
      return res.status(409).json({
        error: error.message
      });
    }

    console.error("Transaction submission failed:", error);
    return res.status(500).json({
      error: "Could not save transaction"
    });
  }
});

router.get("/:id", (req, res) => {
  const transaction = getTransaction(req.params.id);

  if (!transaction) {
    return res.status(404).json({
      error: "Transaction not found"
    });
  }

  return res.status(200).json(transaction);
});

export default router;