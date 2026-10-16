
import { useEffect, useState } from "react";
import "./App.css";

const API = "http://localhost:3000/api";

export default function App() {
  const [health, setHealth] = useState(null);
  const [form, setForm] = useState({
    amount: "",
    currency: "INR",
    description: "",
  });
  const [transactionId, setTransactionId] = useState("");
  const [transaction, setTransaction] = useState(null);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  async function loadHealth() {
    try {
      const res = await fetch(`${API}/health`);
      if (!res.ok) throw new Error();
      setHealth(await res.json());
    } catch {
      setHealth(null);
    }
  }

  useEffect(() => {
    loadHealth();
    const timer = setInterval(loadHealth, 5000);
    return () => clearInterval(timer);
  }, []);

  async function submitTransaction(e) {
    e.preventDefault();
    setLoading(true);
    setError("");
    setMessage("");
    setTransaction(null);

    try {
      const res = await fetch(`${API}/transactions`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": crypto.randomUUID(),
        },
        body: JSON.stringify({
          amount: Number(form.amount),
          currency: form.currency,
          description: form.description,
        }),
      });

      const data = await res.json();

      if (!res.ok) {
        throw new Error(data.error || data.message || "Submission failed");
      }

      const id = data.transactionId || data.id;

      setMessage("Transaction submitted successfully!");
      if (id) {
        setTransactionId(id);
        await lookupTransaction(id);
      }
      setForm({ amount: "", currency: "INR", description: "" });
      loadHealth();
    } catch (err) {
      setError(err.message || "Could not connect to the API.");
    } finally {
      setLoading(false);
    }
  }

  async function lookupTransaction(id = transactionId) {
    if (!id.trim()) {
      setError("Please enter a transaction ID.");
      return;
    }

    setError("");
    setTransaction(null);

    try {
      const res = await fetch(
        `${API}/transactions/${encodeURIComponent(id.trim())}`
      );
      const data = await res.json();

      if (!res.ok) {
        throw new Error(data.error || data.message || "Transaction not found");
      }

      setTransaction(data);
    } catch (err) {
      setError(err.message || "Unable to retrieve transaction.");
    }
  }

  const metrics = [
    {
      label: "Completed",
      value: health?.completedTransactions ?? "—",
      color: "green",
      icon: "✓",
    },
    {
      label: "Pending",
      value: health?.pendingTransactions ?? "—",
      color: "orange",
      icon: "◷",
    },
    {
      label: "Processing",
      value: health?.processingTransactions ?? "—",
      color: "blue",
      icon: "↻",
    },
    {
      label: "Failed",
      value: health?.failedTransactions ?? "—",
      color: "red",
      icon: "!",
    },
  ];

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <div className="brand-logo">T</div>
          <div>
            <h1>TransactFlow</h1>
            <p>Transaction Processing System</p>
          </div>
        </div>

        <div className="connection">
          <span className={`dot ${health ? "online" : ""}`} />
          {health ? "API Connected" : "API Unavailable"}
        </div>
      </header>

      <main>
        <section className="welcome">
          <div>
            <p className="eyebrow">SYSTEM OVERVIEW</p>
            <h2>Transaction Dashboard</h2>
            <p>Monitor transactions, track payments, and check system health.</p>
          </div>
          <button className="secondary-button" onClick={loadHealth}>
            ↻ Refresh
          </button>
        </section>

        <section className="metrics">
          {metrics.map((item) => (
            <article className="metric-card" key={item.label}>
              <div className="metric-top">
                <span>{item.label}</span>
                <span className={`metric-icon ${item.color}`}>
                  {item.icon}
                </span>
              </div>
              <strong>{item.value}</strong>
              <p>{health ? "Live system count" : "Waiting for API"}</p>
            </article>
          ))}
        </section>

        <section className="main-grid">
          <article className="panel">
            <div className="panel-title">
              <div>
                <p className="eyebrow">NEW PAYMENT</p>
                <h3>Submit Transaction</h3>
              </div>
              <span className="title-icon">＋</span>
            </div>

            <form onSubmit={submitTransaction}>
              <label>
                Amount
                <input
                  type="number"
                  min="0.01"
                  step="0.01"
                  placeholder="Enter amount"
                  value={form.amount}
                  onChange={(e) =>
                    setForm({ ...form, amount: e.target.value })
                  }
                  required
                />
              </label>

              <label>
                Currency
                <select
                  value={form.currency}
                  onChange={(e) =>
                    setForm({ ...form, currency: e.target.value })
                  }
                >
                  <option value="INR">INR — Indian Rupee</option>
                  <option value="USD">USD — US Dollar</option>
                  <option value="EUR">EUR — Euro</option>
                  <option value="GBP">GBP — Pound Sterling</option>
                </select>
              </label>

              <label>
                Description
                <input
                  type="text"
                  placeholder="e.g. Online payment"
                  value={form.description}
                  onChange={(e) =>
                    setForm({ ...form, description: e.target.value })
                  }
                  required
                />
              </label>

              <button className="primary-button" disabled={loading}>
                {loading ? "Submitting..." : "Submit Transaction →"}
              </button>
            </form>

            {message && <p className="success-message">{message}</p>}
          </article>

          <article className="panel">
            <div className="panel-title">
              <div>
                <p className="eyebrow">TRANSACTION LOOKUP</p>
                <h3>Track Transaction</h3>
              </div>
              <span className="title-icon">⌕</span>
            </div>

            <label>
              Transaction ID
              <input
                type="text"
                placeholder="Paste transaction UUID"
                value={transactionId}
                onChange={(e) => setTransactionId(e.target.value)}
              />
            </label>

            <button
              className="primary-button"
              onClick={() => lookupTransaction()}
            >
              Find Transaction →
            </button>

            {transaction && (
              <div className="transaction-result">
                <p className="result-heading">Transaction Details</p>
                <div className="result-row">
                  <span>Status</span>
                  <span
                    className={`status ${String(
                      transaction.status || "unknown"
                    ).toLowerCase()}`}
                  >
                    {transaction.status || "Unknown"}
                  </span>
                </div>
                <div className="result-row">
                  <span>Amount</span>
                  <strong>
                    {transaction.amount ?? "—"}{" "}
                    {transaction.currency ?? ""}
                  </strong>
                </div>
                <div className="result-row">
                  <span>Description</span>
                  <strong>{transaction.description ?? "—"}</strong>
                </div>
                <p className="transaction-id">
                  {transaction.id || transaction.transactionId || transactionId}
                </p>
              </div>
            )}
          </article>
        </section>

        {error && (
          <div className="error-message">
            {error}
            <button onClick={() => setError("")}>Dismiss</button>
          </div>
        )}

        <section className="system-panel">
          <div className="panel-title">
            <div>
              <p className="eyebrow">INFRASTRUCTURE</p>
              <h3>System Health</h3>
            </div>
            <span className="health-label">
              {health?.status === "ok" ? "● Operational" : "● Unavailable"}
            </span>
          </div>

          <div className="health-grid">
            <div className="health-item">
              <span>Transaction API</span>
              <strong>{health?.status === "ok" ? "Healthy" : "Unavailable"}</strong>
            </div>
            <div className="health-item">
              <span>Redis</span>
              <strong>{health?.redis || "Unknown"}</strong>
            </div>
            <div className="health-item">
              <span>Pending messages</span>
              <strong>{health?.pendingMessages ?? "—"}</strong>
            </div>
            <div className="health-item">
              <span>Delayed retries</span>
              <strong>{health?.delayedRetries ?? "—"}</strong>
            </div>
          </div>

          <p className="updated">
            {health?.timestamp
              ? `Last updated ${new Date(health.timestamp).toLocaleTimeString()}`
              : "Waiting for health data"}
          </p>
        </section>
      </main>

      <footer>
        TransactFlow · High Performance Transaction Processing System
      </footer>
    </div>
  );
}
