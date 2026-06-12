# 🚀 Outreach Automation API

A scalable, distributed backend system for ingesting high-volume outreach campaigns, queueing them safely, and dispatching messages at a controlled, rate-limited pace.

## 🏗️ System Architecture

This project uses a **decoupled producer/consumer pattern** to absorb traffic spikes without overwhelming downstream integrations.

* **API Service (FastAPI):** Ingests campaign staging requests, validates JWT auth, and writes pending jobs to MongoDB and RabbitMQ.
* **Message Broker (RabbitMQ):** Holds pending dispatch jobs, supporting delayed/scheduled delivery via the delayed-message exchange plugin.
* **Database (MongoDB):** Source of truth for job state — workers check here to enforce idempotency and handle edge cases like double-sends or rescheduled jobs.
* **Worker Service (Async Python):** Consumes jobs from RabbitMQ at a controlled pace, with retry/backoff and dead-letter handling for permanent failures.
* **Rate Limiter (Redis):** Sliding-window rate limit (default: 20 messages/minute) protecting outbound dispatch from exceeding external API limits.

## 🛠️ Tech Stack
* **Python 3.12+** (managed via `uv`)
* **FastAPI**
* **MongoDB & Motor**
* **RabbitMQ & aio-pika**
* **Redis**
* **HTTPX**

---

## ⚙️ Prerequisites
* Docker & Docker Compose
* Python 3.12+
* [uv](https://github.com/astral-sh/uv)

## 🚀 Installation & Setup

**1. Clone and install dependencies**
```bash
git clone https://github.com/yourusername/outreach-automation-api.git
cd outreach-automation-api
uv venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
uv sync
```

**2. Configure environment variables**

Create a `.env` file:

```env
MONGO_URI="mongodb://localhost:27017"
REDIS_URI="redis://localhost:6379"
RABBITMQ_URI="amqp://guest:guest@localhost/"
```

**3. Spin up backing services**

```bash
docker-compose -f docker-compose.dev.yml up -d
```

**4. Run the ecosystem**

*Terminal 1 (API):*
```bash
fastapi dev api/main.py
```

*Terminal 2 (Worker):*
```bash
python -m worker.main
```

---

## 📊 Load Testing & Scalability Verification

Dedicated scripts verify ingestion throughput and per-worker dispatch capacity.

**1. High-volume ingestion test**

Fires concurrent batches into the API to measure database/broker throughput.

```bash
python -m tests.simulate_traffic
```

**2. Sustained load test**

Sends a steady stream of requests at a fixed rate over a fixed duration.

```bash
python -m tests.constant_load
```

**3. Metrics report**

Analyzes the `dispatch_jobs` collection in MongoDB to report processing throughput, queue backlog, and per-worker daily capacity.

```bash
python -m tests.generate_report
```

---

## 📁 Project Structure

```text
outreach_automation_api/
├── api/                  # FastAPI application & HTTP routers
├── worker/               # Async task consumer & template engine
├── shared/               # Shared database schemas and models
├── services/             # Core singletons (Mongo, Redis, RMQ, HTTP, Echo)
├── integrations/         # External API clients
├── tests/                # Load testing and reporting scripts
├── config/               # App configuration and environment management
├── docker-compose.dev.yml
├── topology.yaml         # RabbitMQ exchange/queue definitions
├── pyproject.toml
└── uv.lock
```