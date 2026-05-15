# AI Finance Agent

An AI-powered financial reconciliation agent built with LangGraph, FastAPI, and the Anthropic Claude API. The agent fetches exchange and blockchain balances via MCP tools, detects discrepancies, classifies risk, routes high-risk cases through a human approval workflow, and produces a full audit trail compliant with SOX requirements.

## Features

- **MCP server** exposing finance tools over HTTP (streamable transport)
- **LangGraph conditional routing** — auto-approve low risk, escalate high risk for human review
- **Human-in-the-loop** via LangGraph `interrupt()` + SQLite checkpointing; resumes on `POST /approvals/{id}/approve`
- **Kafka integration** — publish/consume reconciliation events asynchronously
- **OpenTelemetry tracing** — distributed spans exported to Jaeger
- **Temporal Saga workflows** — durable, compensating reconciliation with signal-based human approval
- **Multi-agent architecture** — parallel Risk Analyst, Compliance Officer, and Operations Specialist agents synthesized by a Supervisor (CRO) into an executive summary
- **SOX-friendly audit logging** — append-only JSON log with HMAC-SHA256 integrity hashes per record, queryable via REST

## Stack

| Layer | Technology |
|---|---|
| API | FastAPI + Uvicorn |
| Workflow orchestration | LangGraph (StateGraph + SQLite checkpointer) |
| Durable workflows | Temporal (Saga pattern) |
| AI model | Anthropic Claude Haiku 4.5 (`claude-haiku-4-5`) |
| LLM framework | LangChain + langchain-anthropic |
| Tool protocol | MCP (Model Context Protocol) — streamable HTTP |
| Async messaging | Apache Kafka (KRaft, no Zookeeper) + aiokafka |
| Observability | OpenTelemetry SDK + Jaeger (OTLP) |
| Persistence | SQLite (LangGraph checkpoints), PostgreSQL (Temporal) |
| Validation | Pydantic |
| Config | python-dotenv |
| HTTP client | httpx |
| Language | Python 3.11+ |

## Project structure

```
ai-finance-agent/
├── app/
│   ├── agents/
│   │   ├── compliance.py           # SOX/AML compliance officer agent
│   │   ├── operations.py           # Blockchain operations specialist agent
│   │   ├── risk_analyst.py         # Senior risk analyst agent
│   │   └── supervisor.py           # CRO supervisor — synthesizes all three agents
│   ├── api/
│   │   └── approvals.py            # Approval/rejection endpoints (LangGraph resume)
│   ├── audit/
│   │   ├── events.py               # Audit event type constants
│   │   ├── logger.py               # Append-only JSON logger with HMAC integrity
│   │   └── router.py               # GET /audit endpoints
│   ├── config/
│   │   └── settings.py             # Environment variable loading
│   ├── kafka/
│   │   ├── consumer.py             # aiokafka async consumer
│   │   ├── producer.py             # aiokafka async producer
│   │   └── topics.py               # Topic name constants
│   ├── mcp_client/
│   │   └── client.py               # MCP client (streamable HTTP, tool cache)
│   ├── mcp_server/
│   │   └── server.py               # FastMCP server exposing finance tools
│   ├── schemas/
│   │   └── reconciliation.py       # Pydantic request schema
│   ├── services/
│   │   ├── approval_service.py     # In-memory workflow store
│   │   ├── claude_service.py       # Claude LLM client
│   │   └── telegram_service.py     # Telegram approval notifications
│   ├── telemetry/
│   │   └── tracer.py               # OTel setup + get_tracer()
│   ├── temporal/
│   │   ├── activities.py           # Temporal activities (fetch, reconcile, notify, finalize)
│   │   ├── worker.py               # Temporal worker setup
│   │   └── workflows.py            # ReconciliationSaga with compensation
│   ├── workflows/
│   │   ├── graph.py                # Standard LangGraph StateGraph
│   │   ├── multi_agent_graph.py    # Multi-agent LangGraph StateGraph
│   │   ├── nodes.py                # All LangGraph node functions
│   │   ├── routes.py               # Conditional routing logic
│   │   └── state.py                # WorkflowState TypedDict
│   └── main.py                     # FastAPI app, lifespan, all endpoints
├── docker-compose.yml              # PostgreSQL, Temporal, Jaeger, Kafka
├── requirements.txt
├── .env
└── README.md
```

## Architecture

### Standard flow (`POST /reconcile`)

```
[start]
   │
   ▼
reconciliation_agent          ← fetches balances via MCP (parallel), runs LLM analysis
   │
   ├── requires_approval=true ──► human_approval_node   ← sends Telegram notification
   │                                      │
   │                               wait_for_approval_node  ← interrupt() — pauses here
   │                                      │
   │                              POST /approvals/{id}/approve or /reject
   │                                      │
   │                                    [end]
   │
   └── requires_approval=false ──► auto_approve_node ──► [end]
```

### Multi-agent flow (`POST /reconcile/multi-agent`)

```
[start]
   │
   ▼
multi_agent_reconciliation_node
   ├── get_exchange_balance  ┐  (parallel, via MCP)
   ├── get_blockchain_balance┘
   ├── risk_analyst.run()    ┐
   ├── compliance.run()      ├─ (parallel, asyncio.gather)
   └── operations.run()      ┘
          │
          ▼
   supervisor.run()           ← synthesizes into executive summary + APPROVE/REJECT/ESCALATE
          │
   ┌──────┴──────┐
   ▼             ▼
human_approval  auto_approve   ← same approval nodes as standard flow
```

### Temporal Saga flow (`POST /reconcile/temporal`)

```
ReconciliationSaga
   ├── fetch_balances_activity
   ├── reconcile_activity
   ├── notify_approval_activity    (if requires_approval)
   ├── wait_condition              ← blocks up to 24h for signal
   │       └── receive_decision signal  ← POST /reconcile/temporal/{id}/approve or /reject
   └── finalize_activity
         └── on failure: cancel_approval_activity (Saga compensation)
```

### Kafka flow (`POST /reconcile/kafka`)

```
POST /reconcile/kafka
   │
   └── publishes to reconciliation.requested
               │
               ▼
        kafka consumer
               │
               ▼
        handle_reconciliation_from_kafka  ← runs standard graph
```

**Topics**

| Topic | When |
|---|---|
| `reconciliation.requested` | Reconciliation triggered via Kafka |
| `reconciliation.completed` | Balances fetched and risk classified |
| `reconciliation.approval_requested` | Human review required |
| `reconciliation.approved` | Human approved |
| `reconciliation.rejected` | Human rejected |
| `reconciliation.auto_approved` | Auto-approved (low risk) |

## Setup

### 1. Clone and create virtual environment

```bash
git clone <repo-url>
cd ai-finance-agent

python -m venv venv

# Linux/macOS
source venv/bin/activate

# Windows
venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment

Create a `.env` file in the project root:

```env
ANTHROPIC_API_KEY=your_api_key_here

# Optional — Telegram notifications for human approval
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# Infrastructure (defaults match docker-compose.yml)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
TEMPORAL_HOST=localhost:7233

# SOX audit log signing key — change in production
AUDIT_HMAC_KEY=change-me-in-production
```

### 4. Start infrastructure

```bash
docker compose up -d
```

This starts: PostgreSQL, Temporal + Temporal UI, Jaeger, and Kafka.

| Service | URL |
|---|---|
| Temporal UI | http://localhost:8080 |
| Jaeger UI | http://localhost:16686 |
| Kafka | localhost:9092 |

### 5. Run the server

```bash
uvicorn app.main:app --reload
```

API available at `http://localhost:8000`. Interactive docs at `http://localhost:8000/docs`.

## API

### Reconciliation

| Method | Path | Description |
|---|---|---|
| `POST` | `/reconcile` | Standard LangGraph flow |
| `POST` | `/reconcile/multi-agent` | Multi-agent flow with specialist analysis |
| `POST` | `/reconcile/kafka` | Publish to Kafka topic (async trigger) |
| `POST` | `/reconcile/temporal` | Start a Temporal Saga workflow |
| `POST` | `/reconcile/temporal/{id}/approve` | Send approval signal to Temporal |
| `POST` | `/reconcile/temporal/{id}/reject` | Send rejection signal to Temporal |

**Request body** (for `/reconcile` and `/reconcile/multi-agent`):

```json
{
  "exchange_balance": 100000,
  "blockchain_balance": 85000
}
```

**Response — `/reconcile/multi-agent`:**

```json
{
  "workflow_id": "b749dba2-...",
  "risk_analysis": "The $15,000 discrepancy represents a 15% variance...",
  "compliance_analysis": "This discrepancy may trigger SAR reporting obligations under BSA...",
  "recommendations": "Root causes likely include unsettled on-chain transactions...",
  "executive_summary": "ESCALATE. A $15,000 high-risk discrepancy requires immediate review...",
  "status": "WAITING_APPROVAL"
}
```

### Human-in-the-loop approvals (LangGraph)

| Method | Path | Description |
|---|---|---|
| `GET` | `/approvals/pending` | List workflows awaiting approval |
| `GET` | `/approvals/{id}` | Get workflow status |
| `POST` | `/approvals/{id}/approve` | Resume workflow with approval |
| `POST` | `/approvals/{id}/reject` | Resume workflow with rejection |

### Audit log

| Method | Path | Description |
|---|---|---|
| `GET` | `/audit/` | All audit events (optional `?workflow_id=` filter) |
| `GET` | `/audit/{workflow_id}` | Full audit trail for a workflow + integrity check |

**Audit trail response:**

```json
{
  "workflow_id": "b749dba2-...",
  "count": 3,
  "integrity_valid": true,
  "records": [
    {
      "event_id": "...",
      "timestamp": "2026-05-15T14:32:01.123456+00:00",
      "event_type": "RECONCILIATION_STARTED",
      "workflow_id": "b749dba2-...",
      "actor": "system",
      "data": { "exchange_balance": 100000, "blockchain_balance": 85000 },
      "integrity_hash": "a3f9e2..."
    },
    {
      "event_type": "APPROVAL_REQUESTED",
      "actor": "system",
      "data": { "difference": 15000, "risk_level": "high" }
    },
    {
      "event_type": "APPROVAL_RECEIVED",
      "actor": "human",
      "data": { "approved": true, "status": "APPROVED" }
    }
  ]
}
```

**Audit event types:**

| Event | Actor | When |
|---|---|---|
| `RECONCILIATION_STARTED` | system | Workflow begins |
| `RECONCILIATION_COMPLETED` | system | Balances fetched, risk classified |
| `MULTI_AGENT_ANALYSIS_COMPLETED` | system | All specialist agents finished |
| `APPROVAL_REQUESTED` | system | Human review requested |
| `APPROVAL_RECEIVED` | human | Approve or reject decision recorded |
| `AUTO_APPROVED` | system | Low-risk, no approval needed |

The audit log is written to `audit.log` (append-only). Each record is signed with HMAC-SHA256 using `AUDIT_HMAC_KEY`. The `integrity_valid` field in the audit trail response confirms no record has been tampered with.

## Testing

**Low risk (auto-approved):**
```bash
curl -X POST http://localhost:8000/reconcile \
  -H "Content-Type: application/json" \
  -d '{"exchange_balance": 100000, "blockchain_balance": 100050}'
```

**High risk (requires human approval):**
```bash
# 1. Start reconciliation
curl -X POST http://localhost:8000/reconcile \
  -H "Content-Type: application/json" \
  -d '{"exchange_balance": 100000, "blockchain_balance": 85000}'
# → save the workflow_id

# 2. Approve
curl -X POST http://localhost:8000/approvals/{workflow_id}/approve
```

**Multi-agent with high risk:**
```bash
curl -X POST http://localhost:8000/reconcile/multi-agent \
  -H "Content-Type: application/json" \
  -d '{"exchange_balance": 100000, "blockchain_balance": 85000}'
```

**View audit trail:**
```bash
curl http://localhost:8000/audit/{workflow_id}
```
