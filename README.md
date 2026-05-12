# AI Finance Agent

An AI-powered financial reconciliation agent built with LangGraph, FastAPI, and the Anthropic Claude API. The agent uses Claude's tool calling to fetch financial balances from multiple sources, detects discrepancies, classifies risk, and determines whether human approval is required.

## What it does

1. Invokes Claude 3.5 Sonnet with bound finance tools
2. Claude calls `get_exchange_balance` and `get_blockchain_balance` via tool use
3. Computes the discrepancy between sources
4. Classifies risk level (`low` / `medium` / `high`)
5. Decides whether human approval is needed
6. Returns a structured JSON result

## Stack

| Layer | Technology |
| --- | --- |
| API | FastAPI + Uvicorn |
| Workflow | LangGraph |
| AI Model | Anthropic Claude 4.5 Haiku (`claude-haiku-4-5`) |
| LLM Framework | LangChain + langchain-anthropic |
| Validation | Pydantic |
| Config | python-dotenv |
| HTTP client | httpx |
| Language | Python 3.11+ |

## Project structure

```
ai-finance-agent/
├── app/
│   ├── config/
│   │   └── settings.py                 # Environment variable loading
│   ├── schemas/
│   │   └── reconciliation.py           # Pydantic output schema
│   ├── services/
│   │   └── claude_service.py           # Claude client + tool binding
│   ├── tools/
│   │   └── finance_tools.py            # LangChain tools (exchange, blockchain)
│   ├── workflows/
│   │   ├── state.py                    # LangGraph WorkflowState TypedDict
│   │   ├── nodes.py                    # reconciliation_agent node logic
│   │   └── graph.py                    # LangGraph StateGraph definition
│   └── main.py                         # FastAPI app + /reconcile endpoint
├── requirements.txt
├── .env
└── README.md
```

## How the workflow runs

**LangGraph flow**

```
[start] --> reconciliation_agent --> [end]
```

**Inside `reconciliation_agent`:**

1. Prompts Claude to retrieve balances using available tools
2. Claude responds with tool calls (`get_exchange_balance`, `get_blockchain_balance`)
3. Each tool is invoked and returns the balance value
4. Risk logic is applied to the results:
   - `difference = |exchange_balance - blockchain_balance|`
   - `risk_level`: `low` (<= 100) / `medium` (> 100) / `high` (> 1000)
   - `requires_approval = difference > 100`

**Workflow state (`WorkflowState`)**

```python
class WorkflowState(TypedDict, total=False):
    exchange_balance: float
    blockchain_balance: float
    difference: float
    risk_level: str
    requires_approval: bool
    llm_response: str
```

**Output**
```json
{
    "exchange_balance": 10000,
    "blockchain_balance": 9700,
    "difference": 300.0,
    "risk_level": "medium",
    "requires_approval": true
}
```

## Setup

**1. Clone and create virtual environment**

```bash
git clone <repo-url>
cd ai-finance-agent

python -m venv venv

# Linux/macOS
source venv/bin/activate

# Windows
venv\Scripts\activate
```

**2. Install dependencies**

```bash
pip install -r requirements.txt
```

`requirements.txt`:
```
fastapi
uvicorn
langgraph
langchain
langchain-anthropic
anthropic
pydantic
python-dotenv
httpx
```

**3. Configure environment**

Create a `.env` file in the project root:

```env
ANTHROPIC_API_KEY=your_api_key_here
```

**4. Run the server**

```bash
uvicorn app.main:app --reload
```

The API will be available at `http://localhost:8000`.

## API

### `POST /reconcile`

Triggers a reconciliation run. The agent fetches balances automatically via tool calling — no request body required.

**Response**

```json
{
    "exchange_balance": 10000,
    "blockchain_balance": 9700,
    "difference": 300.0,
    "risk_level": "medium",
    "requires_approval": true
}
```

Interactive docs are available at `http://localhost:8000/docs`.
