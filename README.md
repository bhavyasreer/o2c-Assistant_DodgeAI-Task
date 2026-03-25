# O2C AI — Order-to-Cash Data Explorer

This project provides a web UI to explore an SAP Order-to-Cash (O2C) dataset as a graph and ask natural-language questions. The backend converts questions to SQL, runs them against a local SQLite database, and (optionally) uses an LLM to turn the results into a concise answer.

---

## Live demo

When deployed (example instructions below), the app is available at `/` (React UI) and exposes these API endpoints:

- `POST /api/chat`
- `GET  /api/graph`
- `GET  /api/graph/expand/{node_id}`
- `GET  /api/graph/node/{node_id}`
- `GET  /api/examples`

---

## Architecture overview

### Frontend

- React + Vite
- Graph visualization: `reactflow`
- Chat UI: calls `POST /api/chat` and highlights entity IDs mentioned in responses

The frontend is served by the FastAPI backend from the built `frontend/dist` output (so production is a single deployable service).

### Backend

- FastAPI (`app/main.py`)
- SQLite database shipped with the repo: `data.db`
- REST API under `/api/*`

---

## Database choice

The app uses **SQLite** because:

- It is a single-file database (`data.db`), which is ideal for demo and deployment.
- The user queries are naturally expressed as SQL over a relational schema.
- FastAPI can open short-lived connections per request without a separate DB service.

The shipped database includes an analytical view (`v_order_to_cash`) plus normalized tables for customers, products, orders, deliveries, billing, payments, and journal entries.

---

## LLM prompting strategy (two-step)

When `GROQ_API_KEY` is set, `POST /api/chat` uses a **two-stage** approach and responses are generated in natural language by the LLM.

1. **Natural language → SQL**
   - Prompt: `_SQL_PROMPT_TEMPLATE`
   - Includes:
     - a compact schema description (tables + join rules + dataset notes)
     - strict formatting requirements:
       - return ONLY SQL
       - no markdown code fences
       - output MUST start with `SELECT`
       - use SQLite syntax
       - prefer the `v_order_to_cash` view for exploration

2. **SQL results → natural language answer**
   - Prompt: `_NARRATION_PROMPT_TEMPLATE`
   - Inputs:
     - the user question
     - the executed SQL
     - the returned rows (as text)
   - Output rules:
     - be concise
     - do NOT hallucinate: only use the rows provided
     - do NOT mention SQL/table internals
     - currency formatting: dataset is INR; write `INR <amount>`

LLM settings:

- model: `llama-3.3-70b-versatile` (via Groq)
- `temperature=0` for more deterministic SQL generation

---

## Guardrails & safety

This project uses multiple layers to reduce failure modes and prevent unsafe outputs:

### Domain guardrail (blocking unrelated prompts)

- `_is_domain_question()` rejects questions that do not look like they relate to the O2C dataset domain.

### SQL guardrails (LLM output hardening)

- `_extract_sql()` attempts to extract a `SELECT ...` statement from model output.
- `_sanitize_sql()` further enforces:
  - only a single statement (keeps text before the first `;`)
  - the SQL must start with `SELECT`
  - common write/DCL keywords are rejected (`insert|update|delete|drop|alter|create|...`)

### Natural-language output hardening

- `_sanitize_natural_language_answer()`:
  - removes code fences
  - converts accidental `$123` to `INR 123`
  - rejects outputs that look like SQL

### Deterministic fallback for “no LLM” demos
If `GROQ_API_KEY` is **not** set:
- numeric entity-ID questions still work via deterministic DB lookups (`_lookup_ids_direct`)
- common “example” analytics questions use a limited rule-based SQL fallback (`_rule_based_answer`)

When `GROQ_API_KEY` is set, the deterministic shortcut for ID-queries is disabled so the LLM is responsible for the final natural-language phrasing.

---

## Local setup

### Backend

1. (Optional) create and activate a virtual env
2. Install dependencies:

```bash
python -m pip install -r requirements.txt
```

3. Start the server:

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

4. (Optional) enable LLM:

```bash
export GROQ_API_KEY="..."
```

### Frontend build

```bash
cd frontend
npm install
npm run build
```

In production, the backend serves the built frontend automatically.

---

## Docker / production deployment

### Build image

```bash
docker build -t o2c-ai .
```

### Run image

```bash
docker run -p 8000:8000 \
  -e GROQ_API_KEY="..." \
  o2c-ai
```

Then open:

- `http://localhost:8000/`

---

## Creating the submission demo link (Render example)

1. Push this repo to a **public GitHub** repository.
2. In Render: create a **Web Service** → choose **Docker**.
3. Set the Dockerfile path to `Dockerfile` (repo root).
4. Add environment variable `GROQ_API_KEY` if you want full LLM chat.
5. After deployment, the service URL is your public demo link.

Health check suggestion (optional): `GET /api/examples`

