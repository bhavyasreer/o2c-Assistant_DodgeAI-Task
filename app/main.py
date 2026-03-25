"""
app/main.py — FastAPI backend for the SAP O2C natural-language query interface.

Endpoints
---------
POST /api/chat      Natural-language question → SQL → formatted answer (via SqlAssistant)
GET  /api/graph     v_order_to_cash → {nodes, edges} graph payload
GET  /api/examples  Curated example questions the UI can display as quick-start prompts

Environment variables
---------------------
    GROQ_API_KEY     — Groq API key (required for /api/chat)

Run locally
-----------
    uvicorn app.main:app --reload
"""

import os
import re
import sqlite3
import time
import textwrap
from pathlib import Path
from contextlib import contextmanager
from typing import Any, Generator

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# ── Configuration ──────────────────────────────────────────────────────────────

DB_PATH = os.getenv("DB_PATH", "data.db")

_client: Any | None = None
_client_key: str | None = None
_OUT_OF_SCOPE_MSG = (
    "This system is designed to answer questions related to the provided dataset only."
)

# In-memory session context (conversation memory).
# Intended for frontend-provided session_id created per page lifetime.
_SESSION_CTX: dict[str, dict[str, Any]] = {}
_SESSION_CTX_MAX_AGE_S = 60 * 60  # 1 hour
_SESSION_CTX_MAX_SIZE = 5000


def _get_model() -> Any:
    """Return (and lazily create) the shared Groq client."""
    global _client, _client_key
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise EnvironmentError("GROQ_API_KEY environment variable is not set.")
    if _client is None or _client_key != api_key:
        try:
            from groq import Groq  # type: ignore
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "Missing dependency: 'groq'. Add it to requirements.txt and install dependencies."
            ) from exc
        _client = Groq(api_key=api_key)
        _client_key = api_key
    return _client

# ── FastAPI app ────────────────────────────────────────────────────────────────

app = FastAPI(
    title="SAP O2C Assistant",
    description="Natural-language querying over a SAP Order-to-Cash SQLite database.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Database helpers ───────────────────────────────────────────────────────────

@contextmanager
def get_db() -> Generator[sqlite3.Connection, None, None]:
    """Yield a short-lived SQLite connection with row_factory set."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()


# ── SqlAssistant ───────────────────────────────────────────────────────────────

# Compact schema handed to the LLM so it can write correct SQLite queries.
_SCHEMA_CONTEXT = textwrap.dedent("""
    SQLite database: SAP Order-to-Cash (O2C) system.

    TABLES
    ------
    customers          (id PK, name)
    products           (id PK, base_unit, product_type)
    product_descriptions (product_id FK->products, language, name)   PK=(product_id,language)
    orders             (id PK, customer_id FK->customers, creation_date, total_net_amount,
                        currency, delivery_status, payment_terms, requested_delivery_date)
    order_items        (order_id FK->orders, item_id, product_id FK->products,
                        quantity, quantity_unit, net_amount, currency)  PK=(order_id,item_id)
    deliveries         (id PK, order_id FK->orders, creation_date,
                        goods_movement_status, picking_status)
    billing            (id PK, delivery_id FK->deliveries, customer_id FK->customers,
                        billing_date, total_net_amount, currency,
                        is_cancelled [0/1], accounting_document)
    payments           (id PK, customer_id FK->customers, amount, currency,
                        posting_date, clearing_date, clearing_document, fiscal_year)
    journal_entries    (id PK autoincrement, accounting_document, billing_document FK->billing,
                        customer_id FK->customers, gl_account, amount, currency,
                        posting_date, document_type, profit_center)
    customer_company_assignments (customer_id FK->customers, company_code, reconciliation_account,
                        payment_terms, customer_account_group, deletion_indicator)
    customer_sales_area_assignments (customer_id FK->customers, sales_organization,
                        distribution_channel, division, currency, customer_payment_terms,
                        delivery_priority, incoterms_classification, shipping_condition)
    business_partner_addresses (business_partner_id FK->customers, address_id, city_name,
                        country, postal_code, region, street_name)
    plants             (plant_id PK, plant_name, sales_organization, distribution_channel, division)
    product_plants     (product_id FK->products, plant_id FK->plants, profit_center)
    product_storage_locations (product_id FK->products, plant_id FK->plants, storage_location)
    sales_order_schedule_lines (sales_order_id FK->orders, sales_order_item_id, schedule_line,
                        confirmed_delivery_date, confirmed_order_qty)

    VIEW
    ----
    v_order_to_cash -- pre-joins all tables; use for exploratory queries.
    Key columns: customer_id, customer_name, order_id, order_date,
                 order_total_amount, product_id, product_name,
                 delivery_id, billing_id, billed_amount,
                 billing_is_cancelled, payment_id, payment_amount,
                 journal_accounting_doc, journal_amount

    JOIN RULES (important)
    ----------------------
    - orders       -> customers  : orders.customer_id = customers.id
    - order_items  -> orders     : order_items.order_id = orders.id
    - deliveries   -> orders     : deliveries.order_id = orders.id
    - billing      -> deliveries : billing.delivery_id = deliveries.id
    - payments     -> billing    : payments.clearing_document = billing.accounting_document
    - journal_entries -> billing : journal_entries.billing_document = billing.id
    - product_descriptions join  : pd.product_id = p.id AND pd.language = 'EN'
    - customer assignments joins : cca.customer_id = customers.id
    - sales area joins           : csa.customer_id = customers.id
    - addresses joins            : bpa.business_partner_id = customers.id
    - product to plant joins     : pp.product_id = products.id and pp.plant_id = plants.plant_id

    DATA NOTES
    ----------
    - All IDs have leading zeros stripped  (e.g. customer_id = '310000108')
    - billing.id prefixes: '90*' = original invoice, '91*' = cancellation
    - Always filter billing with is_cancelled = 0 unless cancellations are requested
    - Currency is INR throughout
""").strip()

_SQL_PROMPT_TEMPLATE = textwrap.dedent("""
    You are an expert SQL generator.
    Your task: Convert the user's question into a valid SQLite SQL query.

    SCHEMA
    ------
    {schema}

    STRICT RULES:
    - Return ONLY the SQL query
    - Do NOT explain anything
    - Do NOT include markdown (no triple-backtick fences)
    - Do NOT include any text before or after the SQL
    - Output MUST start with SELECT
    - Use only SQLite syntax
    - Prefer using v_order_to_cash view

    EXAMPLE
    -------
    Input: "Top products by billing"
    Output:
    SELECT product_name, COUNT(*) AS billing_count
    FROM v_order_to_cash
    GROUP BY product_name
    ORDER BY billing_count DESC
    LIMIT 10;

    Question: {question}
""").strip()

_NARRATION_PROMPT_TEMPLATE = textwrap.dedent("""
    Convert the following SQL query results into a clear natural language answer.

    Original question: {question}

    SQL executed:
    {sql}

    Query results:
    {results}

    Rules:
    - Be concise
    - Mention key numbers and entities
    - Summarize if there are multiple rows
    - Do NOT hallucinate — use only the data provided above
    - Only state facts that are directly supported by the rows shown in "Query results"
    - If the question asks for something not present in the query results, say you cannot determine it from the returned data
    - Currency in this dataset is always INR. Always write `INR <amount>` and never use `$` or other currency symbols.
    - Do not mention SQL, tables, joins, or any internal reasoning
    - Return only the final answer (no SQL, no explanation)
""").strip()


class SqlAssistant:
    """
    Translates a natural-language question into SQL, runs it against the
    local SQLite database, and uses Gemini to narrate the results.
    """

    def __init__(self, db_path: str = DB_PATH) -> None:
        self.db_path = db_path

    # ── Public API ─────────────────────────────────────────────────────────

    def answer(self, question: str, session_id: str | None = None) -> str:
        """Return a plain-English answer to *question*."""
        question = (question or "").strip()
        # Normalize any SAP-style numeric IDs inside the text so the
        # LLM-generated SQL uses the same ID format as the DB.
        question = self._normalize_ids_in_text(question)

        # ── Optional conversation memory (conservative) ────────────────
        ids_in_question = self._extract_candidate_ids(question)
        explicit_entity_ctx = self._extract_entity_context(question)

        if session_id:
            session_id = session_id.strip()
            self._prune_session_ctx()

            if explicit_entity_ctx:
                self._set_session_ctx(session_id, explicit_entity_ctx)
            elif ids_in_question:
                # If user provides a new ID but we can't infer entity type,
                # clear context to avoid mixing entities.
                self._clear_session_ctx(session_id)

            session_ctx = self._get_session_ctx(session_id)
            if session_ctx and not ids_in_question and self._looks_like_followup(question):
                # Inject context without triggering the "direct lookup" shortcut.
                entity_kw = self._entity_type_to_keyword(session_ctx["type"])
                question = (
                    f"Referring to the previously mentioned {entity_kw} "
                    f"(ID {session_ctx['id']}): {question}"
                )
            elif session_ctx and not ids_in_question and not self._looks_like_followup(question):
                # General data questions should stop entity-scoped context.
                self._clear_session_ctx(session_id)

        if not self._is_domain_question(question):
            return _OUT_OF_SCOPE_MSG

        llm_configured = bool(os.getenv("GROQ_API_KEY"))
        direct_lookup = self._lookup_ids_direct(question)
        # 🔥 NEW: use session memory if no ID in question
        if not direct_lookup and session_id:
            session_ctx = self._get_session_ctx(session_id)
            if session_ctx:
                entity_id = session_ctx["id"]
                direct_lookup = self._lookup_ids_direct(entity_id)

        # When LLM is configured, we intentionally DO NOT shortcut ID queries
        # to deterministic DB text. We want responses to be generated (and
        # phrased) by the LLM for a consistent natural-language experience.
        if direct_lookup and self._is_id_lookup_question(question) and not llm_configured:
            return direct_lookup

        if not llm_configured:
            if direct_lookup:
                return direct_lookup
            fallback = self._rule_based_answer(question)
            if fallback:
                return fallback
            return "LLM is not configured. Please set GROQ_API_KEY."

        sql = self._generate_sql(question)
        if sql is None:
            if direct_lookup and not llm_configured:
                return direct_lookup
            return (
                "I couldn't generate a SQL query for that question using the LLM. "
                "Please try rephrasing or ask about customers, orders, products, "
                "deliveries, billing, payments, or journal entries."
            )

        rows, columns = self._run_query(sql)
        if rows is None:  # sqlite3 error — already logged
            return "The generated SQL query failed to execute. Check server logs for details."
        if not rows and direct_lookup and not self._looks_like_followup(question) and not llm_configured:
            # If SQL comes back empty for an explicit ID question (often due to
            # cancellation filters), return deterministic DB lookup instead.
            return direct_lookup

        answer = self._narrate(question, sql, rows, columns)
        answer = self._sanitize_natural_language_answer(answer)
        if not answer.strip():  # Gemini narration returned empty / not natural language
            return f"Found {len(rows)} record(s) based on your query."
        return answer

    # ── Private helpers ────────────────────────────────────────────────────

    def _generate_sql(self, question: str) -> str | None:
        """Call Gemini to produce a SQL query for *question*."""
        prompt = _SQL_PROMPT_TEMPLATE.format(
            schema=_SCHEMA_CONTEXT,
            question=question,
        )
        response_text = self._call_gemini(prompt)
        print("Gemini raw response:", response_text)
        sql = self._extract_sql(response_text)
        if sql:
            sql = self._sanitize_sql(sql)
        return sql

    def _run_query(self, sql: str) -> tuple[list[dict], list[str]]:
        """Execute *sql* and return (rows-as-dicts, column-names)."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.execute(sql)
            rows = [dict(r) for r in cur.fetchall()]
            columns = [d[0] for d in cur.description] if cur.description else []
            conn.close()
            return rows, columns
        except sqlite3.Error as exc:
            print(f"SQL execution error: {exc}\nSQL was: {sql}")
            return None, None  # signals a hard failure to answer()

    def _narrate(
        self,
        question: str,
        sql: str,
        rows: list[dict],
        columns: list[str],
    ) -> str:
        """Ask Gemini to turn the raw query results into readable prose."""
        if not rows:
            result_text = "The query returned no rows."
        elif len(rows) > 50:
            # Summarise large result sets instead of dumping them all
            sample = rows[:10]
            result_text = (
                f"Query returned {len(rows)} rows. First 10 rows:\n"
                + self._rows_to_text(sample, columns)
            )
        else:
            result_text = self._rows_to_text(rows, columns)

        prompt = _NARRATION_PROMPT_TEMPLATE.format(
            question=question,
            sql=sql,
            results=result_text,
        )
        return self._call_gemini(prompt)

    @staticmethod
    def _sanitize_natural_language_answer(text: str) -> str:
        """
        Reject SQL-like / code-fenced outputs from the LLM.
        This keeps /api/chat responses as natural language.
        """
        t = (text or "").strip()
        if not t:
            return ""

        # Drop code fences if the model produced them.
        if "```" in t:
            t = t.replace("```", "").strip()

        # Dataset currency is INR; avoid $ symbol that LLMs sometimes insert.
        # Replace "$123" -> "INR 123"
        t = re.sub(r"\$(\s*\d)", r"INR \1", t)
        t = t.replace(" $", " INR ")

        # If it looks like the model returned SQL (common failure mode), reject it.
        if re.search(r"(?is)^\s*select\s", t):
            return ""
        if re.search(r"(?is)\bselect\b.*\bfrom\b", t):
            return ""

        return t

    # ── Groq API call ──────────────────────────────────────────────────────

    @staticmethod
    def _call_gemini(prompt: str) -> str:
        """Send *prompt* to Groq and return the response text."""
        try:
            client = _get_model()
            response = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
            )
            return response.choices[0].message.content or ""
        except EnvironmentError as exc:
            print(f"Groq config error: {exc}")
            return ""
        except Exception as exc:
            print(f"Groq API error: {exc}")
            return ""

    # ── Utilities ──────────────────────────────────────────────────────────

    @staticmethod
    def _extract_sql(text: str) -> str | None:
        """
        Extract a SQL SELECT statement from *text*.

        Tries three strategies in order:
        1. ```sql ... ``` fenced block  (Gemini sometimes ignores the no-markdown rule)
        2. Bare SELECT from first occurrence to end of string
        3. Return None — caller handles the failure
        """
        import re

        if not text or not text.strip():
            return None

        # Strategy 1: fenced ```sql ... ``` block
        match = re.search(r"```sql\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # Strategy 2: extract from the first SELECT to the end of the string
        # This handles clean responses ("SELECT ...") and responses where Gemini
        # prepends a short sentence before the query.
        match = re.search(r"(SELECT\b.+)", text, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()

        return None

    @staticmethod
    def _sanitize_sql(sql: str) -> str | None:
        """
        Guardrail for LLM-generated SQL:
        - allow only a single SELECT statement
        - reject common write/DCL keywords
        """
        if not sql:
            return None
        t = sql.strip()
        # Only keep the first statement (prevents "SELECT ...; DROP ..." attacks).
        t = t.split(";", 1)[0].strip()

        if not re.match(r"(?is)^select\b", t):
            return None

        banned = r"(?is)\b(insert|update|delete|drop|alter|create|truncate|replace|grant|revoke)\b"
        if re.search(banned, t):
            return None

        return t

    @staticmethod
    def _is_domain_question(question: str) -> bool:
        """
        Guardrail: allow only prompts clearly about the O2C dataset/domain.

        We intentionally use simple deterministic checks so unrelated prompts
        are blocked before any LLM call.
        """
        q = (question or "").strip().lower()
        if not q:
            return False

        domain_terms = {
            "o2c",
            "order",
            "orders",
            "order-to-cash",
            "cash",
            "customer",
            "customers",
            "product",
            "products",
            "delivery",
            "deliveries",
            "billing",
            "invoice",
            "invoices",
            "payment",
            "payments",
            "journal",
            "gl",
            "accounting",
            "amount",
            "revenue",
            "billed",
            "cancelled",
            "sqlite",
            "dataset",
            "data",
        }

        if any(term in q for term in domain_terms):
            return True

        # Common dataset-style IDs in this project are mostly long numerics.
        if re.search(r"\b\d{6,}\b", q):
            return True

        return False

    @staticmethod
    def _entity_type_to_keyword(entity_type: str) -> str:
        mapping = {
            "customer": "customer",
            "order": "order",
            "delivery": "delivery",
            "billing": "billing",
            "payment": "payment",
            "product": "product",
        }
        return mapping.get(entity_type, "entity")

    def _extract_entity_context(self, question: str) -> dict[str, str] | None:
        """
        Extract a single (entity_type, normalized_id) from a question.
        Used for conservative conversation memory.
        """
        q = (question or "").lower()
        patterns: list[tuple[str, str]] = [
            ("customer", r"\bcustomer(?:s)?\s*(0*\d{6,12})\b"),
            ("order", r"\border(?:s)?\s*(0*\d{6,12})\b"),
            ("delivery", r"\bdelivery(?:s)?\s*(0*\d{6,12})\b"),
            ("billing", r"\b(?:billing|invoice|invoices)\s*(0*\d{6,12})\b"),
            ("payment", r"\bpayment(?:s)?\s*(0*\d{6,12})\b"),
            ("product", r"\bproduct(?:s)?\s*(0*\d{6,12})\b"),
        ]
        for entity_type, pat in patterns:
            m = re.search(pat, q)
            if m:
                raw_id = m.group(1)
                nid = self._normalize_id(raw_id)
                if nid:
                    return {"type": entity_type, "id": nid}
        return None

    @staticmethod
    def _looks_like_followup(question: str) -> bool:
        """Heuristic: pronoun-based follow-ups that likely refer to the previous entity."""
        q = (question or "").lower()
        return bool(
            re.search(
                r"\b(it|its|this|that|these|those|previous|above|mentioned)\b",
                q,
            )
        )

    @staticmethod
    def _prune_session_ctx_impl() -> None:
        now = time.time()
        to_del: list[str] = []
        for sid, ctx in _SESSION_CTX.items():
            if now - float(ctx.get("last_seen", now)) > _SESSION_CTX_MAX_AGE_S:
                to_del.append(sid)
        for sid in to_del:
            _SESSION_CTX.pop(sid, None)

    def _prune_session_ctx(self) -> None:
        self._prune_session_ctx_impl()
        if len(_SESSION_CTX) > _SESSION_CTX_MAX_SIZE:
            # crude eviction: drop oldest `last_seen`
            oldest = sorted(_SESSION_CTX.items(), key=lambda kv: float(kv[1].get("last_seen", 0)))
            for sid, _ in oldest[: len(_SESSION_CTX) - _SESSION_CTX_MAX_SIZE]:
                _SESSION_CTX.pop(sid, None)

    def _get_session_ctx(self, session_id: str) -> dict[str, str] | None:
        sid = (session_id or "").strip()
        if not sid:
            return None
        ctx = _SESSION_CTX.get(sid)
        if not ctx:
            return None
        ctx["last_seen"] = time.time()
        return {"type": ctx["type"], "id": ctx["id"]}

    def _set_session_ctx(self, session_id: str, entity_ctx: dict[str, str]) -> None:
        sid = (session_id or "").strip()
        if not sid:
            return
        _SESSION_CTX[sid] = {
            "type": entity_ctx["type"],
            "id": entity_ctx["id"],
            "last_seen": time.time(),
        }

    def _clear_session_ctx(self, session_id: str) -> None:
        sid = (session_id or "").strip()
        if not sid:
            return
        _SESSION_CTX.pop(sid, None)

    @staticmethod
    def _is_id_lookup_question(question: str) -> bool:
        """
        Detect explicit "direct lookup" prompts that start with an entity keyword.
        This avoids triggering direct lookup for follow-ups that rely on memory/context.
        """
        q = (question or "").strip().lower()
        return bool(
            re.search(
                r"^(?:tell|show|give|details|detail|about|for)?\s*"
                r"(customer|order|delivery|billing|invoice|payment|product|document)\s*0*\d{6,12}\b",
                q,
            )
        )

    @staticmethod
    def _rows_to_text(rows: list[dict], columns: list[str]) -> str:
        """Format rows as a compact readable string for the narration prompt."""
        if not rows:
            return "(no rows)"
        lines = [", ".join(columns)]
        for row in rows:
            lines.append(", ".join(str(row.get(c, "")) for c in columns))
        return "\n".join(lines)

    def _lookup_ids_direct(self, question: str) -> str | None:
        """
        Deterministic fallback for ID-centric questions.

        Used when LLM is unavailable/fails, so numeric-id queries still work.
        """
        ids = self._extract_candidate_ids(question)
        if not ids:
            return None

        summaries: list[str] = []
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            for entity_id in ids[:5]:
                summary = self._lookup_one_id(conn, entity_id, question)
                if summary:
                    summaries.append(summary)

        if not summaries:
            return None
        return " | ".join(summaries)

    def _rule_based_answer(self, question: str) -> str | None:
        """
        Limited deterministic NL->SQL fallback when GROQ_API_KEY is not set.
        This keeps the demo usable without an external LLM.
        """
        q = (question or "").strip().lower()
        if not q:
            return None

        # Helper: clamp numeric values parsed from text.
        def _clamp_int(value: str | None, default: int, lo: int, hi: int) -> int:
            try:
                n = int(value) if value is not None else default
            except ValueError:
                n = default
            return max(lo, min(hi, n))

        # Top products by ordered quantity (commonly asked by the UI examples).
        if ("top" in q) and ("product" in q) and any(w in q for w in ["quantity", "qty", "ordered"]):
            limit = _clamp_int(re.search(r"\btop\s+(\d+)\b", q).group(1) if re.search(r"\btop\s+(\d+)\b", q) else None, 10, 1, 25)
            sql = f"""
                SELECT
                    COALESCE(pd.name, p.id) AS product_name,
                    SUM(oi.quantity) AS total_ordered_qty,
                    SUM(oi.net_amount) AS total_revenue_INR
                FROM order_items oi
                JOIN products p ON p.id = oi.product_id
                LEFT JOIN product_descriptions pd
                    ON pd.product_id = p.id AND pd.language = 'EN'
                WHERE oi.product_id IS NOT NULL
                GROUP BY p.id, COALESCE(pd.name, p.id)
                ORDER BY total_ordered_qty DESC
                LIMIT {limit}
            """
            rows, _ = self._run_query(sql)
            if rows is None or not rows:
                return "I couldn't find matching product/order data for that question."
            lines: list[str] = []
            for i, r in enumerate(rows[:limit]):
                name = r.get("product_name") or r.get("product_id") or "Unknown product"
                qty = r.get("total_ordered_qty") or 0
                rev = r.get("total_revenue_INR") or 0
                try:
                    qty_f = float(qty)
                    rev_f = float(rev)
                except (TypeError, ValueError):
                    qty_f = 0.0
                    rev_f = 0.0
                lines.append(f"{i+1}. {name}: {qty_f:.2f} units, revenue INR {rev_f:.2f}")
            return "\n".join(lines)

        # Revenue by customer excluding cancelled invoices.
        if ("revenue" in q) and any(w in q for w in ["customer", "customers"]) and ("cancel" in q or "cancelled" in q):
            limit = _clamp_int(re.search(r"\btop\s+(\d+)\b", q).group(1) if re.search(r"\btop\s+(\d+)\b", q) else None, 10, 1, 25)
            sql = f"""
                SELECT
                    c.name AS customer_name,
                    COALESCE(ROUND(SUM(b.total_net_amount), 2), 0) AS total_billed_INR
                FROM billing b
                JOIN customers c ON c.id = b.customer_id
                WHERE b.is_cancelled = 0
                GROUP BY c.id, c.name
                ORDER BY total_billed_INR DESC
                LIMIT {limit}
            """
            rows, _ = self._run_query(sql)
            if rows is None or not rows:
                return "I couldn't find matching billing data for that question."
            lines: list[str] = []
            for i, r in enumerate(rows[:limit]):
                name = r.get("customer_name") or "Unknown customer"
                amount = r.get("total_billed_INR") or 0
                try:
                    amt_f = float(amount)
                except (TypeError, ValueError):
                    amt_f = 0.0
                lines.append(f"{i+1}. {name}: total billed revenue INR {amt_f:.2f}")
            return "\n".join(lines)

        # Outstanding invoices (no linked payments).
        if ("invoice" in q or "invoices" in q) and (
            ("outstanding" in q or "open" in q) or ("not" in q and ("paid" in q or "payment" in q))
        ):
            limit = _clamp_int(re.search(r"\b(\d+)\b", q).group(1) if re.search(r"\b(\d+)\b", q) else None, 20, 1, 50)
            sql = f"""
                SELECT
                    b.id AS invoice_id,
                    c.name AS customer_name,
                    b.billing_date,
                    COALESCE(ROUND(b.total_net_amount, 2), 0) AS invoice_amount
                FROM billing b
                JOIN customers c ON c.id = b.customer_id
                WHERE b.is_cancelled = 0
                  AND NOT EXISTS (
                    SELECT 1 FROM payments p
                    WHERE p.clearing_document = b.accounting_document
                  )
                ORDER BY b.billing_date DESC
                LIMIT {limit}
            """
            rows, _ = self._run_query(sql)
            if rows is None or not rows:
                return "No outstanding (unpaid) invoices were found in the dataset."
            # Keep it concise for the demo.
            lines = []
            for i, r in enumerate(rows[:10]):
                inv_id = r.get("invoice_id") or "Unknown invoice"
                cust = r.get("customer_name") or "Unknown customer"
                date = (r.get("billing_date") or "")[:10] or "unknown date"
                amt = r.get("invoice_amount") or 0
                try:
                    amt_f = float(amt)
                except (TypeError, ValueError):
                    amt_f = 0.0
                lines.append(f"{i+1}. Invoice {inv_id} ({cust}) billed on {date} — INR {amt_f:.2f}")
            return "\n".join(lines)

        # Orders delivered but not yet billed.
        if ("delivered" in q) and ("not" in q) and ("billed" in q or "billing" in q):
            limit = _clamp_int(re.search(r"\b(\d+)\b", q).group(1) if re.search(r"\b(\d+)\b", q) else None, 20, 1, 50)
            sql = f"""
                SELECT
                    o.id AS order_id,
                    c.name AS customer_name,
                    o.creation_date AS order_date,
                    (
                        SELECT COUNT(*)
                        FROM deliveries d
                        WHERE d.order_id = o.id
                    ) AS delivery_count
                FROM orders o
                JOIN customers c ON c.id = o.customer_id
                WHERE EXISTS (SELECT 1 FROM deliveries d WHERE d.order_id = o.id)
                  AND NOT EXISTS (
                    SELECT 1
                    FROM billing b
                    JOIN deliveries d2 ON d2.id = b.delivery_id
                    WHERE d2.order_id = o.id
                  )
                ORDER BY o.creation_date DESC
                LIMIT {limit}
            """
            rows, _ = self._run_query(sql)
            if rows is None or not rows:
                return "I couldn't find orders that are delivered but not yet billed."
            lines: list[str] = []
            for i, r in enumerate(rows[:10]):
                oid = r.get("order_id") or "Unknown order"
                cust = r.get("customer_name") or "Unknown customer"
                date = (r.get("order_date") or "")[:10] or "unknown date"
                dc = r.get("delivery_count") or 0
                try:
                    dc_f = float(dc)
                except (TypeError, ValueError):
                    dc_f = 0.0
                lines.append(f"{i+1}. Order {oid} ({cust}) created {date} — {int(dc_f)} delivery(ies) not yet billed")
            return "\n".join(lines)

        # Journal entries for billing (recent billing documents).
        if ("journal" in q) and ("billing" in q or "invoice" in q):
            billing_limit_m = re.search(r"\b(\d+)\b.*\b(billing|invoices|documents)\b", q)
            billing_limit = _clamp_int(billing_limit_m.group(1) if billing_limit_m else None, 10, 1, 20)
            outer_limit = _clamp_int(None, 120, 20, 300)
            sql = f"""
                SELECT
                    b.id AS billing_id,
                    b.billing_date AS billing_date,
                    je.gl_account AS gl_account,
                    COALESCE(ROUND(je.amount, 2), 0) AS journal_amount,
                    je.posting_date AS posting_date
                FROM journal_entries je
                JOIN billing b ON je.billing_document = b.id
                WHERE b.id IN (
                    SELECT id FROM billing ORDER BY billing_date DESC LIMIT {billing_limit}
                )
                ORDER BY b.billing_date DESC, je.posting_date DESC
                LIMIT {outer_limit}
            """
            rows, _ = self._run_query(sql)
            if rows is None or not rows:
                return "I couldn't find journal entries for recent billing documents."
            # Group by billing_id, preserve ordering by billing_date.
            by_billing: dict[str, list[dict]] = {}
            order: list[str] = []
            for r in rows:
                bid = r.get("billing_id")
                if not bid:
                    continue
                if bid not in by_billing:
                    by_billing[bid] = []
                    order.append(bid)
                if len(by_billing[bid]) < 5:
                    by_billing[bid].append(r)

            lines: list[str] = []
            for i, bid in enumerate(order[:billing_limit]):
                entries = by_billing.get(bid, [])
                date = (entries[0].get("billing_date") or "")[:10] if entries else "unknown date"
                lines.append(f"{i+1}. Billing {bid} (billed {date})")
                for e in entries[:5]:
                    gl = e.get("gl_account") or "GL ?"
                    amt = e.get("journal_amount") or 0
                    try:
                        amt_f = float(amt)
                    except (TypeError, ValueError):
                        amt_f = 0.0
                    pdate = (e.get("posting_date") or "")[:10] or "unknown posting date"
                    lines.append(f"   - {pdate}: {gl} -> INR {amt_f:.2f}")
            return "\n".join(lines)

        return None

    @staticmethod
    def _normalize_id(id_str: str) -> str:
        """Normalize SAP IDs by stripping leading zeros (e.g. 0740598 -> 740598)."""
        s = (id_str or "").strip()
        if not s:
            return ""
        stripped = s.lstrip("0")
        return stripped if stripped else "0"

    def _normalize_ids_in_text(self, text: str) -> str:
        """Strip leading zeros for any 6–12 digit numeric tokens in the text."""
        t = text or ""

        def repl(m: re.Match) -> str:
            nid = self._normalize_id(m.group(0))
            return nid if nid else m.group(0)

        return re.sub(r"\b\d{6,12}\b", repl, t)

    @staticmethod
    def _extract_candidate_ids(question: str) -> list[str]:
        raw_ids = re.findall(r"\b\d{6,12}\b", question or "")
        # preserve order, remove duplicates after normalization
        out: list[str] = []
        seen: set[str] = set()
        for rid in raw_ids:
            nid = SqlAssistant._normalize_id(rid)
            if not nid:
                continue
            if nid not in seen:
                seen.add(nid)
                out.append(nid)
        return out

    
    def _lookup_one_id(self, conn: sqlite3.Connection, entity_id: str, question: str | None = None) -> str | None:
        customer = conn.execute(
            "SELECT id, name FROM customers WHERE id = ?",
            (entity_id,),
        ).fetchone()
        if customer:
            order_count = conn.execute(
                "SELECT COUNT(*) AS c FROM orders WHERE customer_id = ?",
                (entity_id,),
            ).fetchone()["c"]
            billed = conn.execute(
                "SELECT COALESCE(ROUND(SUM(total_net_amount),2), 0) AS s "
                "FROM billing WHERE customer_id = ? AND is_cancelled = 0",
                (entity_id,),
            ).fetchone()["s"]
            return (
                f"Customer {customer['name']} (ID {entity_id}) has {order_count} order(s). "
                f"Total billed revenue (excluding cancellations) is INR {billed:.2f}."
            )

        order = conn.execute(
            "SELECT id, customer_id, creation_date, total_net_amount, currency, delivery_status "
            "FROM orders WHERE id = ?",
            (entity_id,),
        ).fetchone()
        if order:
            order_customer = conn.execute(
                "SELECT id, name FROM customers WHERE id = ?",
                (order["customer_id"],),
            ).fetchone()
            item_count = conn.execute(
                "SELECT COUNT(*) AS c FROM order_items WHERE order_id = ?",
                (entity_id,),
            ).fetchone()["c"]
            amount = order["total_net_amount"]
            amount_text = f"{order['currency']} {amount:.2f}" if amount is not None else ""
            status = order["delivery_status"] or "unknown"
            created_on = (order["creation_date"] or "")[:10] or "unknown date"
            if order_customer:
                customer_text = f"{order_customer['name']} (ID {order_customer['id']})"
            else:
                customer_text = f"customer ID {order['customer_id']}"
            return (
                f"Order {entity_id} was created on {created_on}. "
                f"It belongs to {customer_text}. "
                f"Total net amount is {amount_text}. "
                f"Delivery status is {status}. "
                f"It contains {item_count} line item(s)."
            )

        delivery = conn.execute(
            "SELECT id, order_id, creation_date, goods_movement_status, picking_status "
            "FROM deliveries WHERE id = ?",
            (entity_id,),
        ).fetchone()
        if delivery:
            delivery_created_on = (delivery["creation_date"] or "")[:10] or "unknown date"
            return (
                f"Delivery {entity_id} belongs to order {delivery['order_id']}. "
                f"It was created on {delivery_created_on}. "
                f"Goods movement status is {delivery['goods_movement_status']}. "
                f"Picking status is {delivery['picking_status']}."
            )

        billing = conn.execute(
            "SELECT id, delivery_id, customer_id, billing_date, total_net_amount, currency, "
            "is_cancelled, accounting_document "
            "FROM billing WHERE id = ?",
            (entity_id,),
        ).fetchone()
        # 🔥 Journal entrie
        if billing:
            journal = conn.execute(
                 """
                SELECT gl_account, amount, posting_date
                FROM journal_entries
                WHERE billing_document = ?
                """,
                (entity_id,),
            ).fetchall()

            if journal and question and "journal" in question.lower():
                unique_entries = list({
                    (j['gl_account'], j['amount'])
                    for j in journal
                })
                entries = ", ".join(
                    f"GL {j['gl_account']} → INR {j['amount']:.2f}"
                    for j in journal[:5]
                )
                return f"Journal entries for billing {entity_id}: {entries}"

            payment_count = conn.execute(
                "SELECT COUNT(*) AS c FROM payments WHERE clearing_document = ?",
                (billing["accounting_document"],),
            ).fetchone()["c"]
            cancelled = "yes" if billing["is_cancelled"] else "no"
            billing_customer = conn.execute(
                "SELECT id, name FROM customers WHERE id = ?",
                (billing["customer_id"],),
            ).fetchone()
            customer_text = (
                f"{billing_customer['name']} (ID {billing_customer['id']})"
                if billing_customer
                else f"customer ID {billing['customer_id']}"
            )
            amount = billing["total_net_amount"]
            amount_text = f"{billing['currency']} {amount:.2f}" if amount is not None else ""
            billed_on = (billing["billing_date"] or "")[:10] or "unknown date"
            return (
                f"Invoice {entity_id} was billed on {billed_on}. "
                f"It is for delivery {billing['delivery_id']}. "
                f"It belongs to {customer_text}. "
                f"Invoice amount is {amount_text}. "
                f"Cancelled: {cancelled}. "
                f"It has {payment_count} linked payment(s)."
            )

        payment = conn.execute(
            "SELECT id, customer_id, amount, currency, posting_date, clearing_date, clearing_document "
            "FROM payments WHERE id = ?",
            (entity_id,),
        ).fetchone()
        if payment:
            payment_customer = conn.execute(
                "SELECT id, name FROM customers WHERE id = ?",
                (payment["customer_id"],),
            ).fetchone()
            customer_text = (
                f"{payment_customer['name']} (ID {payment_customer['id']})"
                if payment_customer
                else f"customer ID {payment['customer_id']}"
            )
            amount = payment["amount"]
            amount_text = f"{payment['currency']} {amount:.2f}" if amount is not None else ""
            posted_on = (payment["posting_date"] or "")[:10] or "unknown date"
            cleared_on = (payment["clearing_date"] or "")[:10] or "not cleared"
            return (
                f"Payment {entity_id} was posted on {posted_on}. "
                f"It belongs to {customer_text}. "
                f"Amount is {amount_text}. "
                f"Cleared on: {cleared_on}. "
                f"Clearing document is {payment['clearing_document']}."
            )

        product = conn.execute(
            "SELECT p.id, COALESCE(pd.name, p.id) AS name "
            "FROM products p "
            "LEFT JOIN product_descriptions pd ON pd.product_id = p.id AND pd.language = 'EN' "
            "WHERE p.id = ?",
            (entity_id,),
        ).fetchone()
        if product:
            qty_row = conn.execute(
                "SELECT COALESCE(SUM(quantity), 0) AS q, COALESCE(ROUND(SUM(net_amount),2), 0) AS r "
                "FROM order_items WHERE product_id = ?",
                (entity_id,),
            ).fetchone()
            return (
                f"Product {product['name']} (ID {entity_id}) has total ordered quantity of {qty_row['q']}. "
                f"Total revenue from order items is INR {qty_row['r']:.2f}."
            )

        return None


# Module-level assistant instance (re-used across requests)
_assistant = SqlAssistant(DB_PATH)


# ── Pydantic models ────────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    message: str
    session_id: str | None = None


class ChatResponse(BaseModel):
    answer: str


class GraphNode(BaseModel):
    id: str
    type: str   # "customer" | "order" | "product" | "delivery" | "billing" | "payment"
    label: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class GraphEdge(BaseModel):
    source: str
    target: str
    relation: str


class GraphResponse(BaseModel):
    nodes: list[GraphNode]
    edges: list[GraphEdge]


class NodeMeta(BaseModel):
    id: str
    type: str
    label: str
    properties: dict[str, Any]


class ExampleQuery(BaseModel):
    title: str
    question: str
    description: str


class ExamplesResponse(BaseModel):
    examples: list[ExampleQuery]


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.post("/api/chat", response_model=ChatResponse)
def chat(request: ChatRequest) -> ChatResponse:
    """
    Accept a natural-language question and return a plain-English answer
    backed by a live SQL query against data.db.
    """
    if not request.message.strip():
        raise HTTPException(status_code=400, detail="message must not be empty")

    try:
        answer = _assistant.answer(request.message.strip(), session_id=request.session_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return ChatResponse(answer=answer)


@app.get("/api/graph", response_model=GraphResponse)
def graph() -> GraphResponse:
    """
    Seed view: return only customer nodes.
    Frontend expands from here using /api/graph/expand/{node_id}.
    """
    with get_db() as conn:
        rows = conn.execute("SELECT id, name FROM customers ORDER BY name LIMIT 12").fetchall()

    nodes = [
        GraphNode(
            id=r["id"],
            type="customer",
            label=r["name"],
            metadata={"entity": "customer", "customer_id": r["id"], "customer_name": r["name"]},
        )
        for r in rows
    ]
    return GraphResponse(nodes=nodes, edges=[])


@app.get("/api/graph/node/{node_id}", response_model=NodeMeta)
def node_detail(node_id: str) -> NodeMeta:
    """Return detailed metadata for one node."""
    with get_db() as conn:
        if node_id.startswith("cca:"):
            _, customer_id, company_code = node_id.split(":", 2)
            row = conn.execute(
                """
                SELECT customer_id, company_code, reconciliation_account, payment_terms,
                       customer_account_group, deletion_indicator
                FROM customer_company_assignments
                WHERE customer_id = ? AND company_code = ?
                """,
                (customer_id, company_code),
            ).fetchone()
            if row:
                return NodeMeta(
                    id=node_id,
                    type="customer_company",
                    label=f"Company {company_code}",
                    properties={
                        "customer_id": row["customer_id"],
                        "company_code": row["company_code"],
                        "reconciliation_account": row["reconciliation_account"],
                        "payment_terms": row["payment_terms"],
                        "customer_account_group": row["customer_account_group"],
                        "deletion_indicator": bool(row["deletion_indicator"]),
                    },
                )

        if node_id.startswith("csa:"):
            _, customer_id, sales_org, dist, division = node_id.split(":", 4)
            row = conn.execute(
                """
                SELECT customer_id, sales_organization, distribution_channel, division,
                       currency, customer_payment_terms, delivery_priority,
                       incoterms_classification, incoterms_location1, shipping_condition
                FROM customer_sales_area_assignments
                WHERE customer_id = ? AND sales_organization = ? AND distribution_channel = ? AND division = ?
                """,
                (customer_id, sales_org, dist, division),
            ).fetchone()
            if row:
                return NodeMeta(
                    id=node_id,
                    type="sales_area",
                    label=f"{sales_org}/{dist}/{division}",
                    properties={
                        "customer_id": row["customer_id"],
                        "sales_organization": row["sales_organization"],
                        "distribution_channel": row["distribution_channel"],
                        "division": row["division"],
                        "currency": row["currency"],
                        "payment_terms": row["customer_payment_terms"],
                        "delivery_priority": row["delivery_priority"],
                        "incoterms": row["incoterms_classification"],
                        "incoterms_location": row["incoterms_location1"],
                        "shipping_condition": row["shipping_condition"],
                    },
                )

        if node_id.startswith("addr:"):
            _, customer_id, address_id = node_id.split(":", 2)
            row = conn.execute(
                """
                SELECT business_partner_id, address_id, city_name, country, postal_code,
                       region, street_name, address_time_zone
                FROM business_partner_addresses
                WHERE business_partner_id = ? AND address_id = ?
                """,
                (customer_id, address_id),
            ).fetchone()
            if row:
                return NodeMeta(
                    id=node_id,
                    type="address",
                    label=f"Address {address_id}",
                    properties={
                        "customer_id": row["business_partner_id"],
                        "address_id": row["address_id"],
                        "street": row["street_name"],
                        "city": row["city_name"],
                        "region": row["region"],
                        "country": row["country"],
                        "postal_code": row["postal_code"],
                        "time_zone": row["address_time_zone"],
                    },
                )

        if node_id.startswith("plant:"):
            plant_id = node_id.split(":", 1)[1]
            row = conn.execute(
                """
                SELECT plant_id, plant_name, sales_organization, distribution_channel, division
                FROM plants WHERE plant_id = ?
                """,
                (plant_id,),
            ).fetchone()
            if row:
                products = conn.execute(
                    "SELECT COUNT(*) FROM product_plants WHERE plant_id = ?",
                    (plant_id,),
                ).fetchone()[0]
                return NodeMeta(
                    id=node_id,
                    type="plant",
                    label=row["plant_name"] or f"Plant {plant_id}",
                    properties={
                        "plant_id": row["plant_id"],
                        "sales_organization": row["sales_organization"],
                        "distribution_channel": row["distribution_channel"],
                        "division": row["division"],
                        "products_mapped": products,
                    },
                )

        if node_id.startswith("sch:"):
            _, sales_order_id, sales_order_item_id, schedule_line = node_id.split(":", 3)
            row = conn.execute(
                """
                SELECT sales_order_id, sales_order_item_id, schedule_line,
                       confirmed_delivery_date, order_quantity_unit, confirmed_order_qty
                FROM sales_order_schedule_lines
                WHERE sales_order_id = ? AND sales_order_item_id = ? AND schedule_line = ?
                """,
                (sales_order_id, sales_order_item_id, schedule_line),
            ).fetchone()
            if row:
                return NodeMeta(
                    id=node_id,
                    type="schedule_line",
                    label=f"Schedule {sales_order_item_id}/{schedule_line}",
                    properties={
                        "sales_order_id": row["sales_order_id"],
                        "sales_order_item_id": row["sales_order_item_id"],
                        "schedule_line": row["schedule_line"],
                        "confirmed_delivery_date": (row["confirmed_delivery_date"] or "")[:10],
                        "order_quantity_unit": row["order_quantity_unit"],
                        "confirmed_order_qty": row["confirmed_order_qty"],
                    },
                )

        row = conn.execute("SELECT id, name FROM customers WHERE id = ?", (node_id,)).fetchone()
        if row:
            orders = conn.execute(
                "SELECT COUNT(*) FROM orders WHERE customer_id = ?",
                (node_id,),
            ).fetchone()[0]
            billed = conn.execute(
                "SELECT ROUND(SUM(total_net_amount),2) FROM billing WHERE customer_id = ? AND is_cancelled = 0",
                (node_id,),
            ).fetchone()[0]
            return NodeMeta(
                id=node_id,
                type="customer",
                label=row["name"],
                properties={"orders": orders, "total_billed_INR": billed or 0},
            )

        row = conn.execute(
            "SELECT id, customer_id, creation_date, total_net_amount, currency, delivery_status "
            "FROM orders WHERE id = ?",
            (node_id,),
        ).fetchone()
        if row:
            items = conn.execute(
                "SELECT COUNT(*) FROM order_items WHERE order_id = ?",
                (node_id,),
            ).fetchone()[0]
            return NodeMeta(
                id=node_id,
                type="order",
                label=f"Order {node_id}",
                properties={
                    "customer_id": row["customer_id"],
                    "date": (row["creation_date"] or "")[:10],
                    "net_amount": row["total_net_amount"],
                    "currency": row["currency"],
                    "delivery_status": row["delivery_status"],
                    "line_items": items,
                },
            )

        row = conn.execute(
            "SELECT oi.product_id, pd.name, SUM(oi.quantity) as qty, SUM(oi.net_amount) as revenue "
            "FROM order_items oi "
            "LEFT JOIN product_descriptions pd ON pd.product_id = oi.product_id AND pd.language = 'EN' "
            "WHERE oi.product_id = ? GROUP BY oi.product_id",
            (node_id,),
        ).fetchone()
        if row:
            return NodeMeta(
                id=node_id,
                type="product",
                label=row["name"] or node_id,
                properties={
                    "total_qty_ordered": row["qty"],
                    "total_revenue_INR": round(row["revenue"] or 0, 2),
                },
            )

        row = conn.execute(
            "SELECT id, order_id, creation_date, goods_movement_status, picking_status "
            "FROM deliveries WHERE id = ?",
            (node_id,),
        ).fetchone()
        if row:
            return NodeMeta(
                id=node_id,
                type="delivery",
                label=f"Delivery {node_id}",
                properties={
                    "order_id": row["order_id"],
                    "date": (row["creation_date"] or "")[:10],
                    "goods_movement": row["goods_movement_status"],
                    "picking": row["picking_status"],
                },
            )

        row = conn.execute(
            "SELECT id, billing_date, total_net_amount, currency, is_cancelled, accounting_document "
            "FROM billing WHERE id = ?",
            (node_id,),
        ).fetchone()
        if row:
            return NodeMeta(
                id=node_id,
                type="billing",
                label=f"Invoice {node_id}",
                properties={
                    "date": (row["billing_date"] or "")[:10],
                    "amount": row["total_net_amount"],
                    "currency": row["currency"],
                    "cancelled": bool(row["is_cancelled"]),
                    "accounting_doc": row["accounting_document"],
                },
            )

        row = conn.execute(
            "SELECT id, amount, currency, posting_date, clearing_date FROM payments WHERE id = ?",
            (node_id,),
        ).fetchone()
        if row:
            return NodeMeta(
                id=node_id,
                type="payment",
                label=f"Payment {node_id}",
                properties={
                    "amount": row["amount"],
                    "currency": row["currency"],
                    "posted": (row["posting_date"] or "")[:10],
                    "cleared": (row["clearing_date"] or "")[:10],
                },
            )

    raise HTTPException(status_code=404, detail=f"Node {node_id!r} not found")


@app.get("/api/graph/expand/{node_id}", response_model=GraphResponse)
def expand_node(node_id: str) -> GraphResponse:
    """Return immediate neighbors of a node for progressive graph expansion."""
    nodes: dict[str, GraphNode] = {}
    edges: list[GraphEdge] = []
    seen: set[tuple[str, str, str]] = set()

    def n(nid: str, ntype: str, label: str, metadata: dict[str, Any] | None = None) -> None:
        if nid and nid not in nodes:
            nodes[nid] = GraphNode(id=nid, type=ntype, label=label, metadata=metadata or {})

    def e(src: str, tgt: str, rel: str) -> None:
        if src and tgt:
            key = (src, tgt, rel)
            if key not in seen:
                seen.add(key)
                edges.append(GraphEdge(source=src, target=tgt, relation=rel))

    with get_db() as conn:
        row = conn.execute("SELECT id, name FROM customers WHERE id = ?", (node_id,)).fetchone()
        if row:
            orders = conn.execute(
                "SELECT id, total_net_amount, currency, creation_date FROM orders "
                "WHERE customer_id = ? ORDER BY creation_date DESC",
                (node_id,),
            ).fetchall()
            for o in orders:
                label = f"Order {o['id']} · {o['currency']} {o['total_net_amount']:.0f}"
                n(o["id"], "order", label, {"order_id": o["id"], "date": (o["creation_date"] or "")[:10]})
                e(node_id, o["id"], "places")

            companies = conn.execute(
                """
                SELECT customer_id, company_code, reconciliation_account, payment_terms
                FROM customer_company_assignments
                WHERE customer_id = ?
                """,
                (node_id,),
            ).fetchall()
            for c in companies:
                company_node_id = f"cca:{c['customer_id']}:{c['company_code']}"
                n(
                    company_node_id,
                    "customer_company",
                    f"Company {c['company_code']}",
                    {
                        "customer_id": c["customer_id"],
                        "company_code": c["company_code"],
                        "reconciliation_account": c["reconciliation_account"],
                        "payment_terms": c["payment_terms"],
                    },
                )
                e(node_id, company_node_id, "assigned_to_company")

            sales_areas = conn.execute(
                """
                SELECT customer_id, sales_organization, distribution_channel, division, currency
                FROM customer_sales_area_assignments
                WHERE customer_id = ?
                """,
                (node_id,),
            ).fetchall()
            for s in sales_areas:
                sales_node_id = (
                    f"csa:{s['customer_id']}:{s['sales_organization']}:"
                    f"{s['distribution_channel']}:{s['division']}"
                )
                n(
                    sales_node_id,
                    "sales_area",
                    f"{s['sales_organization']}/{s['distribution_channel']}/{s['division']}",
                    {
                        "customer_id": s["customer_id"],
                        "sales_organization": s["sales_organization"],
                        "distribution_channel": s["distribution_channel"],
                        "division": s["division"],
                        "currency": s["currency"],
                    },
                )
                e(node_id, sales_node_id, "belongs_to_sales_area")

            addresses = conn.execute(
                """
                SELECT business_partner_id, address_id, city_name, country, postal_code
                FROM business_partner_addresses
                WHERE business_partner_id = ?
                """,
                (node_id,),
            ).fetchall()
            for a in addresses:
                addr_node_id = f"addr:{a['business_partner_id']}:{a['address_id']}"
                label = f"{a['city_name'] or 'Address'} ({a['country'] or ''})".strip()
                n(
                    addr_node_id,
                    "address",
                    label,
                    {
                        "customer_id": a["business_partner_id"],
                        "address_id": a["address_id"],
                        "city": a["city_name"],
                        "country": a["country"],
                        "postal_code": a["postal_code"],
                    },
                )
                e(node_id, addr_node_id, "has_address")
            return GraphResponse(nodes=list(nodes.values()), edges=edges)

        row = conn.execute("SELECT id FROM orders WHERE id = ?", (node_id,)).fetchone()
        if row:
            items = conn.execute(
                "SELECT oi.product_id, COALESCE(pd.name, oi.product_id) AS pname "
                "FROM order_items oi "
                "LEFT JOIN product_descriptions pd ON pd.product_id = oi.product_id AND pd.language = 'EN' "
                "WHERE oi.order_id = ?",
                (node_id,),
            ).fetchall()
            for it in items:
                lbl = it["pname"] or it["product_id"]
                if len(lbl) > 28:
                    lbl = lbl[:25] + "..."
                n(it["product_id"], "product", lbl, {"product_id": it["product_id"]})
                e(node_id, it["product_id"], "contains")

            deliveries = conn.execute("SELECT id FROM deliveries WHERE order_id = ?", (node_id,)).fetchall()
            for d in deliveries:
                n(d["id"], "delivery", f"Delivery {d['id']}", {"delivery_id": d["id"]})
                e(node_id, d["id"], "fulfilled_by")

            schedules = conn.execute(
                """
                SELECT sales_order_item_id, schedule_line, confirmed_delivery_date, confirmed_order_qty
                FROM sales_order_schedule_lines
                WHERE sales_order_id = ?
                """,
                (node_id,),
            ).fetchall()
            for s in schedules:
                sid = f"sch:{node_id}:{s['sales_order_item_id']}:{s['schedule_line']}"
                n(
                    sid,
                    "schedule_line",
                    f"Schedule {s['sales_order_item_id']}/{s['schedule_line']}",
                    {
                        "sales_order_id": node_id,
                        "sales_order_item_id": s["sales_order_item_id"],
                        "schedule_line": s["schedule_line"],
                        "confirmed_delivery_date": (s["confirmed_delivery_date"] or "")[:10],
                        "confirmed_order_qty": s["confirmed_order_qty"],
                    },
                )
                e(node_id, sid, "has_schedule_line")
            return GraphResponse(nodes=list(nodes.values()), edges=edges)

        row = conn.execute("SELECT id FROM products WHERE id = ?", (node_id,)).fetchone()
        if row:
            plant_rows = conn.execute(
                """
                SELECT pp.plant_id, p.plant_name, pp.profit_center
                FROM product_plants pp
                LEFT JOIN plants p ON p.plant_id = pp.plant_id
                WHERE pp.product_id = ?
                """,
                (node_id,),
            ).fetchall()
            for p in plant_rows:
                pid = f"plant:{p['plant_id']}"
                n(
                    pid,
                    "plant",
                    p["plant_name"] or f"Plant {p['plant_id']}",
                    {
                        "plant_id": p["plant_id"],
                        "plant_name": p["plant_name"],
                        "profit_center": p["profit_center"],
                    },
                )
                e(node_id, pid, "available_at_plant")
            return GraphResponse(nodes=list(nodes.values()), edges=edges)

        row = conn.execute("SELECT id FROM deliveries WHERE id = ?", (node_id,)).fetchone()
        if row:
            bills = conn.execute(
                "SELECT id, total_net_amount, is_cancelled FROM billing WHERE delivery_id = ?",
                (node_id,),
            ).fetchall()
            for b in bills:
                prefix = "[Cancelled] " if b["is_cancelled"] else ""
                n(
                    b["id"],
                    "billing",
                    f"{prefix}Invoice {b['id']} · {b['total_net_amount']:.0f}",
                    {"billing_id": b["id"], "cancelled": bool(b["is_cancelled"])},
                )
                e(node_id, b["id"], "billed_as")
            return GraphResponse(nodes=list(nodes.values()), edges=edges)

        row = conn.execute(
            "SELECT id, accounting_document, customer_id FROM billing WHERE id = ?",
            (node_id,),
        ).fetchone()
        if row:
            pays = conn.execute(
                "SELECT id, amount, currency FROM payments WHERE clearing_document = ? "
                "OR (customer_id = ? AND clearing_document = ?)",
                (row["accounting_document"], row["customer_id"], row["accounting_document"]),
            ).fetchall()
            for p in pays:
                n(
                    p["id"],
                    "payment",
                    f"Payment · {p['currency']} {p['amount']:.0f}",
                    {"payment_id": p["id"], "amount": p["amount"], "currency": p["currency"]},
                )
                e(node_id, p["id"], "paid_by")
            return GraphResponse(nodes=list(nodes.values()), edges=edges)

    return GraphResponse(nodes=[], edges=[])


@app.get("/api/examples", response_model=ExamplesResponse)
def examples() -> ExamplesResponse:
    """Return curated example questions for the chat UI."""
    return ExamplesResponse(
        examples=[
            ExampleQuery(
                title="Top products",
                question="What are the top 10 products by total quantity ordered?",
                description="Ranks products by ordered quantity with revenue totals.",
            ),
            ExampleQuery(
                title="Trace billing document",
                question=(
                    "Show me the full order-to-cash chain for billing document 90504406: "
                    "which order, delivery, and payment is it linked to?"
                ),
                description="Traces one invoice back through delivery and order to the customer.",
            ),
            ExampleQuery(
                title="Orders not billed",
                question="Which orders have been delivered but not yet billed?",
                description="Finds fulfilment gaps where delivery exists but no invoice was raised.",
            ),
            ExampleQuery(
                title="Revenue by customer",
                question=(
                    "What is the total billed revenue per customer, "
                    "excluding cancelled invoices?"
                ),
                description="Summarises invoiced revenue grouped by sold-to party.",
            ),
            ExampleQuery(
                title="Outstanding invoices",
                question="Which invoices have not been paid yet?",
                description="Lists open receivables where no clearing payment exists.",
            ),
            ExampleQuery(
                title="Journal entries for billing",
                question=(
                    "Show me the journal entries and GL accounts posted "
                    "for the most recent 10 billing documents."
                ),
                description="Connects billing documents to their accounting postings.",
            ),
        ]
    )


# ── Static frontend ──────────────────────────────────────────────────────────
# Serve the built React app so the whole system can be deployed as a single service.
_static_dir = Path(__file__).resolve().parent.parent / "frontend" / "dist"
if _static_dir.exists():
    app.mount("/", StaticFiles(directory=str(_static_dir), html=True), name="frontend")


# ── Dev entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
