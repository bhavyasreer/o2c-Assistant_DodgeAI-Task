🚀 O2C AI — Graph-Based Order-to-Cash Intelligence System

📌 Overview

The Graph-Based Order-to-Cash (O2C) Intelligence System is a full-stack application that enables users to explore business transaction data using natural language queries and interactive graph visualization.

The system converts user questions into SQL queries, executes them on a structured O2C dataset, and returns data-grounded answers. It also provides a graph-based interface to visualize relationships between entities such as customers, orders, deliveries, billing documents, and payments.

Additionally, the system supports multi-turn conversational queries, allowing users to ask contextual follow-up questions.

---

✨ Key Features

- Natural language → SQL using LLM
- Graph-based data exploration (interactive UI)
- Conversational memory (multi-turn queries)
- Deterministic fallback for ID-based queries
- Hybrid AI + rule-based architecture
- Real-time query execution on structured data
- Guardrails for safe and accurate responses

---

🏗️ Architecture

Frontend

- React (Vite)
- React Flow for graph visualization
- Chat UI for natural language interaction

Backend

- FastAPI (Python)
- Handles query processing, SQL execution, LLM integration, and graph APIs

Database

- SQLite ("data.db")
- Contains normalized tables + analytical view ("v_order_to_cash")

LLM

- Groq API (LLaMA 3.3)

---
🔄 Request Flow

User → Frontend (Chat UI)
      ↓
Backend (FastAPI)
      ↓
[Decision Layer]
   → ID-based query → Direct SQL → DB
   → Analytical query → LLM → SQL → DB
      ↓
Result Processing
      ↓
LLM (Natural language response)
      ↓
Frontend (Chat + Graph highlight)

---

🧠 System Design Decisions

1. Hybrid Query System (LLM + Deterministic SQL)

Problem

- LLMs introduce latency and may hallucinate incorrect SQL
- Not all queries require AI-based reasoning

---

✅ Solution: Hybrid Query Optimization

The system dynamically selects execution strategy:

🔹 Deterministic Path (ID-based queries)

When queries include entity IDs (billing/order/delivery):

- Direct SQL lookup is executed
- LLM is used only for formatting the final response

---

🔹 LLM Path (Analytical queries)

For flexible queries:

- LLM generates SQL
- SQL is executed
- Results are converted to natural language

---

🔁 Execution Flow

User Query
   ↓
Decision Layer
   → ID detected → SQL → DB → LLM (formatting)
   → Else → LLM → SQL → DB → LLM (answer)

---

🎯 Why this approach?

- Faster responses
- Reduced LLM usage (cost-efficient)
- Higher accuracy for critical queries
- Better separation of concerns

---

2. Conversational Memory

Why

Users ask follow-up queries naturally.

Example

"Show billing 90678702"
"What about its journal entries?"

Design

- Context from previous queries is stored
- References like “its” are resolved

---

3. Graph-Based Representation

O2C is inherently relational:

Customer → Order → Delivery → Billing → Payment

Benefits

- Better understanding of relationships
- Interactive exploration
- Complements tabular results

---

4. Progressive Graph Expansion

Problem

Full graph → cluttered and unreadable

Solution

- Start with minimal nodes (customers)
- Expand dynamically on user interaction

---

5. Technology Choices

FastAPI

- High performance
- Easy API development
- Python ecosystem compatibility

React + Vite

- Fast UI development
- Component-based architecture

---

Trade-offs

Decision| Trade-off
SQLite| Simplicity vs scalability
LLM usage| Flexibility vs reliability
Graph UI| Usability vs complexity

---

🤖 LLM Prompting Strategy

Two-Stage Prompting

1. Natural Language → SQL

- Prompt includes:
  - schema description
  - table relationships
- Constraints:
  - output must be SQL only
  - must start with SELECT
  - no markdown formatting

---

2. SQL Result → Natural Language

- Input:
  - user question
  - SQL query
  - result rows
- Output:
  - concise
  - data-grounded
  - no hallucination

---

Hallucination Control

- temperature = 0
- schema-aware prompting
- SQL validation before execution

---

Failure Handling

- fallback SQL rules
- controlled error responses

---

🛡️ Guardrails & Reliability

- Domain restriction for queries
- Only SELECT queries allowed
- Blocks write operations (INSERT, UPDATE, DELETE, etc.)
- Output sanitization (removes SQL/code)
- Deterministic fallback for ID queries
- Graceful error handling

---

🗄️ Database Design

Why SQLite?

- Lightweight and portable
- No external dependency
- Easy deployment

---

Core Tables

- customers
- products
- orders
- order_items
- deliveries
- billing
- payments
- journal_entries

---

Analytical View

"v_order_to_cash"

Combines:

orders + items + deliveries + billing + payments

Purpose

- Simplifies queries
- Reduces LLM complexity
- Ensures consistent joins

---

Relationships

Customer → Orders  
Orders → Items → Products  
Orders → Deliveries  
Deliveries → Billing  
Billing → Payments  
Billing → Journal Entries  

---

⚙️ Tech Stack

Backend

- FastAPI (Python)
- Uvicorn

Frontend

- React (Vite)
- React Flow

Database

- SQLite

LLM

- Groq API (LLaMA 3.3)

Deployment

- Docker
- Render

---

🚀 Deployment

The application is containerized using Docker and deployed on Render.

Steps

- Build Docker image
- Deploy as Render Web Service
- Set environment variable:
  - "GROQ_API_KEY"

---

💬 Example Queries

- Show billing 90678702
- What about its journal entries?
- Top products by quantity
- Customers with highest revenue
- Which invoices are unpaid?
- Trace full flow for billing document 91150216

---

⚠️ Challenges & Solutions

1. LLM Hallucination

- Solved using hybrid system + guardrails

2. SQL Accuracy

- Solved using schema-aware prompting

3. Conversational Context

- Solved using memory tracking

4. Graph Complexity

- Solved using progressive expansion


📌 Conclusion

This system demonstrates a practical combination of:

Relational Databases + LLMs + Graph Visualization

By using a hybrid architecture, it achieves:

- High accuracy (SQL-based retrieval)
- Flexibility (LLM-powered queries)
- Usability (interactive graph UI)

---

👤 Author

Bhavya Sree
