# Lab 10: AI Model Inference and Streaming Agents

In this lab we integrate **Google Gemini** directly into Flink SQL pipelines using Confluent LLM integrations.

**Scenario:** A bank wants to flag risky transactions in real time with AI, and power a customer support agent that can look up live account data using a custom Java function.

The two primitives covered:

| Primitive | SQL | What it does |
|---|---|---|
| `ML_PREDICT` | `LATERAL TABLE(ML_PREDICT('model', ...))` | Enriches every event with a model prediction |
| `AI_RUN_AGENT` | `LATERAL TABLE(AI_RUN_AGENT('agent', ...))` | Runs an autonomous reasoning loop per event |

### Prerequisites

- `transactions_faker` from [Lab 1](./lab1.md)
- Confluent Cloud Flink compute pool with AI features enabled
- A Google AI API key — generate one at [aistudio.google.com/app/apikey](https://aistudio.google.com/app/apikey)
- Java 11 and Maven (for the UDF JAR in Part 2)
- Confluent CLI installed and logged in

---

## Part 1 — Real-time Fraud Detection with `ML_PREDICT`

### 0 — Create a Google AI Connection

A **connection** stores your provider credentials once and is reused across models.

```sql
CREATE CONNECTION gemini_conn
WITH (
  'type'    = 'googleai',
  'endpoint' = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:generateContent',
  'api-key' = '<your-google-ai-api-key>'
);
```

Verify it was created:

```sql
SHOW CONNECTIONS;
```

---

### 1 — Register a Fraud Detection Model

Register Gemini as a Flink SQL model. The system prompt turns the general-purpose LLM into a purpose-built fraud classifier.

```sql
CREATE MODEL fraud_classifier
INPUT  (transaction_details STRING)
OUTPUT (risk_level STRING)
COMMENT 'Classifies each transaction as LOW, MEDIUM, or HIGH fraud risk'
WITH (
  'provider'                = 'googleai',
  'task'                    = 'classification',
  'googleai.connection'     = 'gemini_conn',
  'googleai.model_version'  = 'gemini-2.0-flash',
  'googleai.system_prompt'  = 'You are a fraud detection model for a retail bank.
You receive a single line describing a transaction in the format:
  amount:<value>, type:<value>, status:<value>, merchant:<value>
Classify the fraud risk and reply with exactly one word: LOW, MEDIUM, or HIGH.
HIGH: amount over 5000, or Failed status with payment type, or unknown merchant.
MEDIUM: amount 1000-5000, or recurring failed statuses.
LOW: small amounts, successful status, recognizable merchant.'
);
```

> Gemini integration requires exactly one input column of type STRING. All transaction fields are concatenated into a single descriptive string before the model is called.


Verify:

```sql
SHOW MODELS;
```

---

### 2 — Classify Transactions in Real Time

Call the model per row using `ML_PREDICT` inside a `LATERAL TABLE`. Each event flows through the LLM and comes back enriched with `risk_level`.

```sql
SELECT
  t.txn_id,
  t.account_number,
  t.amount,
  t.transaction_type,
  t.status,
  t.merchant,
  t.`timestamp`,
  p.risk_level
FROM transactions_faker AS t,
LATERAL TABLE(ML_PREDICT('fraud_classifier',
  CONCAT('amount:', CAST(t.amount AS STRING),
         ', type:', t.transaction_type,
         ', status:', t.status,
         ', merchant:', t.merchant)
)) AS p;
```

**What this does:**
- `LATERAL TABLE(ML_PREDICT(...))` calls the model once per row as it arrives on the stream
- The model's output columns (`risk_level`) are appended to the original event
- The pipeline is fully streaming — there is no batch boundary

---

## Part 2 — Customer Support Agent with a Custom UDF Tool

A **streaming agent** runs a reasoning loop: the LLM decides which tools to call, calls them, reads the results, and repeats until it can answer. Here the agent's tool is a custom Java UDF that simulates a live account lookup.

```
Customer query arrives
        │
        ▼
  Agent (Gemini)
  ┌──────────────────────────────────────┐
  │  1. Read query                       │
  │  2. Call account_lookup UDF          │
  │  3. Read account data                │
  │  4. Compose response                 │
  └──────────────────────────────────────┘
        │
        ▼
  Response emitted to stream
```

### 3 — Build the Account Lookup UDF

The UDF is a Java `ScalarFunction` that takes an account number and returns account details as a JSON string. In production this would call a real database or API; here it returns deterministic demo data.

The source is in:
```
flink-udf-account-lookup/src/main/java/io/confluent/flink/udf/AccountLookupFunction.java
```

Build the uber JAR:

```bash
cd flink-udf-account-lookup
mvn clean package -DskipFormat
```

This produces `target/flink-udf-account-lookup-1.0.jar`.

---

### 4 — Upload and Register the Function

Upload the JAR to Confluent Cloud as a Flink artifact:

```bash
confluent flink artifact create \
  --environment <env-id> \
  --cloud-provider <gcp|aws|azure> \
  --region <region> \
  --artifact-name account-lookup-udf \
  --artifact-file flink-udf-account-lookup/target/flink-udf-account-lookup-1.0.jar
```

The CLI returns an artifact ID (`cfa-xxxxxxx`). Register the function in Flink SQL:

```sql
CREATE FUNCTION account_lookup AS 'io.confluent.flink.udf.AccountLookupFunction'
USING JAR 'confluent-artifact://<artifact-id>';
```

Test it directly:

```sql
SELECT account_lookup('ACC1000001');
```

You should see a JSON string with the account details.

---

### 5 — Wrap the Function as an Agent Tool

Create a **tool** that exposes the UDF to the agent's reasoning loop:

```sql
CREATE TOOL account_lookup_tool
USING FUNCTION account_lookup
WITH (
  'type'        = 'function',
  'description' = 'Looks up live account information for a given account number.
Returns a JSON object with fields: balance, status, last_transaction_amount, last_transaction_date, and alert_flag.
Call this tool whenever the customer asks about their account balance, recent transactions, or account status.'
);
```

The `description` is what the LLM reads to decide when to call this tool — write it clearly so the model understands the tool's purpose and output format.

---

### 6 — Register the Agent's Model and Create the Agent

Register Gemini as the model driving the agent (`text_generation` task, not `classification`):

```sql
CREATE MODEL support_llm
INPUT  (query STRING)
OUTPUT (response STRING)
COMMENT 'Gemini model powering the customer support agent'
WITH (
  'provider'               = 'googleai',
  'task'                   = 'text_generation',
  'googleai.connection'    = 'gemini_conn',
  'googleai.model_version' = 'gemini-1.5-pro'
);
```

Compose the model and tool into the agent:

```sql
CREATE AGENT customer_support_agent
USING MODEL support_llm
USING PROMPT 'You are a customer support agent for a retail bank.

When a customer asks about their account, always call the account_lookup tool first to get real data.
Keep your responses concise (2-3 sentences) and professional.
If the account has an alert_flag set to true, inform the customer and recommend calling 1-800-BANK-HELP.'

USING TOOLS account_lookup_tool
WITH (
  'max_iterations'           = '5',
  'max_consecutive_failures' = '2'
);
```

---

### 7 — Create a Customer Query Stream

Simulate customer support queries flowing in as a stream:

```sql
CREATE TABLE customer_queries (
  query_id       VARCHAR(36),
  account_number VARCHAR(255),
  query_text     VARCHAR(500),
  `timestamp`    TIMESTAMP(3) WITH LOCAL TIME ZONE,
  WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECONDS
)
DISTRIBUTED BY HASH(account_number) INTO 3 BUCKETS
WITH (
  'connector'                        = 'faker',
  'changelog.mode'                   = 'append',
  'fields.query_id.expression'       = '#{Internet.uuid}',
  'fields.account_number.expression' = 'ACC#{Number.numberBetween ''1000001'',''1000006''}',
  'fields.query_text.expression'     = '#{Options.option
    ''What is my current balance for account ACC1000001?'',
    ''Are there any suspicious transactions on my account ACC1000003?'',
    ''What was my last transaction on account ACC1000002?'',
    ''Is my account ACC1000004 in good standing?'',
    ''Can you check recent activity on account ACC1000005?''}',
  'fields.timestamp.expression'      = '#{date.past ''5'',''SECONDS''}',
  'rows-per-second'                  = '1'
);
```

---

### 8 — Run the Agent on the Stream

Execute the agent over each incoming query using `AI_RUN_AGENT`:

```sql
SELECT
  q.query_id,
  q.account_number,
  q.query_text,
  q.`timestamp`,
  a.response AS agent_response
FROM customer_queries AS q,
LATERAL TABLE(AI_RUN_AGENT(
  'customer_support_agent',
  q.query_text,
  q.query_id
)) AS a;
```

**What this does:**
- `AI_RUN_AGENT('agent', input, context_id)`:
  - `input` — the customer's message sent to the agent
  - `context_id` — a unique ID to correlate responses; use your event's primary key
- For each query the agent calls `account_lookup_tool` (the UDF), reads the account JSON, then composes a response
- One response row is emitted per input query — not one per tool call

---

## Summary

| Step | What you built |
|---|---|
| `CREATE CONNECTION` | Stored Google AI credentials once |
| `CREATE MODEL fraud_classifier` | Gemini-powered fraud risk classifier |
| `ML_PREDICT` | Per-event enrichment — risk label on every transaction |
| `AccountLookupFunction` (UDF) | Java ScalarFunction returning live account data |
| `CREATE TOOL` | Exposed the UDF to the agent's reasoning loop |
| `CREATE AGENT` | Composed model + tool + system prompt |
| `AI_RUN_AGENT` | Ran the agent over a stream of customer queries |

---

