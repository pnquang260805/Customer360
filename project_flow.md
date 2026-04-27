# Customer360 — Full Project Flow

## 1. Architecture Overview

This project implements a **Customer 360** data lakehouse using **Medallion Architecture** (Bronze → Silver → Gold) with Spark Structured Streaming, Hudi, and real-time CDC ingestion.

```mermaid
graph TD
    subgraph Sources["📦 Data Sources"]
        PG[(PostgreSQL)]
        PY[insert_data/main.py<br/>Faker data generator]
        PY --> PG
    end

    subgraph Ingestion["🔄 CDC Ingestion"]
        DEB[Debezium]
        KF[Apache Kafka<br/>4 topics]
        PG -->|WAL| DEB --> KF
    end

    subgraph Bronze["🟫 Bronze Layer"]
        RAW[raw_table<br/>Hudi MOR — raw CDC payloads]
    end

    subgraph Silver["⬜ Silver Layer — Cleaned Data"]
        SC[silver_customer<br/>cleaned, no SK]
        SP[silver_product<br/>cleaned, no SK]
        ST[silver_transaction<br/>cleaned, decoded prices]
    end

    subgraph Dims["🔷 Dimension Layer — SCD Type 2"]
        DC[dim_customer<br/>customer_sk, SCD2]
        DP[dim_product<br/>product_sk, SCD2]
    end

    subgraph Gold["🟡 Gold Layer"]
        FT[fact_transaction<br/>transaction_sk, customer_sk, product_sk]
    end

    subgraph Serving["📊 Serving"]
        MONGO[(MongoDB Atlas<br/>Real-time 360 profiles)]
        MINIO[(MinIO / S3<br/>Hudi tables)]
    end

    KF --> RAW
    RAW --> SC --> DC
    RAW --> SP --> DP
    RAW --> ST
    ST --> FT
    DC --> FT
    DP --> FT
    SC --> MONGO
    ST --> MONGO
    DC & DP & FT --> MINIO
```

---

## 2. Data Sources — `insert_data/main.py`

A Python script using **Faker** populates 4 PostgreSQL tables:

| Table | Key Fields | Notes |
|---|---|---|
| `customer` | customer_id (UUID), phone_number, name, email … | Pre-registered customers |
| `product` | product_id (serial), price, base_price, product_type … | Catalog |
| `transaction` | transaction_id, customer_id, phone_number, source … | 3 types of purchase |
| `event` | event_id, customer_id, type (view / add-to-cart), url | Clickstream |

### 3 Transaction Scenarios

| `i` | Scenario | `customer_id` | `phone_number` | `source` |
|---|---|---|---|---|
| `1` | Online (web) | Existing | Existing | `web` |
| `2` | In-store, **NEW** customer | New UUID | Random new phone | `store` |
| `0` | In-store, existing customer | Existing | Existing | `store` |

---

## 3. CDC Ingestion — Debezium → Kafka

**Debezium** watches PostgreSQL's WAL and emits every change as a CDC event to Kafka:

| Kafka Topic | Source Table |
|---|---|
| `e-commerce-customer.public.customer` | `customer` |
| `e-commerce-product.public.product` | `product` |
| `e-commerce-transaction.public.transaction` | `transaction` |
| `e-commerce-event.public.event` | `event` |

Each message follows the Debezium CDC envelope:
```json
{
  "after":  { ... },
  "op": "c",
  "ts_ms": 1234567890
}
```

> [!NOTE]
> Debezium encodes PostgreSQL `DECIMAL` columns as **Base64 strings** (big-endian unscaled bytes). `TransformUtils.decodeDecimalUdf` handles this decoding throughout the pipeline.

---

## 4. Spark Pipeline — Layer by Layer

### 4.1 Extract — `ExtractKafka`

All 4 Kafka topics are consumed as Spark Structured Streaming DataFrames, each with columns: `offset`, `topic`, `key` (table name), `value` (raw JSON).

They're **union-ed** into a single combined stream:
```scala
val combinedRawDf = customerStreamDf.union(productStreamDf).union(eventStreamDf).union(transactionDf)
```

---

### 4.2 Bronze — `raw_table`

The combined stream is written **as-is** to Hudi (MOR) at `s3a://tables/bronze/raw_table/`. This is the **immutable audit log** — zero transformation.

```mermaid
erDiagram
    raw_table {
        string key "Table name (customer/product/transaction/event)"
        string value "Raw JSON CDC payload"
        string topic "Kafka topic source"
        int partition "Kafka partition"
        long offset "Kafka offset"
        timestamp timestamp "Ingestion time"
    }
```

---

### 4.3 Silver — Cleaned Data (No Surrogate Keys)

Silver tables store **cleaned, typed** data — decoded decimals, normalized phone numbers, proper date types — but **no surrogate keys** and **no SCD2 history**.

#### `silver_customer` — [TransformCustomerSilver.scala](file:///e:/Mine/Code/Customer360/pipeline/src/main/scala/transformers/TransformCustomerSilver.scala)

```mermaid
flowchart LR
    A[Raw CDC JSON] --> B[Filter key=customer]
    B --> C[Parse CDC envelope]
    C --> D[Drop deletes op=d]
    D --> E[Normalize phone_number<br/>strip spaces, +84→0]
    E --> F[Convert epoch days→dates<br/>date_of_birth, creation_date]
    F --> G[Drop address]
    G --> H[silver_customer table]
```

**Schema:**
```mermaid
erDiagram
    silver_customer {
        string customer_id
        string first_name
        string last_name
        string gender
        date date_of_birth
        string email
        string phone_number
        string country
        date customer_creation_date
    }
```

Also written to **MongoDB Atlas** for real-time customer lookup.

---

#### `silver_product` — [TransformProductDim.scala](file:///e:/Mine/Code/Customer360/pipeline/src/main/scala/transformers/TransformProductDim.scala)

```mermaid
flowchart LR
    A[Raw CDC JSON] --> B[Filter key=product]
    B --> C[Parse CDC envelope]
    C --> D[Drop null product_id]
    D --> E[Decode Base64→DECIMAL<br/>price, base_price]
    E --> F[Cast product_id to String]
    F --> G[silver_product table]
```

**Schema:**
```mermaid
erDiagram
    silver_product {
        string product_id
        string product_name
        string product_link
        decimal price
        decimal base_price
        string currency
        string sale_percents
        string product_type
    }
```

---

#### `silver_transaction` — [TransformTransactionSilver.scala](file:///e:/Mine/Code/Customer360/pipeline/src/main/scala/transformers/TransformTransactionSilver.scala)

```mermaid
flowchart LR
    A[Raw CDC JSON] --> B[Filter key=transaction]
    B --> C[Parse CDC envelope]
    C --> D[Decode Base64→DECIMAL<br/>price, total_amount]
    D --> E[Normalize phone_number]
    E --> F[Watermark 10 min]
    F --> G[silver_transaction table]
```

**Schema:**
```mermaid
erDiagram
    silver_transaction {
        string transaction_id
        string customer_id
        string product_id
        string phone_number
        string product_name
        decimal price
        decimal quantity
        decimal total_amount
        timestamp event_time
        string source
        timestamp timestamp
        boolean is_new_customer
    }
```

Also runs a **resolve step**: joins with MongoDB customer lookup → writes enriched transactions to MongoDB for 360 profiles.

---

### 4.4 Dimension Tables — SCD Type 2 (With Surrogate Keys)

Both dimension tables are derived **from their silver counterparts** via `SqlStreamService` using `foreachBatch`. Each micro-batch:

1. **Upserts** cleaned data into the silver table
2. **Generates** a surrogate key (UUID) for each row
3. **Expires** existing current rows in the dim table (`is_current=false, expired_date=today`)
4. **Inserts** new dim rows (`is_current=true, expired_date=9999-12-31`)

#### `dim_customer`

```mermaid
erDiagram
    dim_customer {
        string customer_sk PK
        string customer_id "Natural key"
        string first_name
        string last_name
        string gender
        date date_of_birth
        string email
        string phone_number
        string country
        date customer_creation_date
        date effective_date
        date expired_date
        boolean is_current
    }
```

#### `dim_product`

```mermaid
erDiagram
    dim_product {
        string product_sk PK
        string product_id "Natural key"
        string product_name
        string product_link
        decimal price
        decimal base_price
        string currency
        string sale_percents
        string product_type
        date effective_date
        date expired_date
        boolean is_current
    }
```

#### `dim_date`

```mermaid
erDiagram
    dim_date {
        int date_key PK "YYYYMMDD"
        date date
        int year
        int month
        int day
        int day_of_week
        int quarter
    }
```

---

### 4.5 Gold — `fact_transaction`

#### [TransformFactTransaction.scala](file:///e:/Mine/Code/Customer360/pipeline/src/main/scala/transformers/TransformFactTransaction.scala) — `processFactBatch()`

Runs via `foreachBatch` on the silver_transaction stream. Resolves all surrogate keys:

```mermaid
flowchart TD
    A[Micro-batch of silver_transaction] --> B[Left join dim_customer<br/>ON phone_number, WHERE is_current=true]
    B --> C{customer_sk found?}
    C -->|Yes| D[matched rows]
    C -->|No — new in-store customer| E[Auto-create dim_customer record<br/>customer_sk + customer_id + phone_number<br/>other fields = NULL]
    E --> F[Write to dim_customer via Hudi]
    F --> G[Re-join to get customer_sk]
    D & G --> H[Union all rows]
    H --> I[Left join dim_product<br/>ON product_id, WHERE is_current=true]
    I --> J[Generate transaction_sk UUID]
    J --> K[Write to fact_transaction via Hudi]
```

**Final Star Schema:**

```mermaid
erDiagram
    fact_transaction {
        string transaction_sk PK
        int date_key FK
        string customer_sk FK
        string product_sk FK
        string customer_id
        string transaction_id
        decimal unit_sales_price
        decimal quantity
        decimal total_amount
        timestamp time_stamp
    }
    fact_transaction }|--|| dim_customer : customer_sk
    fact_transaction }|--|| dim_product : product_sk
    fact_transaction }|--|| dim_date : date_key
```

> [!IMPORTANT]
> `customer_sk` is **never null** in the fact table. If an in-store customer doesn't exist in dim_customer, a minimal record is auto-created with only `customer_id`, `customer_sk`, and `phone_number` populated. Other fields remain NULL until the customer registers.

---

### 4.6 Events & Unified Profiles → MongoDB

#### [TransformEvent.scala](file:///e:/Mine/Code/Customer360/pipeline/src/main/scala/transformers/TransformEvent.scala) & [TransformUnifiedProfile.scala](file:///e:/Mine/Code/Customer360/pipeline/src/main/scala/transformers/TransformUnifiedProfile.scala)

Clickstream events and transactions are aggregated by `customer_id` and written to **MongoDB Atlas** to create a single **Unified Customer Profile**.

**Unified Profile Schema (MongoDB):**

```mermaid
erDiagram
    unified_profile {
        string master_id PK "customer_id"
        string first_name
        string last_name
        string gender
        date date_of_birth
        array transaction_log
        array activity_log
    }
```

> [!NOTE]
> `transaction_log` contains an array of recent purchases (id, price, quantity, total), and `activity_log` contains clickstream events (id, type, url, timestamp).

---

## 5. Utilities — `TransformUtils`

| Utility | Purpose |
|---|---|
| `decodeDecimalUdf` | UDF: Debezium Base64 → `java.math.BigDecimal` (scale=2) |
| `normalizePhoneNumber` | Strips spaces/parens/dashes, replaces `+84` → `0` |

---

## 6. Infrastructure (Docker Compose)

| Service | Role |
|---|---|
| **PostgreSQL** | Source OLTP database |
| **Debezium / Kafka Connect** | CDC connector watching PostgreSQL WAL |
| **Apache Kafka** | Message broker (4 topics) |
| **Spark Master + Workers** | Distributed stream processing |
| **Hive Metastore** | Catalog for Hudi tables |
| **MinIO** | S3-compatible object storage (Bronze/Silver/Gold) |
| **MongoDB Atlas** | Cloud NoSQL for real-time customer 360 profiles |

---

## 7. End-to-End Summary

```
PostgreSQL (4 tables)
  → Debezium CDC (WAL capture)
    → Kafka (4 topics)
      → Spark Structured Streaming
          │
          ├─ Bronze: raw_table (Hudi, MinIO) — immutable audit log
          │
          ├─ Silver (cleaned, no SK):
          │   ├─ silver_customer   → also → MongoDB Atlas
          │   ├─ silver_product
          │   └─ silver_transaction → also → MongoDB Atlas (resolved)
          │
          ├─ Dimensions (SCD2, with SK):
          │   ├─ dim_customer  ← derived from silver_customer
          │   └─ dim_product   ← derived from silver_product
          │
          └─ Gold:
              └─ fact_transaction ← joins dim_customer + dim_product
                                    auto-creates dim_customer for new in-store customers
```
