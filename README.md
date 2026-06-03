# PaymentPulse

A real-time payment transaction analytics and fraud detection platform built with Apache Kafka, Apache Spark, MinIO, PostgreSQL, Dagster, Redis, and Streamlit. The platform simulates a high-volume Egyptian payment network processing thousands of daily transactions across 15 service types and 15 governorates, with inline fraud detection powered by a Random Forest model served as a PySpark UDF.

---

## Architecture

```
[Python Simulator]
      │
      ▼ (Kafka Producer)
[Apache Kafka] ── topic: raw_transactions
      │
      ▼ (Spark Structured Streaming)
[Apache Spark]
  ├── Data cleaning & validation
  ├── Feature enrichment
  └── ML fraud scoring (Random Forest UDF)
      │
      ├──▶ [MinIO Data Lake]
      │       ├── Bronze  (raw Parquet)
      │       ├── Silver  (cleaned)
      │       └── Gold    (aggregated)
      │
      ├──▶ [PostgreSQL Data Warehouse]
      │       ├── transactions
      │       ├── merchant_daily_summary
      │       └── governorate_daily_summary
      │
      └──▶ [Redis]
              └── Real-time counters for dashboard

[Dagster] ── schedules daily batch jobs
  ├── Merchant reconciliation DAG
  └── Governorate summary DAG

[Streamlit Dashboard] ── reads from Redis + PostgreSQL
  ├── Live TPS and fraud rate
  ├── Revenue by service type
  └── Transactions by governorate
```

---

## Stack

| Layer | Technology |
|---|---|
| Ingestion | Apache Kafka 7.6, Kafka Connect |
| Stream processing | Apache Spark 3.5.1 Structured Streaming |
| ML fraud scoring | scikit-learn Random Forest, served as PySpark UDF |
| Data lake | MinIO (S3-compatible), Medallion Architecture |
| Data warehouse | PostgreSQL 15 |
| Real-time store | Redis 7.2 |
| Orchestration | Dagster 1.7.7 |
| Dashboard | Streamlit 1.35.0, Plotly |
| Infrastructure | Docker Compose, 15 containerized services |

---

## Services and Ports

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8085 | — |
| Spark UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Dagster UI | http://localhost:3000 | — |
| Streamlit Dashboard | http://localhost:8501 | — |
| Jupyter | http://localhost:8888 | token: pulse |
| PostgreSQL DW | localhost:5432 | pulse / pulse |

---

## Project Structure

```
payment-pulse/
├── simulator/          # Python transaction generator
├── kafka-connect/      # Kafka Connect Dockerfile + S3 connector
├── spark/              # Spark Dockerfile + baked-in JARs
├── spark_jobs/         # Spark streaming job + ML scorer
├── dagster/            # Dagster Dockerfiles + pipeline code
├── jupyter/            # Jupyter Dockerfile
├── streamlit/          # Streamlit dashboard
├── notebooks/          # ML model training notebook
├── sql/                # PostgreSQL schema (init.sql)
├── data/               # Local data volume (gitignored)
└── docker-compose.yml  # Full platform orchestration
```

---

## Running the Platform

**Prerequisites:** Docker Desktop, Git, Python 3.x

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_GITHUB_USERNAME/payment-pulse.git
cd payment-pulse
```

### 2. Download required JARs into spark/jars/

```bash
cd spark/jars

curl -L -o hadoop-aws-3.3.4.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

curl -L -o aws-java-sdk-bundle-1.12.262.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

curl -L -o spark-sql-kafka-0-10_2.12-3.5.1.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar

curl -L -o kafka-clients-3.4.0.jar \
  https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar

curl -L -o spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar

curl -L -o commons-pool2-2.11.1.jar \
  https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

curl -L -o postgresql-42.7.3.jar \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
```

### 3. Start the platform

```bash
cd ../..
docker compose up --build -d
```

### 4. Run the transaction simulator

```bash
cd simulator
pip install kafka-python-ng==2.2.3
python simulator.py
```

### 5. Train the fraud model (optional)

The rule-based fallback scorer is active by default. To replace it with the trained Random Forest model:

- Open http://localhost:8888 and enter token: `pulse`
- Create a new notebook
- Run the cells from `notebooks/train_fraud_model.ipynb` in order
- The saved model files are automatically shared with the Spark container via Docker volume

---

## Domain Context

The platform models a payment network operating in Egypt with the following transaction types:

- Utility bills (electricity, water, gas, telephone)
- Mobile top-ups and internet bills
- Education fees (school and university)
- Government services and traffic fines
- Insurance premiums and credit card payments
- POS and e-commerce purchases
- Donations

Transactions are distributed across 15 Egyptian governorates including Cairo, Giza, Alexandria, Qalyubia, Sharqia, Dakahlia, Beheira, Minya, Assiut, Sohag, Luxor, Aswan, Port Said, Suez, and Ismailia.

Fraud is injected at a 3% rate with patterns including unusually high amounts (EGP 15,000–50,000) and suspicious transaction hours (1am–4am).

---

## Key Engineering Concepts Demonstrated

- **Medallion Architecture** — Bronze / Silver / Gold data lake layers on MinIO
- **Kafka producer-consumer decoupling** — simulator and Spark are fully independent
- **Spark Structured Streaming** — micro-batch processing with foreachBatch sinks
- **ML model serving inside a stream** — Random Forest UDF applied to every record in motion
- **Pre-aggregation pattern** — Redis counters updated by Spark for instant dashboard reads
- **Dual-sink streaming** — same stream written to both MinIO (Parquet) and PostgreSQL (JDBC) simultaneously
- **Dagster orchestration** — scheduled batch reconciliation jobs with full run history and UI
- **Containerized data platform** — 15 services with health checks, dependency ordering, and automatic restarts
