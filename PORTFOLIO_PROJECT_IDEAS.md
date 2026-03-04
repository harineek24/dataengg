# GCP Data Engineering Portfolio Projects

> Portfolio project guides for showcasing **Google Cloud Professional Data Engineering** skills via **Streamlit** apps.
>
> Each project below has a **full recipe-style guide** with architecture diagrams, step-by-step code, validation tests, cleanup instructions, and extension challenges.

---

## Projects

| # | Project | Guide | Key GCP Services | Time |
|---|---------|-------|-------------------|------|
| 1 | API Rate Limiting & Analytics | [View Guide](projects/api-rate-limiting-analytics/api-rate-limiting-analytics.md) | Cloud Run, Firestore, Cloud Monitoring | ~90 min |
| 2 | Data Pipeline Automation | [View Guide](projects/data-pipeline-automation/data-pipeline-automation.md) | BigQuery, Cloud KMS, Pub/Sub, Cloud Functions | ~120 min |
| 3 | Real-Time Streaming Pipeline Dashboard | [View Guide](projects/real-time-streaming-pipeline-dashboard/real-time-streaming-pipeline-dashboard.md) | Pub/Sub, Dataflow, BigQuery | ~120 min |
| 4 | ELT Pipeline Builder & Orchestrator | [View Guide](projects/elt-pipeline-builder/elt-pipeline-builder.md) | BigQuery, Cloud Storage, Cloud Composer | ~120 min |
| 5 | AI Model Bias Detection | [View Guide](projects/ai-model-bias-detection/ai-model-bias-detection.md) | Vertex AI, Cloud Functions, Cloud Scheduler, Pub/Sub | ~90 min |

---

### 1. API Rate Limiting & Analytics
**GCP Services:** Cloud Run, Firestore, Cloud Monitoring
**Skills Demonstrated:** Serverless compute, NoSQL databases, API management, distributed rate limiting
**Estimated Time:** 90 minutes

Build a serverless API gateway with intelligent rate limiting backed by Firestore transactions and real-time usage analytics. Demonstrates Cloud Run auto-scaling, transactional consistency, and production API patterns (X-RateLimit headers).

---

### 2. Data Pipeline Automation
**GCP Services:** BigQuery, Cloud KMS, Cloud Scheduler, Cloud Pub/Sub, Cloud Functions
**Skills Demonstrated:** Continuous queries, CMEK encryption, column-level security, automated compliance
**Estimated Time:** 120 minutes

Build a security-first data pipeline using BigQuery Continuous Queries for real-time SQL transformations and Cloud KMS for three layers of encryption (topic, table, column). Includes automated security audits and compliance audit trails.

---

### 3. Real-Time Streaming Pipeline Dashboard
**GCP Services:** Pub/Sub, Dataflow, BigQuery
**Skills Demonstrated:** Stream processing, pipeline orchestration, windowed aggregations
**Estimated Time:** 120 minutes

End-to-end streaming pipeline: IoT event generator → Pub/Sub → Dataflow (Apache Beam) → BigQuery, with a Streamlit dashboard showing live throughput, windowed aggregations, and dead-letter queue monitoring.

---

### 4. ELT Pipeline Builder & Orchestrator
**GCP Services:** Cloud Storage, BigQuery, Cloud Composer (Airflow)
**Skills Demonstrated:** ELT patterns, orchestration, transformation, scheduling
**Estimated Time:** 120 minutes

Interactive ELT pipeline builder that ingests data from CSV/API → stages in GCS → loads into BigQuery → transforms with SQL. Includes DAG visualization, run history tracking, and data lineage diagrams.

---

### 5. AI Model Bias Detection
**GCP Services:** Vertex AI Model Monitoring, Cloud Functions, Cloud Scheduler, Pub/Sub
**Skills Demonstrated:** ML model monitoring, bias/fairness metrics, responsible AI governance, event-driven architecture
**Estimated Time:** 90 minutes

Automated bias detection system using Vertex AI Model Monitoring for drift tracking, Cloud Functions for event-driven fairness analysis (demographic parity, equalized odds, calibration), and Cloud Scheduler for weekly comprehensive audits with compliance logging.

---

## Deployment

| Method | Best For |
|--------|----------|
| **Streamlit Community Cloud** | Free hosting, public portfolio |
| **Cloud Run** | Containerized, auto-scaling, GCP-native |

See [DEPLOY.md](DEPLOY.md) for step-by-step deployment instructions.
