# 🛡️ Automated Data Quality Framework (POC)

A scalable, metadata-driven Data Quality (DQ) framework designed to validate thousands of data lake tables using **Apache Iceberg**, **Spark**, and **Airflow**.

This project implements a **Hybrid Architecture**:
- **Orchestration:** Runs locally on the Host (Mac/Linux) via Airflow.
- **Compute & Storage:** Runs isolated inside **Kubernetes (Docker Desktop)**.

---

## 🏗️ Architecture

The framework follows the **Circuit Breaker Pattern** to save compute resources:

1.  **Metadata Check:** Validates table existence and schema. If this fails, the pipeline stops immediately.
2.  **Data Validation:** Runs deep data checks (Nulls, Range, Regex, Cross-column).
    * *Optimization 1:* **Incremental Batching** (only processes data from the current run hour).
    * *Optimization 2:* **Column Pruning** (Spark reads only necessary columns).
3.  **Reporting:** Generates an HTML dashboard using **Allure**, regardless of pass/fail status.

---

## 🚀 Key Features

- **Metadata-Driven:** No hardcoded DAGs per table. Rules are defined centrally in `config/schema_registry.json`.
- **Scalable Storage:** Uses **Apache Iceberg** (Parquet) on **MinIO** (S3 Mock).
- **Isolation:** All heavy processing (Spark/Java) happens in ephemeral Kubernetes Pods.
- **Performance:** Vectorized validation using PySpark and Iceberg partition pruning.

---

## 🛠️ Prerequisites

1.  **Docker Desktop**:
    * Kubernetes must be **Enabled** in settings.
    * Context must be set to `docker-desktop`.
2.  **Python 3.9+** installed locally.
3.  **Git**.

---

## ⚙️ Installation & Setup

### 1. Clone the Repository
```bash
git clone [https://github.com/asifm37/dq-framework-poc.git](https://github.com/asifm37/dq-framework-poc.git)
cd dq-framework-poc