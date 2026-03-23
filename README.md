# 🚕 NYC Taxi Big Data Pipeline
## DEAI Final Project 2026

A complete big data engineering pipeline that processes **44 million NYC taxi trips** using industry-standard tools.

---

## 🏗️ Architecture
```
NYC TLC Open Data (44M rows)
        ↓
   [ingest.py]
   Downloads Parquet files
        ↓
   MinIO S3 (raw/)
   Object Storage
        ↓
   [Apache Spark]
   Cleans and processes data
        ↓
   MinIO S3 (silver/ + gold/)
   Clean analytics tables
        ↓
   [DuckDB + FastAPI]
   REST API at localhost:8000
```

---

## 🛠️ Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Object Storage | MinIO (S3) | Store raw/silver/gold data |
| Processing | Apache Spark 3.5 | Clean and aggregate data |
| Query Engine | DuckDB | Fast SQL on Parquet files |
| API Framework | FastAPI | REST endpoints |
| Containers | Docker Compose | Run everything |
| Data Format | Apache Parquet | Efficient columnar storage |

---

## 📁 Data Lake Structure
```
taxi/
  raw/              ← original downloaded files
    yellow_2024_01.parquet
    yellow_2024_02.parquet
    fhvhv_2024_01.parquet
    fhvhv_2024_02.parquet
  silver/           ← cleaned and enriched data
    yellow/
    fhvhv/
  gold/             ← aggregated analytics tables
    rides_per_hour/
    provider_summary/
    top_routes/
    dow_demand/
```

---

## 📂 Project Structure
```
deai-final-project/
  docker-compose.yml     ← full stack orchestration
  ingestion/
    ingest.py            ← downloads and uploads data
    Dockerfile
    requirements.txt
  processing/
    pipeline.py          ← Spark ETL pipeline
  api/
    main.py              ← FastAPI server
    Dockerfile
    requirements.txt
```

---

## 🚀 How to Run

### Prerequisites
- Docker Desktop installed and running

### 1. Start MinIO Storage
```bash
docker compose up -d minio
```
Open MinIO dashboard: http://localhost:9001
Login: minioadmin / minioadmin

### 2. Ingest Data
```bash
docker compose run --rm ingestion
```
Downloads 4 Parquet files from NYC TLC website

### 3. Start Spark
```bash
docker compose up -d spark-master spark-worker
```
Open Spark UI: http://localhost:8080

### 4. Run Spark Pipeline
```bash
docker compose run --rm spark-job
```
Processes 44 million rows into raw, silver and gold layers

### 5. Start API
```bash
docker compose up -d api
```
Open API docs: http://localhost:8000/docs

---

## 🌐 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | /rides-per-day | Daily ride demand over time |
| GET | /rides-per-hour | Peak hour analysis |
| GET | /provider-summary | All providers compared |
| GET | /uber-vs-lyft | Uber vs Lyft head-to-head |
| GET | /top-routes | Most popular routes |
| GET | /day-of-week | Weekly demand patterns |
| GET | /health | Service health check |
| GET | /docs | Interactive Swagger UI |

---

## 📊 Analytical Questions Answered

### Q1 — When is ride demand highest?
**Endpoint:** `/rides-per-hour`

Morning rush (7-9am) and evening rush (5-8pm) are peak times.
Late night rides (10pm-2am) show higher average fares due to surge pricing.

### Q2 — How do Uber and Lyft compare?
**Endpoint:** `/uber-vs-lyft`

Uber dominates NYC with around 87% market share.
Both providers have nearly identical average fares per mile.
Lyft riders tip proportionally more than Uber riders.

### Q3 — Which routes are most popular?
**Endpoint:** `/top-routes`

Airport routes such as JFK and LaGuardia to Manhattan are most frequent.
Short CBD hops dominate by volume but carry lower fares.

---

## 📈 Data Summary

 Dataset                 Rows           Period      
      
 Yellow Taxi             6 million      Jan-Feb 2024
 Uber and Lyft (FHVHV)   38 million     Jan-Feb 2024
 Total                   44 million     Jan-Feb 2024 



## 👨‍🎓 Course
DEAI — Big Data Engineering 2026
