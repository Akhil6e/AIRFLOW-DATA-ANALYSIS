# ✈️ Airflow & Snowflake Flight Data Pipeline

An end-to-end data engineering pipeline built with **Apache Airflow**, **Docker**, **Python**, and **Snowflake** implementing a **Medallion Architecture** (Bronze ➔ Silver ➔ Gold). The pipeline ingests live global flight data from the OpenSky Network REST API, processes and cleans the dataset, calculates aggregated flight statistics, and streams analytical metrics into Snowflake for data visualization and dashboarding.

---

## 🏗️ Architecture & Data Flow

```
┌─────────────────────────┐
│ OpenSky REST API        │
│ (Live Global Flights)   │
└────────────┬────────────┘
             │ Ingest Raw JSON
             ▼
┌─────────────────────────┐
│ 🥉 Bronze Layer          │  Raw storage: /opt/airflow/data/bronze/
└────────────┬────────────┘
             │ Clean & Filter Columns
             ▼
┌─────────────────────────┐
│ 🥈 Silver Layer          │  Structured CSV: /opt/airflow/data/silver/
└────────────┬────────────┘
             │ Aggregate Metrics (by Country)
             ▼
┌─────────────────────────┐
│ 🥇 Gold Layer            │  Aggregated CSV: /opt/airflow/data/gold/
└────────────┬────────────┘
             │ MERGE / Upsert via Snowflake Connector
             ▼
┌─────────────────────────┐
│ ❄️ Snowflake Warehouse   │  Table: FLIGHTS.FLIGHTS_SCHEMA.FLIGHT_TABLE
└─────────────────────────┘
```

---

## 🚀 Key Features

- **Medallion Architecture**: Clear separation of raw ingestion (Bronze), cleaned structured data (Silver), and business-level aggregations (Gold).
- **Automated Airflow Orchestration**: Containerized Apache Airflow DAG running on a 30-minute schedule (`*/30 * * * *`).
- **Snowflake Upserts**: Idempotent data loading into Snowflake using SQL `MERGE INTO` queries to prevent duplicate records.
- **Dockerized Environment**: Fully containerized environment using PostgreSQL as the Airflow backend database.

---

## 📂 Project Structure

```
.
├── dags/
│   └── flight-pipeline.py     # Main Airflow DAG defining pipeline execution order
├── scripts/
│   ├── bronze_layer.py        # API ingestion (Bronze)
│   ├── silver_layer.py        # Data transformation & parsing (Silver)
│   ├── gold_layer.py          # Aggregation logic (Gold)
│   └── snowflake_implement.py # Snowflake load script (MERGE SQL)
├── docker-compose.yml         # Container definitions for Airflow & Postgres
├── DOCUMENTATION.md           # Setup reference & environment configuration
├── .gitignore                 # Git ignore configuration
└── README.md                  # Project overview & documentation
```

---

## ⚙️ Environment Setup & Configuration

### 1. Prerequisites
- [Docker](https://www.docker.com/) and Docker Compose installed.
- A [Snowflake](https://www.snowflake.com/) account.

### 2. Environment Variables
Create a `.env` file in the root directory with the following variables:

```ini
# Postgres Database Credentials
POSTGRES_USER=akhil
POSTGRES_PASSWORD=flights
POSTGRES_DB=flightsdb

# Airflow Admin User Configuration
AIRFLOW_ADMIN_USER=akhil
AIRFLOW_ADMIN_FIRSTNAME=akhil
AIRFLOW_ADMIN_LASTNAME=tiwari
AIRFLOW_ADMIN_EMAIL=admin@example.com
AIRFLOW_ADMIN_PASSWORD=flights
```

---

## ❄️ Snowflake Database & Table Setup

Run the following SQL commands in your Snowflake console to prepare the target warehouse:

```sql
-- 1. Create Database
CREATE DATABASE IF NOT EXISTS FLIGHTS;

-- 2. Create Schema
USE DATABASE FLIGHTS;
CREATE SCHEMA IF NOT EXISTS FLIGHTS_SCHEMA;
USE SCHEMA FLIGHTS_SCHEMA;

-- 3. Create Target Table
CREATE TABLE IF NOT EXISTS FLIGHT_TABLE (
    window_start TIMESTAMP,
    origin_country TEXT,
    total_flights INT,
    avg_velocity FLOAT,
    on_ground INT,
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, origin_country)
);
```

---

## 🔌 Airflow Connection Setup

In the Airflow Web UI (`http://localhost:8080`), navigate to **Admin ➔ Connections** and create a new connection named `flight_snowflake`:

- **Conn Id**: `flight_snowflake`
- **Conn Type**: `Snowflake`
- **Login**: `<Your Snowflake Username>`
- **Password**: `<Your Snowflake Password>`
- **Extra**:
  ```json
  {
    "account": "<your_account_identifier>",
    "warehouse": "<your_warehouse_name>",
    "role": "<your_role_name>"
  }
  ```

---

## 🏃 Getting Started

1. **Start the Airflow Stack**:
   ```bash
   docker compose up -d
   ```

2. **Access the Airflow UI**:
   Open your browser and navigate to `http://localhost:8080`. Log in using the credentials defined in your `.env` file.

3. **Trigger the DAG**:
   Unpause the `flights_ops_medallion_pipe` DAG in the Airflow UI to begin automatic data ingestion and processing.

---

## 📊 Analytics & Dashboard SQL Queries

Use these SQL queries in Snowflake to build analytics dashboards:

### 1. Top 5 Countries by Total Flights
```sql
SELECT
    origin_country,
    SUM(total_flights) AS total_flights
FROM FLIGHTS.FLIGHTS_SCHEMA.FLIGHT_TABLE
GROUP BY origin_country
ORDER BY total_flights DESC
LIMIT 5;
```

### 2. Countries with Fastest Average Flight Speed
```sql
SELECT
    origin_country,
    ROUND(AVG(avg_velocity), 2) AS avg_speed
FROM FLIGHTS.FLIGHTS_SCHEMA.FLIGHT_TABLE
GROUP BY origin_country
ORDER BY avg_speed DESC
LIMIT 5;
```

### 3. Total Flights Ingested (KPI)
```sql
SELECT SUM(total_flights) AS total_flights
FROM FLIGHTS.FLIGHTS_SCHEMA.FLIGHT_TABLE;
```

### 4. Total Active Countries (KPI)
```sql
SELECT COUNT(DISTINCT origin_country) AS active_countries
FROM FLIGHTS.FLIGHTS_SCHEMA.FLIGHT_TABLE;
```

### 5. Daily Flight Activity Trend
```sql
SELECT
    DATE(window_start) AS flight_date,
    SUM(total_flights) AS total_flights
FROM FLIGHTS.FLIGHTS_SCHEMA.FLIGHT_TABLE
GROUP BY DATE(window_start)
ORDER BY flight_date;
```

---

## 👨‍💻 Author

- **Akhil Tiwari** - [GitHub Profile](https://github.com/Akhil6e)
