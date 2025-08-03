# Build a Data Warehouse Ageing Fact Table

This project sets up a simple ETL pipeline using Apache Airflow and PostgreSQL to simulate invoice and payment processing. It generates sample data for invoices, credit notes, and payments, and runs a DAG to process them.

---

## 🚀 Requirements

- Docker
- Docker Compose
- Python 3.12
- (Optional) A PostgreSQL client like DBeaver, TablePlus, or the `psql` CLI

---

## 🐳 Running with Docker

### 1. Start Airflow and PostgreSQL

Run the following command in your terminal to start the services:
```bash
docker compose up --build -d
```

This will start:
- PostgreSQL on port 5432
- Airflow webserver on port 8080
- Airflow scheduler and other components

---

## 🛠️ Database Setup

### 2. Connect to the Database

You can connect using a PostgreSQL client with the following credentials:

- Host: `localhost`
- Port: `5432`
- Username: `airflow`
- Password: `airflow`
- Database: `ageing`

---

## 📦 Initialize Sample Data

### 3. Create Tables and Generate Sample Data

Run the SQL script located at `db/init.sql`. This script will:

- Create the tables: `invoices`, `credit_notes`, `payments`
- Insert 1000 rows into `invoices`
- Insert 1000 rows into `credit_notes`
- Insert 2000 rows into `payments`

You can execute the script using any PostgreSQL client connected to the running database.

---

## ⏱️ Run the DAG

### 4. Access the Airflow Web UI

Open your browser and go to: http://localhost:8080

Login credentials:

- Username: `admin`
- Password: `admin`

### 5. Trigger the DAG

- Look for the DAG named `ageing_pipeline`
- Trigger it manually from the Airflow UI
- Monitor task progress and logs in the UI

---

## 🧪 About the DAG & dbt

The DAG includes several tasks orchestrated by Airflow:

1. **Extract Raw Data** from the source tables: `invoices`, `credit_notes`, and `payments`

2. **Load Raw Data** into staging models

3. **Transform with dbt**:
   - The DAG triggers dbt to run transformations defined in the `dbt/` folder
   - dbt uses SQL models to join and process the source data
   - The final output is an `ageing_fact` table that summarizes outstanding balances by customer and due date range

dbt models are modular, version-controlled, and allow easy testing and documentation. You can extend the logic in the `dbt/models/` directory.

## ✅ Notes

- The sample data is already generated and loaded into the PostgreSQL database, all inputs and output are saved in the `test_data` folder.
- The DAG reads from this data and processes it according to the defined ETL steps.
- You can modify the DAG logic in the `dags/` folder and redeploy as needed.

---
