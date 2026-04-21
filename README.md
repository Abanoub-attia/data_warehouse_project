# 🏢 Data Warehouse Project

![SQL Server](https://img.shields.io/badge/Database-SQL_Server_Express-CC2927)
![T-SQL](https://img.shields.io/badge/Language-T--SQL-blue)
![VS Code](https://img.shields.io/badge/IDE-VS_Code-007ACC)
![Linux Mint](https://img.shields.io/badge/OS-Linux_Mint-87cf3e)
![Medallion Architecture](https://img.shields.io/badge/Architecture-Medallion-orange)
![Star Schema](https://img.shields.io/badge/Modeling-Star_Schema-green)

A comprehensive, end-to-end data warehouse built with **SQL Server**, covering ETL pipelines, dimensional data modeling.

---

## 📐 Architecture Overview

This project follows the **Medallion Architecture**, organizing data across three progressive layers:

| Layer | Description |
|-------|-------------|
| 🥉 **Bronze** | Raw data ingested as-is from source CSV files into SQL Server |
| 🥈 **Silver** | Cleansed, standardized, and normalized data ready for modeling |
| 🥇 **Gold** | Business-ready star schema optimized for reporting and analytics |

---

## 📖 Project Overview

This project demonstrates a production-grade data warehousing solution, encompassing:

1. **Data Architecture** — Designing a modern warehouse using the Medallion Architecture (Bronze → Silver → Gold).
2. **ETL Pipelines** — Extracting, transforming, and loading data from ERP and CRM source systems.
3. **Data Modeling** — Building fact and dimension tables in a star schema for efficient analytical queries.
4. **Data Quality** — Running validation and quality checks across all layers.

---

## 🗂️ Repository Structure

```
├── datasets          # Source data files (ERP & CRM CSV files)
│   ├── source_crm
│   │   ├── cust_info.csv
│   │   ├── prd_info.csv
│   │   └── sales_details.csv
│   └── source_erp
│       ├── CUST_AZ12.csv
│       ├── LOC_A101.csv
│       └── PX_CAT_G1V2.csv
├──  docs          # Architecture diagrams, data catalog, and design docs
│   ├── data_catalog.md
│   ├── data_flow.jpg
│   ├── data_integration.jpg
│   └── data_model.jpg
├── README.md
├── scripts          # T-SQL scripts for each pipeline layer
│   ├── 1_init_database.sql
│   ├── bronze_layer
│   │   ├── 2_create_tables_bronze.sql
│   │   ├── 3_load_data_bronze_proc.sql
│   │   └── 4_load_data_bronze.sql
│   ├── gold_layer
│   │   └── 11_create_view_gold.sql
│   └── silver_layer
│       ├── 10_load_data_silver.sql
│       ├── 6_create_tables_silver.sql
│       └── 8_load_data_silver_proc.sql
└── tests          # Data quality checks and validation scripts
    ├── 12_test_data_quality_after_gold.sql
    ├── 5_test_data_quality_after_bronze.sql
    ├── 7_test_data_quality_before_silver.sql
    └── 9_test_data_quality_after_silver.sql
```

---

## 🔄 ETL Pipeline

### 🥉 Bronze Layer — Raw Ingestion
- Loads data directly from CSV source files into staging tables in SQL Server.
- No transformations applied; data is stored in its original form.

### 🥈 Silver Layer — Cleansing & Transformation
- Handles null values, deduplication, and data type standardization.
- Applies business rules for normalization and data integration across ERP and CRM sources.

### 🥇 Gold Layer — Dimensional Modeling
- Implements a **star schema** with fact and dimension tables.
- Produces clean, analytics-ready views and tables for BI consumption.

---

## 🛠️ Tech Stack & Tools

| Tool | Purpose |
|------|---------|
| **SQL Server Express** | Database engine for hosting the warehouse |
| **VS Code** | Primary code editor for writing and executing SQL scripts |
| **SQL Server (mssql) Extension** | VS Code extension for connecting to and querying SQL Server |
| **T-SQL** | Primary language for all ETL and analytics scripts |
| **Draw.io** | Architecture and data flow diagrams |
| **Git / GitHub** | Version control and collaboration |

---

## 🚀 Getting Started

### Prerequisites

- [SQL Server Express](https://www.microsoft.com/en-us/sql-server/sql-server-downloads) — free database engine
- [VS Code](https://code.visualstudio.com/) — code editor
- [SQL Server (mssql) Extension](https://marketplace.visualstudio.com/items?itemName=ms-mssql.mssql) — VS Code extension for connecting to SQL Server

### Setup Instructions

1. **Clone the repository**
   ```bash
   git clone https://github.com/Abanoub-attia/data_warehouse_project.git
   cd data_warehouse_project
   ```

2. **Initialize the database**
   - Open VS Code and connect to your SQL Server instance using the mssql extension.
   - Run the database initialization script to create the required schemas.

3. **Run the Bronze layer**
   - Execute the scripts in `scripts/bronze/` to load raw CSV data into staging tables.

4. **Run the Silver layer**
   - Execute the scripts in `scripts/silver/` to cleanse and transform the staged data.

5. **Run the Gold layer**
   - Execute the scripts in `scripts/gold/` to build the star schema and analytical views.

6. **Run data quality tests**
   - Execute the scripts in `tests/` to validate data integrity across all layers.

---

## 🧠 Skills Demonstrated

- ✅ Data Warehouse Architecture (Medallion Pattern)
- ✅ ETL Pipeline Development with T-SQL
- ✅ Dimensional Modeling (Star Schema: Facts & Dimensions)
- ✅ Data Cleansing & Quality Validation
- ✅ Documentation & Diagramming
