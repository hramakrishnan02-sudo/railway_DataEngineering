# Railway Operations & Maintenance – CDC Data Engineering Project

## 📌 Project Overview

This project implements an end-to-end **Change Data Capture (CDC) based data engineering pipeline** using Microsoft Azure and Databricks.

The objective is to capture incremental changes from an Azure SQL OLTP database and process them through a modern **Medallion Architecture (Bronze → Silver → Gold)**.

The Gold layer follows a **dimensional/star schema** design and is exposed through a **Databricks SQL Warehouse** for analytical reporting and Power BI.

---

## 🏗️ Architecture

```text
                    ┌─────────────────────────┐
                    │    Azure SQL Database   │
                    │         (OLTP)          │
                    │                         │
                    │  • trains               │
                    │  • schedules            │
                    │  • maintenance_orders   │
                    └────────────┬────────────┘
                                 │
                                 │ SQL CDC
                                 ▼
                    ┌─────────────────────────┐
                    │ Azure Data Factory      │
                    │ Native CDC (Preview)    │
                    │                         │
                    │ Incremental Changes     │
                    │ INSERT / UPDATE / DELETE│
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │      ADLS Gen2          │
                    │       BRONZE            │
                    │                         │
                    │ trains/                 │
                    │ schedules/              │
                    │ maintenance_workorders/│
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │       Databricks        │
                    │   Bronze Delta Tables   │
                    └────────────┬────────────┘
                                 │
                                 │ Delta CDF
                                 ▼
                    ┌─────────────────────────┐
                    │       SILVER            │
                    │                         │
                    │ Data Cleaning           │
                    │ Standardization         │
                    │ Data Quality            │
                    │ AUTO CDC / SCD Type 1   │
                    └────────────┬────────────┘
                                 │
                                 │ Silver CDF
                                 ▼
                    ┌─────────────────────────┐
                    │         GOLD            │
                    │     Star Schema         │
                    │                         │
                    │ dim_train               │
                    │ dim_station             │
                    │ dim_date                │
                    │ dim_maintenance_type   │
                    │ fact_schedule           │
                    │ fact_maintenance        │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │ Databricks SQL Warehouse│
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │        Power BI         │
                    │     Analytics & BI      │
                    └─────────────────────────┘
