# Databricks Practice

## Project Overview

This repository contains practice scripts for Databricks  workflows, including Auto CDC, Auto Loader, data quality and quarantine patterns, and Spark Declarative Pipelines.

## Repository Structure

```text
databricks_practice/
└── src/
    ├── auto_cdc/
    │   └── auto_cdc.py
    ├── autoloader/
    │   └── auto_loader.py
    ├── sdp_quality_quarantine/
    │   └── sdp_quality_pipeline.py
    └── spark_declarative_pipelines/
        └── sdp_pipeline.py
```

## Topics Covered

### 1. Auto CDC

**File:** `src/auto_cdc/auto_cdc.py`

Practice area:
- Auto CDC concepts and change-data-capture pipeline implementation.

### 2. Auto Loader

**File:** `src/autoloader/auto_loader.py`

Practice area:
- Incremental file ingestion with Databricks Auto Loader.

### 3. SDP Quality and Quarantine

**File:** `src/sdp_quality_quarantine/sdp_quality_pipeline.py`

Practice area:
- Data quality expectations.
- Handling invalid records and quarantine patterns in a declarative pipeline.

### 4. Spark Declarative Pipelines

**File:** `src/spark_declarative_pipelines/sdp_pipeline.py`

Practice area:
- Defining a Spark Declarative Pipeline using a declarative approach.

