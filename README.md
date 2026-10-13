# Databricks Practice

## Project Overview

This repository contains hands-on Databricks practice implementations and sample datasets for Auto CDC, clickstream sessionization, Auto Loader, data quality and quarantine, Spark Declarative Pipelines, and Structured Streaming.

## Repository Structure

```text
databricks_practice/
└── src/
    ├── auto_cdc/
    │   ├── sample_data/
    │   │   └── customer_changes.csv
    │   └── auto_cdc.py
    ├── autoloader/
    ├── clickstream/
    │   ├── sample_data/
    │   │   ├── batch1.csv
    │   │   ├── batch2_advance_watermark.csv
    │   │   ├── batch3_finalize_sessions.csv
    │   │   └── batch4_late_events.csv
    │   └── Clickstream.py
    ├── sdp_quality_quarantine/
    │   ├── sample_data/
    │   │   └── orders_with_invalid_rows.csv
    │   └── sdp_quality_pipeline.py
    ├── spark_declarative_pipelines/
    │   └── sample_data/
    │       ├── orders_batch_1.csv
    │       ├── orders_batch_2.csv
    │       ├── orders_batch_3.csv
    │       └── orders_batch_4.csv
    └── structured_streaming/
        ├── sample_data/
        │   ├── events_batch1.csv
        │   └── events_batch2.csv
        └── Structured_streaming.py
```

> The `autoloader/` folder is present in the repository. Its contents are not listed here because they were not visible in the current folder view.

## Topics Covered

### 1. Auto CDC

**Implementation:** `src/auto_cdc/auto_cdc.py`  
**Sample data:** `src/auto_cdc/sample_data/customer_changes.csv`

Practice area:
- Change Data Capture (CDC) processing.
- Applying customer changes using Databricks Auto CDC.

### 2. Auto Loader

**Folder:** `src/autoloader/`

Practice area:
- Incremental file ingestion with Databricks Auto Loader.

Add implementation and sample-data filenames to this section when they are available in the repository view.

### 3. Clickstream Sessionization

**Implementation:** `src/clickstream/Clickstream.py`  
**Sample data:** `src/clickstream/sample_data/`

Sample files:
- `batch1.csv` — initial clickstream events.
- `batch2_advance_watermark.csv` — events used to advance event-time progress.
- `batch3_finalize_sessions.csv` — events used to help finalize sessions.
- `batch4_late_events.csv` — late-arriving events for watermark testing.

Practice area:
- Session windows and user-session aggregation.
- Event-time watermarks.
- Late-arriving event behavior.

### 4. SDP Quality and Quarantine

**Implementation:** `src/sdp_quality_quarantine/sdp_quality_pipeline.py`  
**Sample data:** `src/sdp_quality_quarantine/sample_data/orders_with_invalid_rows.csv`

Practice area:
- Data quality expectations.
- Detecting invalid records.
- Quarantine patterns in a declarative pipeline.

### 5. Spark Declarative Pipelines

**Sample data folder:** `src/spark_declarative_pipelines/sample_data/`

Sample files:
- `orders_batch_1.csv`
- `orders_batch_2.csv`
- `orders_batch_3.csv`
- `orders_batch_4.csv`

Practice area:
- Working with multiple batches of order data in a declarative pipeline.

The implementation filename is not included in this structure because it was not visible in the latest folder view.

### 6. Structured Streaming

**Implementation:** `src/structured_streaming/Structured_streaming.py`  
**Sample data folder:** `src/structured_streaming/sample_data/`

Sample files:
- `events_batch1.csv`
- `events_batch2.csv`

Practice area:
- Structured Streaming ingestion.
- Processing data in batches.
- Observing streaming progress and watermark behavior.

## Working with the Sample Data

Use the sample files provided under each `sample_data/` directory when running the corresponding practice script or notebook. Check each implementation for its expected source path, target table, and checkpoint location before running it.

