# MTA Transit Data Lake — AWS S3 + Python Pipeline

> An automated data engineering pipeline that ingests MTA subway ridership data from NYC Open Data, validates and transforms it, and loads it into a structured AWS S3 data lake — mirroring real-world data lake architecture.

---

## Architecture

```
NYC Open Data API          Python Pipeline              AWS S3 Data Lake
(data.ny.gov)        →    (boto3 + Pandas)      →    ┌─────────────────────┐
                                                       │ raw/subway/         │
  MTA Subway               1. Fetch CSV               │   YYYYMMDD_HHMMSS/  │
  Hourly Ridership    →    2. Validate data      →    │   ridership_raw.csv │
  Dataset                  3. Clean + transform        ├─────────────────────┤
                           4. Upload raw layer         │ processed/subway/   │
                           5. Upload processed  →      │   ridership_clean   │
                           6. Log run report    →      ├─────────────────────┤
                                                       │ logs/               │
                                                       │   run_report.json   │
                                                       └─────────────────────┘
```

---

## Features

- **Automated ingestion** from NYC Open Data public API (no API key required)
- **Data validation** with null checks, duplicate detection, and quality reporting
- **ETL transformation** — standardised column names, timestamp parsing, type casting, derived features
- **S3 data lake layers** — raw, processed, and logs with timestamped partitions
- **Run logging** — every pipeline run generates a JSON report with row counts, checksums, and error tracking
- **Lifecycle policies** — raw data archived to Glacier after 90 days, logs expired after 30 days
- **Full test suite** — 18 pytest unit tests covering validation, transformation, S3 upload, and orchestration
- **Dry-run mode** — test the full pipeline locally without any AWS calls

---

## Project Structure

```
mta-data-lake/
├── src/
│   ├── pipeline.py       # Main ETL pipeline — fetch, validate, transform, load
│   └── setup_s3.py       # One-time S3 bucket + folder structure setup
├── tests/
│   └── test_pipeline.py  # 18 pytest unit tests
├── data/                 # Local output directory (dry-run logs)
├── requirements.txt
├── .env.example          # Environment variable template
├── .gitignore
└── README.md
```

---

## Quickstart

### 1. Clone and install

```bash
git clone https://github.com/rudri123/mta-data-lake.git
cd mta-data-lake
pip install -r requirements.txt
```

### 2. Configure AWS credentials

```bash
cp .env.example .env
# Edit .env with your AWS credentials
```

Or configure via AWS CLI:
```bash
aws configure
```

### 3. Set up S3 bucket (run once)

```bash
python src/setup_s3.py
```

This creates:
```
s3://mta-data-lake-rudri/
├── raw/subway/
├── processed/subway/
└── logs/
```

### 4. Run the pipeline

```bash
# Full run — fetches data and uploads to S3
python src/pipeline.py

# Dry run — runs everything locally, no S3 calls
python src/pipeline.py --dry-run
```

### 5. Run tests

```bash
pytest tests/ -v
```

Expected output:
```
tests/test_pipeline.py::TestValidateRawData::test_valid_dataframe_passes          PASSED
tests/test_pipeline.py::TestValidateRawData::test_empty_dataframe_fails           PASSED
tests/test_pipeline.py::TestCleanAndTransform::test_duplicates_removed            PASSED
tests/test_pipeline.py::TestCleanAndTransform::test_timestamp_parsed...           PASSED
...
18 passed in 1.2s
```

---

## Data Source

**MTA Subway Hourly Ridership** — NYC Open Data  
URL: [data.ny.gov/resource/wujg-7c2s](https://data.ny.gov/resource/wujg-7c2s)  
License: Public Domain — no API key required  
Schema: `transit_timestamp`, `station_complex`, `borough`, `ridership`, `transfers`, `latitude`, `longitude`

---

## S3 Data Lake Structure

| Layer | Path | Contents |
|---|---|---|
| **Raw** | `raw/subway/YYYYMMDD_HHMMSS/` | Original CSV as fetched from API |
| **Processed** | `processed/subway/YYYYMMDD_HHMMSS/` | Cleaned, typed, enriched CSV |
| **Logs** | `logs/YYYYMMDD_HHMMSS/` | JSON run report with metadata |

Each pipeline run creates a timestamped partition — full data lineage is preserved.

---

## Run Report Sample

```json
{
  "run_id": "20240115_083000",
  "timestamp": "2024-01-15T08:30:00.123456",
  "status": "success",
  "rows_fetched": 50000,
  "rows_processed": 49821,
  "validation": {
    "total_rows": 50000,
    "duplicate_rows": 179,
    "passed": true,
    "issues": []
  },
  "s3_keys": {
    "raw": "raw/subway/20240115_083000/mta_ridership_raw.csv",
    "processed": "processed/subway/20240115_083000/mta_ridership_processed.csv"
  },
  "raw_checksum": "a3f5c2d1e8b4f7a9c0e2d5b8f1a4c7e0"
}
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.11 | Pipeline language |
| boto3 | AWS S3 SDK |
| Pandas | Data transformation |
| requests | HTTP data ingestion |
| pytest | Unit testing |
| AWS S3 | Cloud data lake storage |

---

## Author

**Rudri Trivedi**  
MS Computer Science — New York Institute of Technology  
[LinkedIn](https://www.linkedin.com/in/rudri-trivedi-658817226/) · [GitHub](https://github.com/rudri123)
