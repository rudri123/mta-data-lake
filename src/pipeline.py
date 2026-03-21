"""
MTA Transit Data Lake — Ingestion Pipeline
==========================================
Pulls MTA subway ridership data from NYC Open Data (data.ny.gov),
cleans and transforms it, then loads it into an AWS S3 data lake
with a raw / processed / logs folder structure.

Author: Rudri Trivedi
"""

import os
import io
import json
import logging
import hashlib
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

import boto3
import pandas as pd
import requests
from botocore.exceptions import BotoCoreError, ClientError

# ── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
# MTA Subway Hourly Ridership — NYC Open Data (public, no API key needed)
MTA_API_URL = (
    "https://data.ny.gov/resource/wujg-7c2s.csv"
    "?$limit=50000"
    "&$order=transit_timestamp DESC"
)

BUCKET_NAME   = os.getenv("S3_BUCKET_NAME", "mta-data-lake-rudri")
AWS_REGION    = os.getenv("AWS_REGION", "us-east-1")

# S3 folder (prefix) structure
RAW_PREFIX       = "raw/subway/"
PROCESSED_PREFIX = "processed/subway/"
LOGS_PREFIX      = "logs/"


# ── S3 helpers ────────────────────────────────────────────────────────────────

def get_s3_client():
    """Return a boto3 S3 client using env credentials or AWS config."""
    return boto3.client("s3", region_name=AWS_REGION)


def upload_to_s3(s3_client, data: bytes, s3_key: str, content_type: str = "text/csv") -> bool:
    """Upload raw bytes to S3. Returns True on success."""
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=data,
            ContentType=content_type,
        )
        log.info("Uploaded  →  s3://%s/%s  (%d bytes)", BUCKET_NAME, s3_key, len(data))
        return True
    except (BotoCoreError, ClientError) as exc:
        log.error("S3 upload failed for %s: %s", s3_key, exc)
        return False


# ── Ingestion ─────────────────────────────────────────────────────────────────

def fetch_mta_data(url: str = MTA_API_URL) -> pd.DataFrame:
    """
    Download MTA subway ridership CSV from NYC Open Data.
    Returns a raw DataFrame.
    """
    log.info("Fetching MTA data from: %s", url)
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    df = pd.read_csv(io.StringIO(response.text))
    log.info("Fetched %d rows, %d columns", len(df), len(df.columns))
    return df


# ── Validation ────────────────────────────────────────────────────────────────

def validate_raw_data(df: pd.DataFrame) -> dict:
    """
    Run data quality checks on the raw DataFrame.
    Returns a validation report dict.
    """
    report = {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "null_counts": df.isnull().sum().to_dict(),
        "duplicate_rows": int(df.duplicated().sum()),
        "columns": list(df.columns),
        "passed": True,
        "issues": [],
    }

    if len(df) == 0:
        report["passed"] = False
        report["issues"].append("DataFrame is empty — no data fetched")

    null_pct = df.isnull().mean()
    high_null_cols = null_pct[null_pct > 0.5].index.tolist()
    if high_null_cols:
        report["issues"].append(f"High null rate (>50%) in columns: {high_null_cols}")

    if report["duplicate_rows"] > 0:
        report["issues"].append(f"{report['duplicate_rows']} duplicate rows detected")

    log.info(
        "Validation — rows: %d | duplicates: %d | issues: %d",
        report["total_rows"],
        report["duplicate_rows"],
        len(report["issues"]),
    )
    return report


# ── Transformation ────────────────────────────────────────────────────────────

def clean_and_transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and enrich the raw MTA DataFrame.
    Steps:
      1. Standardise column names
      2. Parse timestamps
      3. Drop duplicates
      4. Fill / drop nulls
      5. Add derived columns (date, hour, day_of_week)
      6. Cast numeric columns
    """
    log.info("Starting transformation on %d rows", len(df))

    if df.empty and len(df.columns) == 0:
        log.info("Empty DataFrame received, returning as is")
        return df

    # 1. Standardise column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )

    # 2. Parse timestamp if present
    ts_col = next((c for c in df.columns if "timestamp" in c or "time" in c), None)
    if ts_col:
        df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
        df["date"]        = df[ts_col].dt.date.astype(str)
        df["hour"]        = df[ts_col].dt.hour
        df["day_of_week"] = df[ts_col].dt.day_name()
        log.info("Parsed timestamp column: %s", ts_col)

    # 3. Drop exact duplicates
    before = len(df)
    df = df.drop_duplicates()
    log.info("Removed %d duplicate rows", before - len(df))

    # 4. Drop rows where ALL values are null
    df = df.dropna(how="all")

    # 5. Cast ridership / numeric columns
    numeric_cols = [c for c in df.columns if "ridership" in c or "count" in c or "fare" in c]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # 6. Add pipeline metadata
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()

    log.info("Transformation complete — %d rows remaining", len(df))
    return df


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_pipeline(dry_run: bool = False) -> dict:
    """
    Full pipeline:
      fetch → validate → transform → upload raw → upload processed → log

    Args:
        dry_run: If True, skips S3 uploads (useful for local testing).

    Returns:
        run_report dict with status and metadata.
    """
    run_id    = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    timestamp = datetime.now(timezone.utc).isoformat()

    run_report = {
        "run_id": run_id,
        "timestamp": timestamp,
        "status": "started",
        "rows_fetched": 0,
        "rows_processed": 0,
        "validation": {},
        "s3_keys": {},
        "errors": [],
    }

    try:
        # ── 1. Fetch ──────────────────────────────────────────────────────────
        raw_df = fetch_mta_data()
        run_report["rows_fetched"] = len(raw_df)

        # ── 2. Validate ───────────────────────────────────────────────────────
        validation = validate_raw_data(raw_df)
        run_report["validation"] = validation
        if not validation["passed"]:
            log.warning("Validation issues found: %s", validation["issues"])

        # ── 3. Serialize raw CSV ──────────────────────────────────────────────
        raw_csv_bytes = raw_df.to_csv(index=False).encode("utf-8")
        raw_key       = f"{RAW_PREFIX}{run_id}/mta_ridership_raw.csv"

        # ── 4. Transform ──────────────────────────────────────────────────────
        processed_df  = clean_and_transform(raw_df.copy())
        run_report["rows_processed"] = len(processed_df)
        proc_csv_bytes = processed_df.to_csv(index=False).encode("utf-8")
        proc_key       = f"{PROCESSED_PREFIX}{run_id}/mta_ridership_processed.csv"

        # ── 5. Upload to S3 (or save locally) ─────────────────────────────────
        if not dry_run:
            s3 = get_s3_client()
            upload_to_s3(s3, raw_csv_bytes,  raw_key,  "text/csv")
            upload_to_s3(s3, proc_csv_bytes, proc_key, "text/csv")
            run_report["s3_keys"] = {"raw": raw_key, "processed": proc_key}
        else:
            log.info("LOCAL MODE — saving CSVs to the local 'data/' folder")
            os.makedirs(f"data/{run_id}", exist_ok=True)
            with open(f"data/{run_id}/mta_ridership_raw.csv", "wb") as f:
                f.write(raw_csv_bytes)
            with open(f"data/{run_id}/mta_ridership_processed.csv", "wb") as f:
                f.write(proc_csv_bytes)
            
            run_report["local_paths"] = {
                "raw": f"data/{run_id}/mta_ridership_raw.csv",
                "processed": f"data/{run_id}/mta_ridership_processed.csv"
            }

        # ── 6. Compute checksum for audit ─────────────────────────────────────
        run_report["raw_checksum"] = hashlib.md5(raw_csv_bytes).hexdigest()
        run_report["status"]       = "success"
        log.info("Pipeline completed successfully — run_id: %s", run_id)

    except requests.RequestException as exc:
        run_report["status"] = "failed"
        run_report["errors"].append(f"Network error: {exc}")
        log.error("Pipeline failed — network error: %s", exc)

    except Exception as exc:  # pylint: disable=broad-except
        run_report["status"] = "failed"
        run_report["errors"].append(str(exc))
        log.error("Pipeline failed — unexpected error: %s", exc)

    finally:
        # ── 7. Upload run log to S3 ───────────────────────────────────────────
        log_key        = f"{LOGS_PREFIX}{run_id}/run_report.json"
        log_bytes      = json.dumps(run_report, indent=2, default=str).encode("utf-8")

        if not dry_run:
            try:
                s3 = get_s3_client()
                upload_to_s3(s3, log_bytes, log_key, "application/json")
            except Exception as exc:  # pylint: disable=broad-except
                log.error("Failed to upload run log: %s", exc)
        else:
            # Save log locally during dry run
            os.makedirs("data", exist_ok=True)
            with open(f"data/run_report_{run_id}.json", "w") as f:
                json.dump(run_report, f, indent=2, default=str)
            log.info("DRY RUN — log saved locally: data/run_report_%s.json", run_id)

    return run_report


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="MTA Data Lake Ingestion Pipeline")
    parser.add_argument(
        "--local",
        dest="dry_run",
        action="store_true",
        help="Run pipeline locally (saves data to 'data/' instead of uploading to S3)",
    )
    args = parser.parse_args()

    report = run_pipeline(dry_run=args.dry_run)
    print("\n" + "=" * 50)
    print(f"  Status  : {report['status'].upper()}")
    print(f"  Run ID  : {report['run_id']}")
    print(f"  Fetched : {report['rows_fetched']} rows")
    print(f"  Processed: {report['rows_processed']} rows")
    if report["errors"]:
        print(f"  Errors  : {report['errors']}")
    print("=" * 50)
