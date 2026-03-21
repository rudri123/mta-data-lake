"""
tests/test_pipeline.py
======================
Unit tests for the MTA Data Lake ingestion pipeline.
Tests cover: data validation, transformation, S3 upload logic,
and pipeline orchestration — without making real network or AWS calls.

Run with:
    pytest tests/ -v
"""

import json
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

# Import functions under test
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pipeline import (
    validate_raw_data,
    clean_and_transform,
    upload_to_s3,
    run_pipeline,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_raw_df():
    """Mimics the shape of a real MTA subway ridership CSV."""
    return pd.DataFrame({
        "transit_timestamp":   ["2024-01-15 08:00:00", "2024-01-15 09:00:00",
                                 "2024-01-15 10:00:00", "2024-01-15 08:00:00"],  # last is dup
        "station_complex_id":  ["001", "002", "003", "001"],
        "station_complex":     ["Times Sq-42 St", "Grand Central-42 St", "34 St-Penn Station", "Times Sq-42 St"],
        "borough":             ["M", "M", "M", "M"],
        "ridership":           ["12500", "8300", "9750", "12500"],
        "transfers":           ["450", "320", None, "450"],
        "latitude":            [40.7580, 40.7527, 40.7506, 40.7580],
        "longitude":           [-73.9855, -73.9772, -73.9936, -73.9855],
    })


@pytest.fixture
def empty_df():
    return pd.DataFrame()


@pytest.fixture
def high_null_df():
    return pd.DataFrame({
        "transit_timestamp": [None, None, None, "2024-01-15 08:00:00"],
        "ridership":         [None, None, None, "100"],
        "station_complex":   [None, None, None, "Times Sq"],
    })


# ── validate_raw_data tests ───────────────────────────────────────────────────

class TestValidateRawData:

    def test_valid_dataframe_passes(self, sample_raw_df):
        report = validate_raw_data(sample_raw_df)
        assert report["total_rows"] == 4
        assert report["total_columns"] == 8
        assert isinstance(report["null_counts"], dict)
        assert isinstance(report["passed"], bool)

    def test_empty_dataframe_fails(self, empty_df):
        report = validate_raw_data(empty_df)
        assert report["passed"] is False
        assert any("empty" in issue.lower() for issue in report["issues"])

    def test_duplicate_rows_detected(self, sample_raw_df):
        report = validate_raw_data(sample_raw_df)
        assert report["duplicate_rows"] == 1

    def test_high_null_columns_flagged(self, high_null_df):
        report = validate_raw_data(high_null_df)
        assert len(report["issues"]) > 0
        assert any("null" in issue.lower() for issue in report["issues"])

    def test_report_contains_required_keys(self, sample_raw_df):
        report = validate_raw_data(sample_raw_df)
        required_keys = ["total_rows", "total_columns", "null_counts",
                         "duplicate_rows", "columns", "passed", "issues"]
        for key in required_keys:
            assert key in report, f"Missing key: {key}"


# ── clean_and_transform tests ─────────────────────────────────────────────────

class TestCleanAndTransform:

    def test_returns_dataframe(self, sample_raw_df):
        result = clean_and_transform(sample_raw_df)
        assert isinstance(result, pd.DataFrame)

    def test_duplicates_removed(self, sample_raw_df):
        result = clean_and_transform(sample_raw_df)
        assert len(result) == 3  # 1 duplicate removed from 4 rows

    def test_column_names_standardised(self, sample_raw_df):
        result = clean_and_transform(sample_raw_df)
        for col in result.columns:
            assert col == col.lower(), f"Column not lowercase: {col}"
            assert " " not in col, f"Column has spaces: {col}"

    def test_timestamp_parsed_and_derived_columns_added(self, sample_raw_df):
        result = clean_and_transform(sample_raw_df)
        assert "date" in result.columns
        assert "hour" in result.columns
        assert "day_of_week" in result.columns

    def test_ridership_cast_to_int(self, sample_raw_df):
        result = clean_and_transform(sample_raw_df)
        assert result["ridership"].dtype in [int, "int64", "int32"]

    def test_ingested_at_column_added(self, sample_raw_df):
        result = clean_and_transform(sample_raw_df)
        assert "_ingested_at" in result.columns

    def test_no_fully_null_rows(self, sample_raw_df):
        # Add a fully null row
        df_with_null = sample_raw_df.copy()
        df_with_null.loc[len(df_with_null)] = [None] * len(df_with_null.columns)
        result = clean_and_transform(df_with_null)
        # Should not contain fully null rows
        assert not result.isnull().all(axis=1).any()

    def test_empty_dataframe_handled_gracefully(self, empty_df):
        """Pipeline should not crash on empty input."""
        result = clean_and_transform(empty_df)
        assert isinstance(result, pd.DataFrame)


# ── upload_to_s3 tests ────────────────────────────────────────────────────────

class TestUploadToS3:

    def test_successful_upload_returns_true(self):
        mock_s3 = MagicMock()
        mock_s3.put_object.return_value = {}
        result = upload_to_s3(mock_s3, b"test,data\n1,2", "raw/test.csv")
        assert result is True
        mock_s3.put_object.assert_called_once()

    def test_s3_error_returns_false(self):
        from botocore.exceptions import ClientError
        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchBucket", "Message": "The bucket does not exist"}},
            "PutObject"
        )
        result = upload_to_s3(mock_s3, b"data", "raw/test.csv")
        assert result is False

    def test_correct_bucket_and_key_used(self):
        mock_s3 = MagicMock()
        upload_to_s3(mock_s3, b"data", "raw/subway/test.csv", "text/csv")
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Key"] == "raw/subway/test.csv"
        assert call_kwargs["ContentType"] == "text/csv"
        assert call_kwargs["Body"] == b"data"


# ── run_pipeline integration tests ───────────────────────────────────────────

class TestRunPipeline:

    @patch("pipeline.fetch_mta_data")
    def test_dry_run_returns_success(self, mock_fetch, sample_raw_df):
        mock_fetch.return_value = sample_raw_df
        report = run_pipeline(dry_run=True)
        assert report["status"] == "success"
        assert report["rows_fetched"] == 4
        assert report["rows_processed"] > 0

    @patch("pipeline.fetch_mta_data")
    def test_report_contains_required_keys(self, mock_fetch, sample_raw_df):
        mock_fetch.return_value = sample_raw_df
        report = run_pipeline(dry_run=True)
        required = ["run_id", "timestamp", "status", "rows_fetched",
                    "rows_processed", "validation", "s3_keys", "errors"]
        for key in required:
            assert key in report

    @patch("pipeline.fetch_mta_data")
    def test_network_error_returns_failed_status(self, mock_fetch):
        import requests
        mock_fetch.side_effect = requests.RequestException("Connection refused")
        report = run_pipeline(dry_run=True)
        assert report["status"] == "failed"
        assert len(report["errors"]) > 0

    @patch("pipeline.fetch_mta_data")
    def test_run_id_is_timestamped(self, mock_fetch, sample_raw_df):
        mock_fetch.return_value = sample_raw_df
        report = run_pipeline(dry_run=True)
        # run_id format: YYYYMMDD_HHMMSS
        assert len(report["run_id"]) == 15
        assert "_" in report["run_id"]

    @patch("pipeline.fetch_mta_data")
    def test_dry_run_does_not_call_s3(self, mock_fetch, sample_raw_df):
        mock_fetch.return_value = sample_raw_df
        with patch("pipeline.get_s3_client") as mock_s3_client:
            run_pipeline(dry_run=True)
            mock_s3_client.assert_not_called()

    @patch("pipeline.fetch_mta_data")
    def test_checksum_generated_on_success(self, mock_fetch, sample_raw_df):
        mock_fetch.return_value = sample_raw_df
        report = run_pipeline(dry_run=True)
        assert "raw_checksum" in report
        assert len(report["raw_checksum"]) == 32  # MD5 hex length
