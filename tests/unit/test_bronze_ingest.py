"""Unit tests for the bronze layer ingestion functionality."""

import os
import tempfile

import pandas as pd
import pytest
from pyspark.sql import DataFrame

# This is a placeholder import - the actual module will be created later
from semantic_medallion_data_platform.bronze import ingest


def test_csv_ingestion(spark_session, gcs_emulator):
    """Test ingesting a CSV file into the bronze layer."""
    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
        # Create sample data
        sample_data = pd.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
        )

        # Write to CSV
        sample_data.to_csv(temp_file.name, index=False)

    try:
        # Test the ingestion function
        result_df = ingest.from_csv(spark_session, temp_file.name, "test_table")

        # Verify the result
        assert isinstance(result_df, DataFrame)
        assert result_df.count() == 3
        assert len(result_df.columns) == 3

        # Check that the data was correctly ingested
        pandas_df = result_df.toPandas()
        assert pandas_df["id"].tolist() == [1, 2, 3]
        assert pandas_df["name"].tolist() == ["Alice", "Bob", "Charlie"]
        assert pandas_df["age"].tolist() == [25, 30, 35]

    finally:
        # Clean up the temporary file
        os.unlink(temp_file.name)


def test_json_ingestion(spark_session, gcs_emulator):
    """Test ingesting a JSON file into the bronze layer."""
    # Create a temporary JSON file
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as temp_file:
        # Create sample data
        sample_data = pd.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
        )

        # Write to JSON
        sample_data.to_json(temp_file.name, orient="records", lines=True)

    try:
        # Test the ingestion function
        result_df = ingest.from_json(spark_session, temp_file.name, "test_table")

        # Verify the result
        assert isinstance(result_df, DataFrame)
        assert result_df.count() == 3
        assert len(result_df.columns) == 3

        # Check that the data was correctly ingested
        pandas_df = result_df.toPandas()
        assert pandas_df["id"].tolist() == [1, 2, 3]
        assert pandas_df["name"].tolist() == ["Alice", "Bob", "Charlie"]
        assert pandas_df["age"].tolist() == [25, 30, 35]

    finally:
        # Clean up the temporary file
        os.unlink(temp_file.name)
