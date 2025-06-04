"""
This script is part of the Semantic Medallion Data Platform project.
It extracts the Goyaladi Twitter dataset from Kaggle, processes it, and writes it to a PostgreSQL database in the bronze schema.
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

from semantic_medallion_data_platform.common.log_handler import get_logger
from semantic_medallion_data_platform.common.pyspark import (
    composite_to_hash,
    create_spark_session,
)
from semantic_medallion_data_platform.config.env import get_db_config

logger = get_logger(__name__)

DATASET_SCHEMA = StructType(
    [
        # entity_name,entity_type,entity_description
        StructField("entity_name", StringType(), True),
        StructField("entity_type", StringType(), True),
        StructField("entity_description", StringType(), True),
    ]
)


def main() -> None:
    """Main entry point for the script."""
    _composite_to_hash_udf = F.udf(lambda *cols: composite_to_hash(*cols), StringType())
    try:
        # Read the known entities datasets
        logger.info(f"Reading data from {_raw_data_filepath}")
        known_entities_df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", ",")
            .option("multiline", "true")
            .schema(DATASET_SCHEMA)
            .load(_raw_data_filepath)
        )

        # Sort by name
        known_entities_df = known_entities_df.sort("entity_name")
        logger.info(f"Found {known_entities_df.count()} entities")

        # Add a composite hash column with name and type
        known_entities_df = known_entities_df.withColumn(
            "uri",
            _composite_to_hash_udf("entity_name", "entity_type"),
        )

        # Reorder columns
        known_entities_df = known_entities_df.select(
            "uri", "entity_name", "entity_type", "entity_description"
        )

        # Write to Postgres Bronze layer
        db_config = get_db_config()
        jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}"
        logger.info(f"Writing known entities to {jdbc_url} in bronze.known_entities")
        (
            known_entities_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "bronze.known_entities")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")  # Overwrite the table if it exists
            .save()
        )

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e  # Re-raise the exception after logging it
    finally:
        # Stop Spark session
        spark.stop()


if __name__ == "__main__":
    spark = create_spark_session("brz_01_extract_known_entities")
    parser = argparse.ArgumentParser(description="brz_01_extract_known_entities")
    parser.add_argument(
        "--raw_data_filepath",
        type=str,
        help="Path to the raw data files for ingestion",
    )

    args = parser.parse_args()
    _raw_data_filepath = args.raw_data_filepath

    logger.info("Starting spark pipeline.")
    main()
    logger.info("Spark pipeline completed successfully!")
