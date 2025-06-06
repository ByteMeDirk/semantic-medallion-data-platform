"""
Entity to NewsAPI Source Resolution
This script creates a gold layer table that maps known entities to NewsAPI sources,
providing a comprehensive view of entities and mentions about them semantically.
"""
import argparse

from pyspark.sql import functions as F

from semantic_medallion_data_platform.bronze.brz_01_extract_known_entities import (
    create_spark_session,
)
from semantic_medallion_data_platform.common.log_handler import get_logger
from semantic_medallion_data_platform.config.env import get_db_config

logger = get_logger(__name__)


def main() -> None:
    """Main entry point for the script."""
    try:
        # Get database configuration
        db_config = get_db_config()
        jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}"

        # Load known entities data
        logger.info("Loading known entities data")
        slv_known_entities_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "silver.known_entities")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        # Load newsapi data
        logger.info("Loading newsapi data")
        slv_newsapi_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "silver.newsapi")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        # Load entity to source mapping data
        logger.info("Loading entity to source mapping data")
        slv_entity_to_source_mapping_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "silver.entity_to_source_mapping")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        # Create a wide table by joining entity_to_source_mapping with known_entities and newsapi
        logger.info("Creating entity to newsapi source resolution table")
        entity_to_newsapi_df = (
            slv_entity_to_source_mapping_df.join(
                slv_known_entities_df,
                slv_entity_to_source_mapping_df.known_uri == slv_known_entities_df.uri,
            )
            .join(
                slv_newsapi_df,
                slv_entity_to_source_mapping_df.newsapi_uri == slv_newsapi_df.uri,
            )
            .select(
                slv_known_entities_df.uri.alias("entity_uri"),
                slv_known_entities_df.entity_name.alias("entity_name"),
                slv_known_entities_df.entity_type.alias("entity_type"),
                slv_known_entities_df.entity_description.alias("entity_description"),
                slv_newsapi_df.uri.alias("source_uri"),
                slv_newsapi_df.title.alias("source_title"),
                slv_newsapi_df.description.alias("source_description"),
                slv_newsapi_df.url.alias("source_url"),
                slv_newsapi_df.source_name.alias("source_name"),
                slv_newsapi_df.published_at.alias("source_published_at"),
                F.col("match_string").alias("match_details"),
                F.col("score").alias("match_score"),
            )
        )

        # Show sample of the data
        logger.info("Sample of entity to newsapi source resolution table:")
        entity_to_newsapi_df.show(5, truncate=False)

        # Write the wide table to the gold layer
        logger.info(
            "Writing entity to newsapi source resolution table to gold.entity_to_newsapi"
        )
        (
            entity_to_newsapi_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "gold.entity_to_newsapi")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e  # Re-raise the exception after logging it
    finally:
        # Stop Spark session
        spark.stop()


if __name__ == "__main__":
    spark = create_spark_session("gld_04_entity_to_newsapi")
    parser = argparse.ArgumentParser(
        description="Create entity to newsapi source resolution table for reporting"
    )

    args = parser.parse_args()

    logger.info("Starting spark pipeline.")
    main()
    logger.info("Spark pipeline completed successfully!")
