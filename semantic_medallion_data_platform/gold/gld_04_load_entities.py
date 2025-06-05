"""
Entity Affiliations Wide Table
This script creates a wide table for reporting that shows entity information and their fuzzy match affiliations.
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

        # Load entity to entity mapping data
        logger.info("Loading entity to entity mapping data")
        slv_entity_to_entity_mapping_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "silver.entity_to_entity_mapping")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )

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

        # Create a wide table by joining entity_to_entity_mapping with known_entities twice
        # First join for entity1 information
        logger.info("Creating wide table with entity affiliations")
        entity_affiliations_df = (
            slv_entity_to_entity_mapping_df.join(
                slv_known_entities_df.alias("entity1"),
                slv_entity_to_entity_mapping_df.known1_uri == F.col("entity1.uri"),
            )
            # Second join for entity2 information
            .join(
                slv_known_entities_df.alias("entity2"),
                slv_entity_to_entity_mapping_df.known2_uri == F.col("entity2.uri"),
            )
            # Select and rename columns for clarity
            .select(
                F.col("entity1.uri").alias("entity_uri"),
                F.col("entity1.entity_name").alias("entity_name"),
                F.col("entity1.entity_type").alias("entity_type"),
                F.col("entity1.entity_description").alias("entity_description"),
                F.col("entity2.uri").alias("affiliated_entity_uri"),
                F.col("entity2.entity_name").alias("affiliated_entity_name"),
                F.col("entity2.entity_type").alias("affiliated_entity_type"),
                F.col("entity2.entity_description").alias(
                    "affiliated_entity_description"
                ),
                F.col("match_string").alias("match_details"),
                F.col("score").alias("match_score"),
            )
        )

        # Show sample of the data
        logger.info("Sample of entity affiliations wide table:")
        entity_affiliations_df.show(5, truncate=False)

        # Write the wide table to the gold layer
        logger.info(
            "Writing entity affiliations wide table to gold.entity_affiliations"
        )
        (
            entity_affiliations_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "gold.entity_affiliations")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )

        # Also create the reverse relationship table for completeness
        logger.info("Creating reverse relationship table")
        reverse_entity_affiliations_df = (
            slv_entity_to_entity_mapping_df.join(
                slv_known_entities_df.alias("entity2"),
                slv_entity_to_entity_mapping_df.known2_uri == F.col("entity2.uri"),
            )
            .join(
                slv_known_entities_df.alias("entity1"),
                slv_entity_to_entity_mapping_df.known1_uri == F.col("entity1.uri"),
            )
            .select(
                F.col("entity2.uri").alias("entity_uri"),
                F.col("entity2.entity_name").alias("entity_name"),
                F.col("entity2.entity_type").alias("entity_type"),
                F.col("entity2.entity_description").alias("entity_description"),
                F.col("entity1.uri").alias("affiliated_entity_uri"),
                F.col("entity1.entity_name").alias("affiliated_entity_name"),
                F.col("entity1.entity_type").alias("affiliated_entity_type"),
                F.col("entity1.entity_description").alias(
                    "affiliated_entity_description"
                ),
                F.col("match_string").alias("match_details"),
                F.col("score").alias("match_score"),
            )
        )

        # Combine both directions into a single comprehensive table
        logger.info(
            "Combining both directions into a comprehensive entity affiliations table"
        )
        complete_entity_affiliations_df = entity_affiliations_df.union(
            reverse_entity_affiliations_df
        )

        # Write the comprehensive table to the gold layer
        logger.info(
            "Writing comprehensive entity affiliations table to gold.entity_affiliations_complete"
        )
        (
            complete_entity_affiliations_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "gold.entity_affiliations_complete")
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
    spark = create_spark_session("gld_04_entity_affiliations")
    parser = argparse.ArgumentParser(
        description="Create entity affiliations wide table for reporting"
    )

    args = parser.parse_args()

    logger.info("Starting spark pipeline.")
    main()
    logger.info("Spark pipeline completed successfully!")
