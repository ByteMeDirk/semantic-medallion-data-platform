"""
This script is part of the Semantic Medallion Data Platform project.
It processes known entities from the bronze layer, extracts additional entities from their descriptions using spaCy NLP,
and writes the results to the silver layer in PostgreSQL.

The script performs two main operations:
1. Copies the known entities data as-is from bronze.known_entities to silver.known_entities
2. Extracts entities (locations, organizations, persons) from entity descriptions using NLP
   and writes them to silver.known_entities_entities
"""
import argparse

from pyspark.sql import functions as F

from semantic_medallion_data_platform.bronze.brz_01_extract_known_entities import (
    create_spark_session,
)
from semantic_medallion_data_platform.common.log_handler import get_logger
from semantic_medallion_data_platform.common.nlp import (
    ENTITIES_SCHEMA,
    extract_entities,
)
from semantic_medallion_data_platform.config.env import get_db_config

logger = get_logger(__name__)

# Create a UDF from the extract_entities function
extract_entities_udf = F.udf(extract_entities, ENTITIES_SCHEMA)


def main() -> None:
    """Main entry point for the script."""
    try:
        # Get database configuration
        db_config = get_db_config()
        jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}"

        # Read the bronze newsapi articles
        brz_known_entities_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "bronze.known_entities")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        # Write the data as is to the silver layer (the data is already in the correct format)
        logger.info(f"Writing known entities to {jdbc_url} in silver.known_entities")
        (
            brz_known_entities_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "silver.known_entities")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )

        # Extract entities from "entity_description" column
        logger.info("Extracting entities from 'entity_description' column")
        brz_known_entities_df = brz_known_entities_df.withColumn(
            "entities", extract_entities_udf(F.col("entity_description"))
        )

        # Select relevant columns for processing
        logger.info("Selecting NLP data for exploding")
        brz_known_entities_df = brz_known_entities_df.select(
            "uri", "entity_description", "entities"
        )

        # Explode the entities column
        logger.info("Exploding entities column")
        brz_known_entities_df = brz_known_entities_df.withColumn(
            "entity", F.explode(F.col("entities"))
        )

        # Flatten the exploded entities
        logger.info("Flattening exploded entities")
        brz_known_entities_df = brz_known_entities_df.select(
            "uri",
            F.col("entity.text").alias("entity_text"),
            F.col("entity.type").alias("entity_type"),
        )

        # Replace the 'GPE' type with 'LOC'
        logger.info("Replacing 'GPE' type with 'LOC'")
        brz_known_entities_df = brz_known_entities_df.withColumn(
            "entity_type",
            F.when(F.col("entity_type") == "GPE", "LOC").otherwise(
                F.col("entity_type")
            ),
        )

        # Remove duplicates
        logger.info("Removing duplicates from the DataFrame")
        brz_known_entities_df = brz_known_entities_df.dropDuplicates(
            ["uri", "entity_text", "entity_type"]
        )

        # Write the transformed data to the silver layer
        logger.info(
            f"Writing transformed known entities to {jdbc_url} in silver.known_entities_entities"
        )
        (
            brz_known_entities_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "silver.known_entities_entities")
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
    spark = create_spark_session("slv_02_transform_nlp_known_entities")
    parser = argparse.ArgumentParser(description="slv_02_transform_nlp_known_entities")

    args = parser.parse_args()

    logger.info("Starting spark pipeline.")
    main()
    logger.info("Spark pipeline completed successfully!")
