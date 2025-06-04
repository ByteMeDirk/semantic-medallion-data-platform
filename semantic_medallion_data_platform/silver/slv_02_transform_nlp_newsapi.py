"""
Template
"""
import argparse

import spacy
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from semantic_medallion_data_platform.bronze.brz_01_extract_known_entities import (
    create_spark_session,
)
from semantic_medallion_data_platform.common.log_handler import get_logger
from semantic_medallion_data_platform.config.env import get_db_config

logger = get_logger(__name__)
NLP = spacy.load("en_core_web_lg")

ENTITY_STRUCT = StructType(
    [StructField("text", StringType(), True), StructField("type", StringType(), True)]
)

ENTITIES_SCHEMA = ArrayType(ENTITY_STRUCT)


@F.udf(ENTITIES_SCHEMA)
def extract_entities(text: str) -> list:
    """
    Extract location, organization, and person entities from text using spaCy.

    Args:
        text: The input text to process

    Returns:
        A list of dictionaries with entity text and type
    """
    if not text:
        return []

    doc = NLP(text)
    entities = [
        {"text": ent.text, "type": ent.label_}
        for ent in doc.ents
        if ent.label_ in ("LOC", "GPE", "ORG", "PERSON")
    ]
    return entities


def main() -> None:
    """Main entry point for the script."""
    try:
        # Get database configuration
        db_config = get_db_config()
        jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}"

        # Read the bronze newsapi articles
        brz_newsapi_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "bronze.newsapi")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        # Write the data as is to the silver layer (the data is already in the correct format)
        logger.info(f"Writing newsapi articles to {jdbc_url} in silver.newsapi")
        (
            brz_newsapi_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "silver.newsapi")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")  # Overwrite the table if it exists
            .save()
        )

        # Extract entities from "title", "description", and "content" columns
        logger.info("Extracting entities from newsapi articles")
        entity_columns = ["title", "description", "content"]
        for col in entity_columns:
            brz_newsapi_df = brz_newsapi_df.withColumn(
                f"{col}_entities", extract_entities(F.col(col))
            )

        # Select relevant columns for processing
        logger.info("Selecting NLP data for exploding")
        brz_newsapi_df = brz_newsapi_df.select(
            "uri", *[f"{col}_entities" for col in entity_columns]
        )

        # Explode the entities columns
        logger.info("Exploding entities columns")
        for col in [f"{col}_entities" for col in entity_columns]:
            brz_newsapi_df = brz_newsapi_df.withColumn(col, F.explode(F.col(col)))

        # Flatten the exploded entities
        logger.info("Flattening exploded entities")
        brz_newsapi_df = (
            brz_newsapi_df.select(
                "uri",
                F.col("title_entities.text").alias("entity_text"),
                F.col("title_entities.type").alias("entity_type"),
            )
            .union(
                brz_newsapi_df.select(
                    "uri",
                    F.col("description_entities.text").alias("entity_text"),
                    F.col("description_entities.type").alias("entity_type"),
                )
            )
            .union(
                brz_newsapi_df.select(
                    "uri",
                    F.col("content_entities.text").alias("entity_text"),
                    F.col("content_entities.type").alias("entity_type"),
                )
            )
        )

        # Replace the 'GPE' type with 'LOC'
        logger.info("Replacing 'GPE' type with 'LOC'")
        brz_newsapi_df = brz_newsapi_df.withColumn(
            "entity_type",
            F.when(F.col("entity_type") == "GPE", "LOC").otherwise(
                F.col("entity_type")
            ),
        )

        # Remove duplicates
        logger.info("Removing duplicate entities")
        brz_newsapi_df = brz_newsapi_df.dropDuplicates(
            ["uri", "entity_text", "entity_type"]
        )

        # Write the transformed data to the silver layer
        logger.info(
            f"Writing transformed newsapi entities to {jdbc_url} in silver.newsapi_entities"
        )
        (
            brz_newsapi_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "silver.newsapi_entities")
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
    spark = create_spark_session("template")
    parser = argparse.ArgumentParser(description="template")

    args = parser.parse_args()

    logger.info("Starting spark pipeline.")
    main()
    logger.info("Spark pipeline completed successfully!")
