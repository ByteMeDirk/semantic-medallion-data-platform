"""
This script is part of the Semantic Medallion Data Platform project.
It creates entity mappings between different data sources in the silver layer using fuzzy matching.

The script performs two main operations:
1. Creates entity-to-source mappings by matching entities from known_entities_entities with entities from newsapi_entities
   and writes the results to silver.entity_to_source_mapping
2. Creates entity-to-entity mappings by matching entities within known_entities_entities
   and writes the results to silver.entity_to_entity_mapping

These mappings enable semantic connections between entities across different data sources.
"""
import argparse

import rapidfuzz

from semantic_medallion_data_platform.bronze.brz_01_extract_known_entities import (
    create_spark_session,
)
from semantic_medallion_data_platform.common.log_handler import get_logger
from semantic_medallion_data_platform.config.env import get_db_config

logger = get_logger(__name__)


def fuzzy_match_entities(entity1: str, entity2: str, threshold: int = 80) -> dict:
    """
    Perform fuzzy matching between two entities using RapidFuzz.

    Args:
        entity1 (str): The first entity to match.
        entity2 (str): The second entity to match.
        threshold (int): The minimum score for a match to be considered valid (0-100).

    Returns:
        dict: A dictionary containing:
            - match (bool): True if the entities match based on the threshold, False otherwise.
            - match_string (str or None): A string in the format "entity1 == entity2" if they match, None otherwise.
            - score (int): The similarity score (0-100) between the two entities.
    """
    score = rapidfuzz.fuzz.ratio(entity1, entity2)
    if score >= threshold:
        return {
            "match": True,
            "match_string": f"{entity1} == {entity2}",
            "score": score,
        }
    else:
        return {"match": False, "match_string": None, "score": score}


def main() -> None:
    """Main entry point for the script."""
    try:
        # Get database configuration
        db_config = get_db_config()
        jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}"

        slv_known_entities_entities_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "silver.known_entities_entities")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        slv_newapi_entities_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "silver.newsapi_entities")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        # Match known_entities_entities.entity_text with newsapi_entities.entity_text
        # Provide the uris for each along with the match score and match string
        matched_entities_df = (
            slv_known_entities_entities_df.alias("known")
            .crossJoin(slv_newapi_entities_df.alias("newsapi"))
            .select(
                "known.entity_text",
                "newsapi.entity_text",
                "known.uri",
                "newsapi.uri",
            )
            .rdd.map(
                lambda row: (
                    fuzzy_match_entities(
                        row[0],
                        row[
                            1
                        ],  # known.entity_text at index 0, newsapi.entity_text at index 1
                    )
                    | {
                        "known_uri": row[2],  # known.uri at index 2
                        "newsapi_uri": row[3],  # newsapi.uri at index 3
                    }
                )
            )
            .filter(lambda x: x["match"])
            .toDF()
        )

        # Write the map table to the silver layer
        logger.info(
            f"Writing matched entities to silver.entity_to_source_mapping table."
        )
        (
            matched_entities_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "silver.entity_to_source_mapping")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )

        # Match known_entities_entities.entity_text with other known_entities_entities.entity_text
        # This produces an entity to entity mapping
        matched_entities_to_entities_df = (
            slv_known_entities_entities_df.alias("known1")
            .crossJoin(slv_known_entities_entities_df.alias("known2"))
            .select(
                "known1.entity_text",
                "known2.entity_text",
                "known1.uri",
                "known2.uri",
            )
            .rdd.map(
                lambda row: (
                    fuzzy_match_entities(
                        row[0],
                        row[
                            1
                        ],  # known1.entity_text at index 0, known2.entity_text at index 1
                    )
                    | {
                        "known1_uri": row[2],  # known1.uri at index 2
                        "known2_uri": row[3],  # known2.uri at index 3
                    }
                )
            )
            .filter(lambda x: x["match"])
            .toDF()
        )

        # Drop where known1_uri == known2_uri
        matched_entities_to_entities_df = matched_entities_to_entities_df.filter(
            "known1_uri != known2_uri"
        )

        # Write the map table to the silver layer
        logger.info(
            f"Writing matched entities to silver.entity_to_entity_mapping table."
        )
        (
            matched_entities_to_entities_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "silver.entity_to_entity_mapping")
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
    spark = create_spark_session("slv_03_transform_entity_to_entity_mapping")
    parser = argparse.ArgumentParser(
        description="slv_03_transform_entity_to_entity_mapping"
    )

    args = parser.parse_args()

    logger.info("Starting spark pipeline.")
    main()
    logger.info("Spark pipeline completed successfully!")
