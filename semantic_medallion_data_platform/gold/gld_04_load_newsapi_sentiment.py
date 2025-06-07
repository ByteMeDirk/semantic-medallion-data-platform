"""
Entity to NewsAPI Sentiment Analysis
This script creates a gold layer table that maps known entities to NewsAPI sources with sentiment analysis,
providing a comprehensive view of entities and the sentiment of mentions about them.
"""
import argparse

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
            .dropDuplicates()
        )

        # Load newsapi sentiment data
        logger.info("Loading newsapi sentiment data")
        slv_newsapi_sentiment_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "silver.newsapi_sentiment")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .load()
            .dropDuplicates()
        )

        # Create a wide table by joining entity_to_source_mapping with known_entities, newsapi, and newsapi_sentiment
        logger.info("Creating entity to newsapi sentiment analysis table")
        entity_to_newsapi_sentiment_df = (
            slv_newsapi_df.join(
                slv_newsapi_sentiment_df,
                slv_newsapi_df.uri == slv_newsapi_sentiment_df.uri,
            )
            .select(
                slv_newsapi_df.uri.alias("uri"),
                slv_newsapi_df.title.alias("title"),
                slv_newsapi_sentiment_df.sentiment_score.alias("sentiment_score"),
                slv_newsapi_sentiment_df.sentiment_label.alias("sentiment_label"),
            )
            .dropDuplicates()
        )

        # Show sample of the data
        logger.info("Sample of entity to newsapi sentiment analysis table:")
        entity_to_newsapi_sentiment_df.show(5, truncate=False)

        # Write the wide table to the gold layer
        logger.info(
            "Writing entity to newsapi sentiment analysis table to gold.entity_to_newsapi_sentiment"
        )
        (
            entity_to_newsapi_sentiment_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "gold.entity_to_newsapi_sentiment")
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
    spark = create_spark_session("gld_04_entity_to_newsapi_sentiment")
    parser = argparse.ArgumentParser(
        description="Create entity to newsapi sentiment analysis table for reporting"
    )

    args = parser.parse_args()

    logger.info("Starting spark pipeline.")
    main()
    logger.info("Spark pipeline completed successfully!")
