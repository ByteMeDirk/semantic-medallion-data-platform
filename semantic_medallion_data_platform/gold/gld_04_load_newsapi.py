"""
Template
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

        # Load Newsapi data
        logger.info("Loading Newsapi data")
        slv_newsapi_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "silver.newsapi")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        # Load Newsapi Entities data
        logger.info("Loading Newsapi Entities data")
        slv_newsapi_entities_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "silver.newsapi_entities")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        # Create a wide table by joining Newsapi entities with known entities
        logger.info("Creating wide table with Newsapi entities and known entities")
        newsapi_entities_wide_df = slv_newsapi_entities_df.join(
            slv_known_entities_df.alias("known_entity"),
            slv_newsapi_entities_df.uri == F.col("known_entity.uri"),
            "left",
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
