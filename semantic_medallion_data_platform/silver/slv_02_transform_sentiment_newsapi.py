"""
This script is part of the Semantic Medallion Data Platform project.
It processes news articles from the silver layer, applies sentiment analysis to their content using BERT,
and writes the results to the silver layer in PostgreSQL.
"""
import argparse
import time

from pyspark.sql.types import StringType, StructField, StructType

from semantic_medallion_data_platform.bronze.brz_01_extract_known_entities import (
    create_spark_session,
)
from semantic_medallion_data_platform.common.log_handler import get_logger
from semantic_medallion_data_platform.common.nlp import (
    SENTIMENT_STRUCT,
    analyze_sentiment,
)
from semantic_medallion_data_platform.config.env import get_db_config

logger = get_logger(__name__)


def main() -> None:
    """Main entry point for the script."""
    try:
        # Get database configuration
        db_config = get_db_config()
        jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}"

        # Read the silver newsapi articles
        logger.info("Reading newsapi articles from silver.newsapi")
        slv_newsapi_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "silver.newsapi")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        slv_newsapi_df = slv_newsapi_df.select("uri", "title", "description", "content")

        # Iterate over each record to apply sentiment analysis
        # Pyspark API crashes when using UDFs with large datasets, so we will use a simple loop
        sentiment_scores = []
        for row in slv_newsapi_df.collect():
            uri = row["uri"]
            title = row["title"]
            content = row["content"]

            logger.info(f"Analyzing sentiment for article: {uri} - {title}")

            # Apply sentiment analysis
            content_sentiment = analyze_sentiment(content)
            sentiment_scores.append(
                {
                    "uri": uri,
                    "title": title,
                    "sentiment": content_sentiment,
                }
            )

            # Slow down for HuggingFace API rate limits
            time.sleep(5)  # Adjust sleep time as needed

        # Create a DataFrame from the sentiment scores
        sentiment_df = spark.createDataFrame(
            sentiment_scores,
            StructType(
                [
                    StructField("uri", StringType(), True),
                    StructField("title", StringType(), True),
                    StructField("sentiment", SENTIMENT_STRUCT, True),
                ]
            ),
        )

        # Explode the sentiment column to separate score and label
        sentiment_df = sentiment_df.select(
            "uri",
            "title",
            sentiment_df.sentiment["score"].alias("sentiment_score"),
            sentiment_df.sentiment["label"].alias("sentiment_label"),
        )

        # Write the sentiment results back to silver layer
        logger.info("Writing sentiment results to silver.sentiment_newsapi")
        (
            sentiment_df.write.format("jdbc")
            .mode("overwrite")
            .option("url", jdbc_url)
            .option("dbtable", "silver.newsapi_sentiment")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .save()
        )

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    spark = create_spark_session("slv_02_transform_sentiment_newsapi")
    parser = argparse.ArgumentParser(description="slv_02_transform_sentiment_newsapi")
    args = parser.parse_args()

    logger.info("Starting spark pipeline.")
    main()
    logger.info("Spark pipeline completed successfully!")
