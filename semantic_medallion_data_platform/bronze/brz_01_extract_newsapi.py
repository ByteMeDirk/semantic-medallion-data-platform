"""
This script is part of the Semantic Medallion Data Platform project.
It extracts news articles related to known entities using the NewsAPI,
processes them, and writes them to a PostgreSQL database in the bronze schema.
"""
import argparse
from datetime import datetime, timedelta
from typing import Any, Dict, List

from newsapi import NewsApiClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

from semantic_medallion_data_platform.common.log_handler import get_logger
from semantic_medallion_data_platform.common.pyspark import composite_to_hash
from semantic_medallion_data_platform.config.env import get_db_config, get_env_var

logger = get_logger(__name__)

# Schema for the news articles
NEWSAPI_SCHEMA = StructType(
    [
        StructField("uri", StringType(), False),
        StructField("entity_uri", StringType(), True),
        StructField("entity_name", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("content", StringType(), True),
        StructField("url", StringType(), True),
        StructField("source_name", StringType(), True),
        StructField("author", StringType(), True),
        StructField("published_at", TimestampType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ]
)


def create_spark_session(app_name: str) -> SparkSession:
    """Create a Spark session for local development.

    Args:
        app_name: The name of the Spark application.

    Returns:
        A configured SparkSession object.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")  # Use local mode with all available cores
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )


def fetch_news_for_entity(
    newsapi_client: NewsApiClient, entity_name: str, days_back: int = 7
) -> List[Dict]:
    """Fetch news articles for a specific entity.

    Args:
        newsapi_client: The NewsAPI client.
        entity_name: The name of the entity to search for.
        days_back: Number of days to look back for articles.

    Returns:
        A list of news articles.
    """
    try:
        # Calculate the date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        # Format dates for NewsAPI
        from_date = start_date.strftime("%Y-%m-%d")
        to_date = end_date.strftime("%Y-%m-%d")

        # Query NewsAPI for articles related to the entity
        logger.info(
            f"Fetching news for entity: {entity_name} from {from_date} to {to_date}"
        )
        response = newsapi_client.get_everything(
            q=entity_name,
            from_param=from_date,
            to=to_date,
            language="en",
            sort_by="relevancy",
            page_size=100,  # Maximum allowed by NewsAPI
        )

        articles = response.get("articles", [])
        logger.info(f"Found {len(articles)} articles for entity: {entity_name}")
        return articles
    except Exception as e:
        logger.error(f"Error fetching news for entity {entity_name}: {e}")
        return []


def main() -> None:
    """Main entry point for the script."""
    _composite_to_hash_udf = F.udf(lambda *cols: composite_to_hash(*cols), StringType())

    try:
        # Initialize NewsAPI client
        newsapi_key = get_env_var("NEWSAPI_KEY")
        newsapi_client = NewsApiClient(api_key=newsapi_key)

        # Get database configuration
        db_config = get_db_config()
        jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}"

        # Read known entities from the database
        logger.info("Reading known entities from the database")
        known_entities_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "bronze.known_entities")
            .option("user", db_config["user"])
            .option("password", db_config["password"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        # Convert to Python list for processing
        entities = known_entities_df.select("uri", "entity_name").collect()
        logger.info(f"Found {len(entities)} entities to process")

        # Fetch news for each entity
        all_articles = []
        current_timestamp = datetime.now()

        for entity in entities:
            entity_uri = entity["uri"]
            entity_name = entity["entity_name"]

            articles = fetch_news_for_entity(
                newsapi_client, entity_name, days_back=_days_back
            )

            # Process each article and add to the list
            for article in articles:
                # Extract article data
                title = article.get("title", "")
                description = article.get("description", "")
                content = article.get("content", "")
                url = article.get("url", "")
                source_name = article.get("source", {}).get("name", "")
                author = article.get("author", "")
                published_at = article.get("publishedAt", "")

                # Create a unique URI for the article
                article_uri = composite_to_hash(url, title, entity_name)

                # Convert publishedAt to timestamp
                try:
                    published_timestamp = datetime.strptime(
                        published_at, "%Y-%m-%dT%H:%M:%SZ"
                    )
                except (ValueError, TypeError):
                    published_timestamp = None

                # Add to the list of articles
                all_articles.append(
                    {
                        "uri": article_uri,
                        "entity_uri": entity_uri,
                        "entity_name": entity_name,
                        "title": title,
                        "description": description,
                        "content": content,
                        "url": url,
                        "source_name": source_name,
                        "author": author,
                        "published_at": published_timestamp,
                        "ingestion_timestamp": current_timestamp,
                    }
                )

        # Convert the list of articles to a DataFrame
        if all_articles:
            logger.info(f"Creating DataFrame with {len(all_articles)} articles")
            news_df = spark.createDataFrame(all_articles, schema=NEWSAPI_SCHEMA)

            # Remove duplicates based on URI
            news_df = news_df.dropDuplicates(["uri"])
            logger.info(f"After removing duplicates: {news_df.count()} articles")

            # Write to Postgres Bronze layer
            logger.info(f"Writing news articles to {jdbc_url} in bronze.newsapi")
            (
                news_df.write.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", "bronze.newsapi")
                .option("user", db_config["user"])
                .option("password", db_config["password"])
                .option("driver", "org.postgresql.Driver")
                .mode("append")  # Append to the table if it exists
                .save()
            )
        else:
            logger.warning("No articles found for any entity")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e  # Re-raise the exception after logging it
    finally:
        # Stop Spark session
        spark.stop()


if __name__ == "__main__":
    spark = create_spark_session("brz_01_extract_newsapi")
    parser = argparse.ArgumentParser(description="brz_01_extract_newsapi")
    parser.add_argument(
        "--days_back",
        type=int,
        default=7,
        help="Number of days to look back for news articles",
    )

    args = parser.parse_args()
    _days_back = args.days_back

    logger.info("Starting spark pipeline.")
    main()
    logger.info("Spark pipeline completed successfully!")
