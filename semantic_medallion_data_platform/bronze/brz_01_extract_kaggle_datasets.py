"""

"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

from semantic_medallion_data_platform.common.log_handler import get_logger
from semantic_medallion_data_platform.common.pyspark import composite_to_hash
from semantic_medallion_data_platform.config.env import get_db_config

logger = get_logger(__name__)

GOYALADI_TWITTER_DATASET_SCHEMA = StructType(
    [
        StructField("Tweet_ID", StringType(), True),
        StructField("Username", StringType(), True),
        StructField("Text", StringType(), True),
        StructField("Retweets", IntegerType(), True),
        StructField("Likes", IntegerType(), True),
        StructField("Timestamp", StringType(), True),
    ]
)

# Register the composite_to_hash function as a UDF
_composite_to_hash_udf = F.udf(lambda *cols: composite_to_hash(*cols), StringType())


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


def extract_goyaladi_twitter_dataset(spark: SparkSession) -> None:
    # Extracting data/goyaladi-twitter-dataset.csv
    logger.info("Extracting data/goyaladi-twitter-dataset.csv from Kaggle datasets.")
    goyaladi_twitter_dataset_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", ",")
        .option("multiline", "true")
        .schema(GOYALADI_TWITTER_DATASET_SCHEMA)
        .load("data/goyaladi-twitter-dataset.csv")
    )

    # Clean up the data
    goyaladi_twitter_dataset_df = (
        goyaladi_twitter_dataset_df.withColumnRenamed("Tweet_ID", "tweet_id")
        .withColumnRenamed("Username", "username")
        .withColumnRenamed("Text", "text")
        .withColumnRenamed("Retweets", "retweets")
        .withColumnRenamed("Likes", "likes")
        .withColumnRenamed("Timestamp", "timestamp")
    )

    goyaladi_twitter_dataset_df = goyaladi_twitter_dataset_df.withColumn(
        "timestamp", goyaladi_twitter_dataset_df["Timestamp"].cast(TimestampType())
    )

    # Add a unique ID column using the composite_to_hash function
    goyaladi_twitter_dataset_df = goyaladi_twitter_dataset_df.withColumn(
        "uid",
        _composite_to_hash_udf(
            goyaladi_twitter_dataset_df["tweet_id"],
            goyaladi_twitter_dataset_df["username"],
            goyaladi_twitter_dataset_df["text"],
        ),
    )

    # Re-order the columns
    goyaladi_twitter_dataset_df = goyaladi_twitter_dataset_df.select(
        "uid", "tweet_id", "username", "text", "retweets", "likes", "timestamp"
    )

    # Write to Postgres bronze schema
    logger.info("Writing goyaladi_twitter_dataset_df to Postgres bronze schema.")

    # Get database configuration from environment variables
    db_config = get_db_config()

    # Construct JDBC URL
    jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}"

    goyaladi_twitter_dataset_df.write.format("jdbc").option("url", jdbc_url).option(
        "dbtable", "bronze.goyaladi_twitter_dataset"
    ).option("user", db_config["user"]).option(
        "password", db_config["password"]
    ).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "overwrite"
    ).save()


def main() -> None:
    """Main entry point for the script."""
    # Create Spark session
    spark = create_spark_session("brz_01_extract_kaggle_datasets")

    try:
        extract_goyaladi_twitter_dataset(spark)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e  # Re-raise the exception after logging it
    finally:
        # Stop Spark session
        spark.stop()


if __name__ == "__main__":
    logger.info("Starting spark pipeline.")
    main()
    logger.info("Spark pipeline completed successfully!")
