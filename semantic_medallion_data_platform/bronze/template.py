"""

"""

from pyspark.sql import SparkSession

from semantic_medallion_data_platform.common.log_handler import get_logger

logger = get_logger(__name__)


def create_spark_session(app_name: str) -> SparkSession:
    """Create a Spark session connected to the Docker Spark cluster.

    Args:
        app_name: The name of the Spark application.

    Returns:
        A configured SparkSession object.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master("spark://spark-master:7077")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def main() -> None:
    """Main entry point for the script."""
    # Create Spark session
    spark = create_spark_session("brz_01_extract_kaggle_datasets")

    try:
        pass
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
