"""
Template
"""
import argparse

from semantic_medallion_data_platform.bronze.brz_01_extract_known_entities import (
    create_spark_session,
)
from semantic_medallion_data_platform.common.log_handler import get_logger

logger = get_logger(__name__)


def main() -> None:
    """Main entry point for the script."""
    try:
        pass
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
