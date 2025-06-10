"""
This module provides utilities for hashing composite columns using SHA256.
"""

import hashlib
import logging
from typing import Any

from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    """Create a Spark session for local development.

    Args:
        app_name: The name of the Spark application.

    Returns:
        A configured SparkSession object.
    """

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def composite_to_hash(*columns: Any) -> str:
    """
    Generates a SHA256 hash from a composite set of column values.

    This function takes an arbitrary number of arguments, converts each to a string,
    concatenates them, and then computes a SHA256 hash of the resulting string.
    This is useful for creating a unique, fixed-length identifier for a record
    based on the values of multiple columns.

    Args:
        *columns (Any): Variable-length argument list representing the column values.
                        Each argument will be converted to a string before concatenation.
                        These can be of any type that can be safely converted to a string.

    Returns:
        str: A hexadecimal string representing the SHA256 hash of the concatenated
             column values. The hash will always be 64 characters long.

    Raises:
        TypeError: If any of the input `columns` cannot be reliably converted to a string.
                   While the `str()` function is robust, certain complex objects might
                   lead to unexpected string representations or errors during conversion.
        Exception: Catches and re-raises any other unexpected errors that occur during
                   the hashing process, providing an error log message.

    Example:
        >>> # Example with string and integer columns
        >>> hash_val_1 = composite_to_hash("user_id_123", 456, "active")
        >>> print(hash_val_1)
        # Expected output (will vary): 'e00b8c6e2a2d4f8b9d0c1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b'

        >>> # Example with a list and boolean
        >>> hash_val_2 = composite_to_hash("item_sku_789", [1, 2, 3], True)
        >>> print(hash_val_2)
        # Expected output (will vary): 'f11c9d7f3b4e5d6c7a8b9c0d1e2f3a4b5c6d7e8f8a9b0c1d2e3f4a5b6c7d8e9f'

        >>> # Example of error handling (conceptual - str() is very robust)
        >>> class CustomObject:
        ...     def __str__(self):
        ...         raise ValueError("Error during string conversion")
        >>> try:
        ...     composite_to_hash(CustomObject())
        ... except Exception as e:
        ...     print(f"Caught expected error: {e}")
    """
    try:
        # Concatenate the string representation of each column into a single string.
        # Using `str()` ensures that various data types (int, float, list, dict, etc.)
        # are converted to their string representation before concatenation.
        concatenated_string = "".join(str(column) for column in columns)

        # Encode the concatenated string to bytes using UTF-8.
        # Hashing algorithms operate on bytes, not strings.
        encoded_string = concatenated_string.encode("utf-8")

        # Generate a SHA256 hash object from the encoded string.
        hash_object = hashlib.sha256(encoded_string)

        # Return the hexadecimal representation of the hash.
        # This is a fixed-length string (64 characters for SHA256).
        return hash_object.hexdigest()
    except TypeError as e:
        # Catch specific TypeError in case str() conversion fails for some complex object
        logging.error(
            f"TypeError during column hashing: {e} - Ensure all columns are convertible to string."
        )
        raise TypeError(
            f"One or more columns could not be converted to a string for hashing: {e}"
        ) from e
    except Exception as e:
        # Catch any other unexpected errors during the hashing process.
        logging.error(
            f"An unexpected error occurred while hashing columns: {e}", exc_info=True
        )
        # Re-raise the exception after logging, to allow the calling code to handle it.
        raise e
