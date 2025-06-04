"""
Module for loading environment variables from .env file.
"""

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Get the project root directory
ROOT_DIR = Path(__file__).parent.parent.parent

# Load environment variables from .env file
load_dotenv(ROOT_DIR / ".env")


def get_env_var(var_name: str, default: Optional[str] = None) -> str:
    """
    Get an environment variable or return a default value.

    Args:
        var_name: The name of the environment variable.
        default: The default value to return if the environment variable is not set.

    Returns:
        The value of the environment variable or the default value.

    Raises:
        ValueError: If the environment variable is not set and no default value is provided.
    """
    value = os.getenv(var_name, default)
    if value is None:
        raise ValueError(
            f"Environment variable {var_name} is not set and no default value provided"
        )
    return value


# Database configuration
def get_db_config() -> dict:
    """
    Get the database configuration from environment variables.

    Returns:
        A dictionary containing the database configuration.
    """
    return {
        "host": get_env_var("DB_HOST"),
        "port": get_env_var("DB_PORT"),
        "database": get_env_var("DB_NAME"),
        "user": get_env_var("DB_USER"),
        "password": get_env_var("DB_PASSWORD"),
        "newsapi_key": get_env_var("NEWSAPI_KEY"),
    }
