"""
Test the environment configuration module.
"""

import os
from unittest import TestCase, mock

from semantic_medallion_data_platform.config.env import get_db_config, get_env_var


class TestEnvConfig(TestCase):
    """Test the environment configuration module."""

    @mock.patch.dict(os.environ, {"TEST_VAR": "test_value"})
    def test_get_env_var_existing(self):
        """Test getting an existing environment variable."""
        value = get_env_var("TEST_VAR")
        self.assertEqual(value, "test_value")

    @mock.patch.dict(os.environ, {})
    def test_get_env_var_default(self):
        """Test getting a non-existing environment variable with a default value."""
        value = get_env_var("TEST_VAR", "default_value")
        self.assertEqual(value, "default_value")

    @mock.patch.dict(os.environ, {})
    def test_get_env_var_missing(self):
        """Test getting a non-existing environment variable without a default value."""
        with self.assertRaises(ValueError):
            get_env_var("TEST_VAR")

    @mock.patch.dict(
        os.environ,
        {
            "DB_HOST": "test_host",
            "DB_PORT": "5432",
            "DB_NAME": "test_db",
            "DB_USER": "test_user",
            "DB_PASSWORD": "test_password",
        },
    )
    def test_get_db_config(self):
        """Test getting the database configuration."""
        config = get_db_config()
        self.assertEqual(
            config,
            {
                "host": "test_host",
                "port": "5432",
                "database": "test_db",
                "user": "test_user",
                "password": "test_password",
            },
        )
