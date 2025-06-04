"""
Test the entity mapping functions.
"""

from unittest import TestCase

from semantic_medallion_data_platform.silver.slv_03_transform_entity_to_entity_mapping import (
    fuzzy_match_entities,
)


class TestEntityMapping(TestCase):
    """Test the entity mapping functions."""

    def test_fuzzy_match_entities_exact_match(self):
        """Test fuzzy matching with exact match."""
        result = fuzzy_match_entities("Apple Inc.", "Apple Inc.", threshold=80)
        self.assertTrue(result["match"])
        self.assertEqual(result["match_string"], "Apple Inc. == Apple Inc.")
        self.assertEqual(result["score"], 100)

    def test_fuzzy_match_entities_close_match(self):
        """Test fuzzy matching with close match."""
        result = fuzzy_match_entities("Apple Inc", "Apple Inc.", threshold=80)
        self.assertTrue(result["match"])
        self.assertEqual(result["match_string"], "Apple Inc == Apple Inc.")
        self.assertTrue(result["score"] >= 80)

    def test_fuzzy_match_entities_no_match(self):
        """Test fuzzy matching with no match."""
        result = fuzzy_match_entities(
            "Apple Inc.", "Microsoft Corporation", threshold=80
        )
        self.assertFalse(result["match"])
        self.assertIsNone(result["match_string"])
        self.assertTrue(result["score"] < 80)

    def test_fuzzy_match_entities_threshold(self):
        """Test fuzzy matching with different thresholds."""
        # These entities are similar but not identical
        entity1 = "New York City"
        entity2 = "New York"

        # With a high threshold, they shouldn't match
        result_high = fuzzy_match_entities(entity1, entity2, threshold=90)
        self.assertFalse(result_high["match"])

        # With a lower threshold, they should match
        result_low = fuzzy_match_entities(entity1, entity2, threshold=70)
        self.assertTrue(result_low["match"])

        # Both should have the same score
        self.assertEqual(result_high["score"], result_low["score"])
