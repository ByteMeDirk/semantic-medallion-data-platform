"""
Test the NLP functions.
"""

from unittest import TestCase

from semantic_medallion_data_platform.common.nlp import extract_entities


class TestNLP(TestCase):
    """Test the NLP functions."""

    def test_extract_entities_empty(self):
        """Test entity extraction with empty text."""
        result = extract_entities("")
        self.assertEqual(result, [])

    def test_extract_entities_none(self):
        """Test entity extraction with None."""
        result = extract_entities(None)
        self.assertEqual(result, [])

    def test_extract_entities_person(self):
        """Test entity extraction with person entity."""
        text = "Tim Cook is the CEO of Apple Inc."
        result = extract_entities(text)

        # Check that we found at least one person entity
        person_entities = [entity for entity in result if entity["type"] == "PERSON"]
        self.assertTrue(len(person_entities) > 0)

        # Check that "Tim Cook" is identified as a person
        tim_cook_found = any(
            entity["text"].lower() == "tim cook" and entity["type"] == "PERSON"
            for entity in result
        )
        self.assertTrue(tim_cook_found)

    def test_extract_entities_organization(self):
        """Test entity extraction with organization entity."""
        text = "Apple Inc. is a technology company based in Cupertino."
        result = extract_entities(text)

        # Check that we found at least one organization entity
        org_entities = [entity for entity in result if entity["type"] == "ORG"]
        self.assertTrue(len(org_entities) > 0)

        # Check that "Apple Inc." is identified as an organization
        apple_found = any(
            "apple" in entity["text"].lower() and entity["type"] == "ORG"
            for entity in result
        )
        self.assertTrue(apple_found)

    def test_extract_entities_location(self):
        """Test entity extraction with location entity."""
        text = "New York City is the most populous city in the United States."
        result = extract_entities(text)

        # Check that we found at least one location entity
        loc_entities = [entity for entity in result if entity["type"] in ("LOC", "GPE")]
        self.assertTrue(len(loc_entities) > 0)

        # Check that "New York City" is identified as a location
        nyc_found = any(
            "new york" in entity["text"].lower() and entity["type"] in ("LOC", "GPE")
            for entity in result
        )
        self.assertTrue(nyc_found)
