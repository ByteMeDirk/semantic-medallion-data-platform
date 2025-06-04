"""
Common NLP utilities for the Semantic Medallion Data Platform.

This module provides reusable NLP functions for entity extraction and other text processing tasks.
"""
import spacy
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

# Load spaCy NLP model
NLP = spacy.load("en_core_web_lg")

# Define schema for entity extraction
ENTITY_STRUCT = StructType(
    [StructField("text", StringType(), True), StructField("type", StringType(), True)]
)

ENTITIES_SCHEMA = ArrayType(ENTITY_STRUCT)


def extract_entities(text: str) -> list:
    """
    Extract location, organization, and person entities from text using spaCy.

    Args:
        text: The input text to process

    Returns:
        A list of dictionaries with entity text and type
    """
    if not text:
        return []

    doc = NLP(text)
    entities = [
        {"text": ent.text, "type": ent.label_}
        for ent in doc.ents
        if ent.label_ in ("LOC", "GPE", "ORG", "PERSON")
    ]
    return entities
