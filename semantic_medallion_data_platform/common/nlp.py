"""
Common NLP utilities for the Semantic Medallion Data Platform.

This module provides reusable NLP functions for entity extraction, sentiment analysis, and other text processing tasks.
"""
import logging

import spacy
from pyspark.sql.types import ArrayType, FloatType, StringType, StructField, StructType
from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline

# Load spaCy NLP model
NLP = spacy.load("en_core_web_lg")

# Load BERT model for sentiment analysis
MODEL_NAME = "tabularisai/multilingual-sentiment-analysis"  # Standard sentiment model
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
sentiment_analyzer = pipeline("text-classification", model=model, tokenizer=tokenizer)

# Define schema for entity extraction
ENTITY_STRUCT = StructType(
    [StructField("text", StringType(), True), StructField("type", StringType(), True)]
)

ENTITIES_SCHEMA = ArrayType(ENTITY_STRUCT)

# Define schema for sentiment analysis
SENTIMENT_STRUCT = StructType(
    [StructField("score", FloatType(), True), StructField("label", StringType(), True)]
)


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


def analyze_sentiment(text: str) -> dict:
    """
    Analyze sentiment of text using a pre-trained BERT model.

    Args:
        text: The input text to analyze

    Returns:
        A dictionary with sentiment score and label
    """
    if not text:
        return {"score": 0.0, "label": "NEUTRAL"}

    # Truncate text if it's too long for BERT (typically 512 tokens)
    max_length = 512
    if len(text.split()) > max_length:
        text = " ".join(text.split()[:max_length])

    try:
        # Run sentiment analysis
        result = sentiment_analyzer(text)[0]
        return {"score": float(result["score"]), "label": result["label"]}
    except Exception as e:
        # Log error and return neutral sentiment
        logging.error(f"Error analyzing sentiment: {e}")
        return {"score": 0.0, "label": "NEUTRAL"}
