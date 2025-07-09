"""
Common NLP utilities for the Semantic Medallion Data Platform.

This module provides reusable NLP functions for entity extraction, sentiment analysis, and other text processing tasks.
"""
import logging

import spacy
from pyspark.sql.types import ArrayType, FloatType, StringType, StructField, StructType
from transformers import (
    AutoModelForSequenceClassification,
    AutoModelForTokenClassification,
    AutoTokenizer,
    pipeline,
)

# Load spaCy NLP model
NLP = spacy.load("en_core_web_lg")

# Load BERT model for sentiment analysis
SENTIMENT_MODEL_NAME = (
    "tabularisai/multilingual-sentiment-analysis"  # Standard sentiment model
)
BERT_NER_MODEL_NAME = (
    "dbmdz/bert-large-cased-finetuned-conll03-english"  # Standard NER model
)
SENTIMENT_TOKENIZER = AutoTokenizer.from_pretrained(SENTIMENT_MODEL_NAME)
SENTIMENT_MODEL = AutoModelForSequenceClassification.from_pretrained(
    SENTIMENT_MODEL_NAME
)
SENTIMENT_ANALYZER = pipeline(
    "text-classification", model=SENTIMENT_MODEL, tokenizer=SENTIMENT_TOKENIZER
)
BERT_TOKENIZER = AutoTokenizer.from_pretrained(BERT_NER_MODEL_NAME)
BERT_MODEL = AutoModelForTokenClassification.from_pretrained(BERT_NER_MODEL_NAME)
BERT_NER_PIPELINE = pipeline(
    "ner", model=BERT_MODEL, tokenizer=BERT_TOKENIZER, aggregation_strategy="simple"
)

# Define schema for entity extraction
ENTITY_STRUCT = StructType(
    [StructField("text", StringType(), True), StructField("type", StringType(), True)]
)

ENTITIES_SCHEMA = ArrayType(ENTITY_STRUCT)

# Define schema for sentiment analysis
SENTIMENT_STRUCT = StructType(
    [StructField("score", FloatType(), True), StructField("label", StringType(), True)]
)


def extract_entities_spacy(text: str) -> list:
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


def extract_entities_bert(text: str) -> list:
    # Updated mapping to handle actual BERT output labels
    label_mapping = {
        "PER": "PERSON",
        "PERSON": "PERSON",
        "LOC": "LOC",
        "LOCATION": "LOC",
        "ORG": "ORG",
        "ORGANIZATION": "ORG",
        "MISC": "MISC",
    }

    if not text:
        return []

    entities = BERT_NER_PIPELINE(text)
    mapped_entities = []

    for ent in entities:
        entity_group = ent.get("entity_group", "")
        mapped_type = label_mapping.get(entity_group, entity_group)

        # Only include entities we want to evaluate
        if mapped_type in ["PERSON", "ORG", "LOC"]:
            mapped_entities.append(
                {
                    "text": ent["word"],
                    "type": mapped_type,
                }
            )

    return mapped_entities


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
        result = SENTIMENT_ANALYZER(text)[0]
        return {"score": float(result["score"]), "label": result["label"]}
    except Exception as e:
        # Log error and return neutral sentiment
        logging.error(f"Error analyzing sentiment: {e}")
        return {"score": 0.0, "label": "NEUTRAL"}
