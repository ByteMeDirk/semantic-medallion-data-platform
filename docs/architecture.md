# Medallion Architecture

## Overview

The medallion architecture is a data organization framework that structures data into three distinct layers, each with
its own purpose and characteristics:

1. **Bronze Layer (Raw Data)**
2. **Silver Layer (Validated Data)**
3. **Gold Layer (Business Data)**

This architecture provides a clear separation of concerns and enables efficient data processing and governance.

## Bronze Layer

The Bronze layer contains raw data ingested from various sources with minimal or no transformations.

### Characteristics:

- Raw, unprocessed data
- Exact copy of source data
- Immutable
- Append-only
- Full history preserved

### Storage:

- Local filesystem: `data/bronze`
- PostgreSQL schema: `bronze`

### Data Sources:
- NewsAPI articles
- Known entities (organizations, locations, persons)

## Silver Layer

The Silver layer contains cleansed, validated, and transformed data that is ready for analysis.

### Characteristics:

- Validated and cleansed data
- Standardized schemas
- Data quality checks applied
- Duplicate records removed
- Type conversions applied
- Business keys established
- Natural Language Processing (NLP) transformations applied

### Storage:

- Local filesystem: `data/silver`
- PostgreSQL schema: `silver`

### Processes:
- Entity extraction from news articles using spaCy
- Entity normalization and deduplication
- Entity linking with known entities

## Gold Layer

The Gold layer contains business-level aggregates and metrics that are ready for consumption by end-users and
applications.

### Characteristics:

- Business-level aggregates
- Denormalized for query performance
- Optimized for specific use cases
- Ready for consumption
- Often includes dimensional models

### Storage:

- Local filesystem: `data/gold`
- PostgreSQL schema: `gold`

## Data Flow

The typical data flow in the medallion architecture is:

1. **Ingestion**: Data is ingested from source systems into the Bronze layer
2. **Validation**: Data is validated, cleansed, and transformed from Bronze to Silver
3. **Aggregation**: Data is aggregated and modeled from Silver to Gold

## Benefits

- **Separation of Concerns**: Each layer has a specific purpose and set of operations
- **Data Quality**: Progressive improvement of data quality as it moves through layers
- **Auditability**: Full lineage and history of data transformations
- **Reprocessing**: Ability to reprocess data from raw sources if needed
- **Performance**: Optimized storage and query performance at each layer
