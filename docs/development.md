# Development Guide

This guide provides instructions for setting up and working with the Semantic Medallion Data Platform in a development
environment.

## Prerequisites

Before you begin, ensure you have the following installed:

- Python 3.9+
- [Poetry](https://python-poetry.org/docs/#installation)
- [Docker](https://docs.docker.com/get-docker/) and Docker Compose

## Initial Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/semantic-medallion-data-platform.git
   cd semantic-medallion-data-platform
   ```

2. Install dependencies:
   ```bash
   poetry install
   ```

3. Set up pre-commit hooks:
   ```bash
   poetry run pre-commit install
   ```

## Local Development Environment

The project includes a Docker Compose configuration that sets up a local development environment with:

- PostgreSQL database
- Metabase (data visualization and reporting tool)

Metabase provides a user-friendly interface for creating reports and dashboards based on the data in the PostgreSQL database. It can be accessed at http://localhost:3000 after starting the Docker environment.

To start the local environment:

```bash
cd docker
docker-compose up -d
```

To verify that all services are running:

```bash
docker-compose ps
```

To view logs from a specific service:

```bash
docker-compose logs -f <service-name>
```

## Environment Configuration

Create a `.env` file in the project root with the following variables:

```
# Local Development
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=medallion

# API Keys
NEWSAPI_KEY=your_newsapi_key_here  # Get your key from https://newsapi.org/
```

## Running Tests

To run the test suite:

```bash
poetry run pytest
```

To run tests with coverage:

```bash
poetry run pytest --cov=semantic_medallion_data_platform
```

## Code Quality

The project uses several tools to maintain code quality:

- **Black**: Code formatter
  ```bash
  poetry run black .
  ```

- **isort**: Import sorter
  ```bash
  poetry run isort .
  ```

- **Flake8**: Linter
  ```bash
  poetry run flake8
  ```

- **mypy**: Type checker
  ```bash
  poetry run mypy semantic_medallion_data_platform
  ```

## Working with the Medallion Architecture

### Bronze Layer

#### Using the Bronze Layer Scripts

The project includes scripts for ingesting data into the Bronze layer:

1. **Extract News Articles from NewsAPI**:

   ```bash
   python -m semantic_medallion_data_platform.bronze.brz_01_extract_newsapi --days_back 7
   ```

   This script:
    - Fetches known entities from the database
    - Queries NewsAPI for articles mentioning each entity
    - Stores the articles in the bronze.newsapi table

2. **Extract Known Entities**:

   ```bash
   python -m semantic_medallion_data_platform.bronze.brz_01_extract_known_entities --raw_data_filepath data/known_entities/
   ```

   This script:
    - Reads entity data from CSV files in the specified directory
    - Processes and transforms the data
    - Stores the entities in the bronze.known_entities table

#### Programmatic Access

To ingest data into the Bronze layer programmatically:

```python
from semantic_medallion_data_platform.bronze import ingest

# Ingest data from a source
ingest.from_csv("path/to/file.csv", "destination_table")
```

### Silver Layer

#### Using the Silver Layer Scripts

1. **Transform and Extract Entities from NewsAPI Articles**:

   ```bash
   python -m semantic_medallion_data_platform.silver.slv_02_transform_nlp_newsapi
   ```

   This script:
    - Reads news articles from the bronze.newsapi table
    - Copies the raw articles to the silver.newsapi table
    - Uses spaCy NLP to extract named entities (locations, organizations, persons) from article text
    - Normalizes entity types (e.g., converts 'GPE' to 'LOC')
    - Removes duplicate entities
    - Stores extracted entities in the silver.newsapi_entities table

#### Programmatic Access

To process data from Bronze to Silver:

```python
from semantic_medallion_data_platform.silver import transform

# Transform data from Bronze to Silver
transform.bronze_to_silver("source_table", "destination_table")
```

### Gold Layer

To aggregate data from Silver to Gold:

```python
from semantic_medallion_data_platform.gold import aggregate

# Aggregate data from Silver to Gold
aggregate.silver_to_gold("source_table", "destination_table")
```


## Troubleshooting

### Common Issues

1. **Docker services not starting**:
    - Check Docker logs: `docker-compose logs`
    - Ensure ports are not already in use

2. **Poetry dependency issues**:
    - Update Poetry: `poetry self update`
    - Clear cache: `poetry cache clear pypi --all`
