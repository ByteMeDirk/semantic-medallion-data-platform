# Semantic Medallion Data Platform

A modern data platform implementing the medallion architecture with local processing.

## Architecture Overview

This project implements a medallion architecture for data lakes, which organizes data into three layers:

1. **Bronze Layer (Raw)**: Raw data ingested from various sources
2. **Silver Layer (Validated)**: Cleansed, validated, and transformed data
3. **Gold Layer (Business)**: Business-level aggregates and metrics ready for consumption

## Tech Stack

- **Data Processing**: PySpark, Delta Lake
- **Database**: PostgreSQL
- **Orchestration**: Prefect
- **Transformation**: dbt
- **Data Quality**: Great Expectations
- **Reporting & Visualization**: Metabase
- **Local Development**: Docker, Poetry
- **External APIs**: NewsAPI

## Project Structure

```
semantic-medallion-data-platform/
├── .github/                      # GitHub Actions workflows
├── data/                         # Data files
│   └── known_entities/           # Known entities data files
├── docs/                         # Documentation
├── semantic_medallion_data_platform/  # Main package
│   ├── bronze/                   # Bronze layer processing
│   │   ├── brz_01_extract_newsapi.py        # Extract news articles from NewsAPI
│   │   └── brz_01_extract_known_entities.py # Extract known entities from CSV files
│   ├── silver/                   # Silver layer processing
│   ├── gold/                     # Gold layer processing
│   ├── common/                   # Shared utilities
│   └── config/                   # Configuration
├── tests/                        # Unit and integration tests
├── docker/                       # Docker configurations
├── .pre-commit-config.yaml       # Pre-commit hooks
├── pyproject.toml                # Poetry configuration
└── README.md                     # This file
```

## Getting Started

### Prerequisites

- Python 3.9+
- [Poetry](https://python-poetry.org/docs/#installation)
- [Docker](https://docs.docker.com/get-docker/)

### Installation

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

4. Create a `.env` file from the template:
   ```bash
   cp .env.example .env
   ```

   Edit the `.env` file to set your database credentials and other environment variables. Make sure to set your NewsAPI key if you plan to use the news article extraction functionality:
   ```
   NEWSAPI_KEY=your_newsapi_key_here
   ```

   You can obtain a NewsAPI key by signing up at [https://newsapi.org/](https://newsapi.org/).

### Local Development

Start the local development environment:

```bash
cd docker
docker-compose up -d
```

This will start:
- Local PostgreSQL database
- Metabase (data visualization and reporting tool) accessible at http://localhost:3000

### Running Tests

```bash
poetry run pytest
```

### Running Bronze Layer Processes

#### Extracting News Articles from NewsAPI

To extract news articles for known entities:

```bash
cd semantic-medallion-data-platform
python -m semantic_medallion_data_platform.bronze.brz_01_extract_newsapi --days_back 7
```

This will:
1. Fetch known entities from the database
2. Query NewsAPI for articles mentioning each entity
3. Store the articles in the bronze.newsapi table

#### Extracting Known Entities

To load known entities from CSV files:

```bash
cd semantic-medallion-data-platform
python -m semantic_medallion_data_platform.bronze.brz_01_extract_known_entities --raw_data_filepath data/known_entities/
```

This will:
1. Read entity data from CSV files in the specified directory
2. Process and transform the data
3. Store the entities in the bronze.known_entities table


## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
