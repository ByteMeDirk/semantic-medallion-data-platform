# Semantic Medallion Data Platform

A modern data platform implementing the medallion architecture on Google Cloud Platform.

## Architecture Overview

This project implements a medallion architecture for data lakes, which organizes data into three layers:

1. **Bronze Layer (Raw)**: Raw data ingested from various sources
2. **Silver Layer (Validated)**: Cleansed, validated, and transformed data
3. **Gold Layer (Business)**: Business-level aggregates and metrics ready for consumption

## Tech Stack

- **Data Processing**: PySpark, Delta Lake
- **Cloud Infrastructure**: Google Cloud Platform (GCS, BigQuery)
- **Orchestration**: Prefect
- **Transformation**: dbt
- **Data Quality**: Great Expectations
- **Local Development**: Docker, Poetry
- **Infrastructure as Code**: Terraform

## Project Structure

```
semantic-medallion-data-platform/
├── .github/                      # GitHub Actions workflows
├── docs/                         # Documentation
├── infrastructure/               # Terraform configurations
│   ├── environments/             # Environment-specific configurations
│   └── modules/                  # Reusable Terraform modules
├── semantic_medallion_data_platform/  # Main package
│   ├── bronze/                   # Bronze layer processing
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
- [Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli)
- Google Cloud account with appropriate permissions

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

   Edit the `.env` file to set your database credentials and other environment variables.

### Local Development

Start the local development environment:

```bash
docker-compose up -d
```

This will start:
- Local PostgreSQL database
- Local GCS emulator
- Other required services

### Running Tests

```bash
poetry run pytest
```

### Deploying to GCP

1. Initialize Terraform:
   ```bash
   cd infrastructure/environments/dev
   terraform init
   ```

2. Apply Terraform configuration:
   ```bash
   terraform apply
   ```

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
