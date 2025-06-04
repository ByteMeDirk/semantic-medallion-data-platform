# Development Guide

This guide provides instructions for setting up and working with the Semantic Medallion Data Platform in a development environment.

## Prerequisites

Before you begin, ensure you have the following installed:

- Python 3.9+
- [Poetry](https://python-poetry.org/docs/#installation)
- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- [Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli) (for infrastructure deployment)
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) (for GCP deployment)

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
- GCS emulator
- Spark cluster (master and worker)

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

# GCS Emulator
GCS_EMULATOR_HOST=http://localhost:4443

# Spark
SPARK_MASTER_URL=spark://localhost:7077
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

To ingest data into the Bronze layer:

```python
from semantic_medallion_data_platform.bronze import ingest

# Ingest data from a source
ingest.from_csv("path/to/file.csv", "destination_table")
```

### Silver Layer

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

## Infrastructure Deployment

To deploy the infrastructure to GCP:

1. Authenticate with Google Cloud:
   ```bash
   gcloud auth application-default login
   ```

2. Initialize Terraform:
   ```bash
   cd infrastructure/environments/dev
   terraform init
   ```

3. Plan the deployment:
   ```bash
   terraform plan -var="project_id=your-project-id"
   ```

4. Apply the changes:
   ```bash
   terraform apply -var="project_id=your-project-id"
   ```

## Troubleshooting

### Common Issues

1. **Docker services not starting**:
   - Check Docker logs: `docker-compose logs`
   - Ensure ports are not already in use

2. **Terraform deployment failures**:
   - Verify GCP authentication: `gcloud auth list`
   - Check project permissions

3. **Poetry dependency issues**:
   - Update Poetry: `poetry self update`
   - Clear cache: `poetry cache clear pypi --all`
