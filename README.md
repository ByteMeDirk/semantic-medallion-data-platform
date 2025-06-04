# Semantic Medallion Data Platform

A modern data platform implementing the medallion architecture with local processing.

## Architecture Overview

This project implements a medallion architecture for data lakes, which organizes data into three layers:

1. **Bronze Layer (Raw)**: Raw data ingested from various sources
2. **Silver Layer (Validated)**: Cleansed, validated, and transformed data
3. **Gold Layer (Business)**: Business-level aggregates and metrics ready for consumption

## Tech Stack

- **Data Processing**: PySpark
- **Database**: PostgreSQL
- **Orchestration**: None
- **Transformation**: pyspark
- **Reporting & Visualization**: Metabase
- **Local Development**: Docker, Poetry
- **External APIs**: NewsAPI
- **Infrastructure as Code**: Terraform

## Project Structure

```
semantic-medallion-data-platform/
├── .github/                      # GitHub Actions workflows
├── data/                         # Data files
│   └── known_entities/           # Known entities data files
├── docs/                         # Documentation
├── infrastructure/               # Infrastructure as Code
│   └── terraform/                # Terraform configuration for Digital Ocean
│       ├── main.tf               # Main Terraform configuration
│       ├── variables.tf          # Variable definitions
│       ├── outputs.tf            # Output definitions
│       ├── terraform.tfvars.example # Example variables file
│       └── setup.sh              # Setup script for Terraform
├── semantic_medallion_data_platform/  # Main package
│   ├── bronze/                   # Bronze layer processing
│   │   ├── brz_01_extract_newsapi.py        # Extract news articles from NewsAPI
│   │   └── brz_01_extract_known_entities.py # Extract known entities from CSV files
│   ├── silver/                   # Silver layer processing
│   │   ├── slv_02_transform_nlp_known_entities.py  # Extract entities from known entities descriptions
│   │   ├── slv_02_transform_nlp_newsapi.py         # Extract entities from news articles
│   │   └── slv_03_transform_entity_to_entity_mapping.py  # Create entity mappings
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

### Running Silver Layer Processes

#### Processing Known Entities with NLP

To extract entities from known entities descriptions:

```bash
cd semantic-medallion-data-platform
python -m semantic_medallion_data_platform.silver.slv_02_transform_nlp_known_entities
```

This will:
1. Copy known entities from bronze.known_entities to silver.known_entities
2. Extract entities (locations, organizations, persons) from entity descriptions using NLP
3. Store the extracted entities in the silver.known_entities_entities table

#### Processing News Articles with NLP

To extract entities from news articles:

```bash
cd semantic-medallion-data-platform
python -m semantic_medallion_data_platform.silver.slv_02_transform_nlp_newsapi
```

This will:
1. Copy news articles from bronze.newsapi to silver.newsapi
2. Extract entities from article title, description, and content using NLP
3. Store the extracted entities in the silver.newsapi_entities table

#### Creating Entity Mappings

To create entity-to-entity and entity-to-source mappings:

```bash
cd semantic-medallion-data-platform
python -m semantic_medallion_data_platform.silver.slv_03_transform_entity_to_entity_mapping
```

This will:
1. Create entity-to-source mappings between known_entities_entities and newsapi_entities
2. Create entity-to-entity mappings within known_entities_entities
3. Store the mappings in silver.entity_to_source_mapping and silver.entity_to_entity_mapping tables


## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## Infrastructure Setup with Terraform

This project uses Terraform to manage infrastructure on Digital Ocean, including a PostgreSQL database. Follow these steps to set up the infrastructure:

### Prerequisites

- [Terraform](https://www.terraform.io/downloads.html) (version 1.0.0 or later)
- [Digital Ocean account](https://www.digitalocean.com/)
- [Digital Ocean API token](https://cloud.digitalocean.com/account/api/tokens)

### Setup Instructions

1. Navigate to the Terraform directory:
   ```bash
   cd infrastructure/terraform
   ```

2. Create a `terraform.tfvars` file from the example:
   ```bash
   cp terraform.tfvars.example terraform.tfvars
   ```

3. Edit the `terraform.tfvars` file to add your Digital Ocean API token:
   ```bash
   # Open with your favorite editor
   nano terraform.tfvars
   ```

4. Initialize Terraform:
   ```bash
   terraform init
   ```

5. Plan the infrastructure changes:
   ```bash
   terraform plan -out=tfplan
   ```

6. Apply the infrastructure changes:
   ```bash
   terraform apply tfplan
   ```

7. After successful application, Terraform will output connection details for your PostgreSQL database:
   - Database host
   - Database port
   - Database name
   - Database user
   - Database password (sensitive)
   - Database URI (sensitive)

### Infrastructure Components

The Terraform configuration creates the following resources on Digital Ocean:

- **PostgreSQL Database Cluster**:
  - Version: PostgreSQL 15
  - Size: db-s-1vcpu-1gb (1 vCPU, 1GB RAM)
  - Region: Configurable (default: London - lon1)
  - Node Count: 1

- **Database**:
  - Name: semantic_data_platform

- **Database User**:
  - Name: semantic_app_user

### Managing Infrastructure

- To update the infrastructure after making changes to the Terraform files:
  ```bash
  terraform plan -out=tfplan   # Preview changes
  terraform apply tfplan       # Apply changes
  ```

- To destroy the infrastructure when no longer needed:
  ```bash
  terraform destroy
  ```

For more detailed information about the infrastructure setup, see [INFRASTRUCTURE.md](docs/INFRASTRUCTURE.md).

For deployment instructions, see [DEPLOYMENT.md](docs/DEPLOYMENT.md).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
