# Deployment Guide

## Overview

This document provides instructions for deploying the Semantic Medallion Data Platform to Digital Ocean. The deployment process involves setting up the infrastructure using Terraform and then deploying the application to use the provisioned resources.

## Prerequisites

Before deploying, ensure you have the following:

- [Terraform](https://www.terraform.io/downloads.html) (version 1.0.0 or later)
- [Digital Ocean account](https://www.digitalocean.com/)
- [Digital Ocean API token](https://cloud.digitalocean.com/account/api/tokens)
- [Python 3.9+](https://www.python.org/downloads/)
- [Poetry](https://python-poetry.org/docs/#installation)

## Deployment Process

### Step 1: Set Up Infrastructure

First, set up the infrastructure using Terraform:

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

7. After successful application, Terraform will output connection details for your PostgreSQL database. Save these details for the next step.

### Step 2: Configure Application Environment

Create a `.env` file in the project root with the connection details from the previous step:

```
# Database Connection (from Terraform outputs)
POSTGRES_HOST=<postgres_host output>
POSTGRES_PORT=<postgres_port output>
POSTGRES_USER=<postgres_user output>
POSTGRES_PASSWORD=<postgres_password output>
POSTGRES_DB=<postgres_database output>

# API Keys
NEWSAPI_KEY=your_newsapi_key_here  # Get your key from https://newsapi.org/
```

### Step 3: Initialize the Database

Run the database initialization script to create the necessary schemas and tables:

```bash
python -m semantic_medallion_data_platform.config.initialize_db
```

### Step 4: Load Initial Data

Load the initial data into the database:

```bash
# Load known entities
python -m semantic_medallion_data_platform.bronze.brz_01_extract_known_entities --raw_data_filepath data/known_entities/

# Extract news articles (optional)
python -m semantic_medallion_data_platform.bronze.brz_01_extract_newsapi --days_back 7
```

### Step 5: Process Data Through Layers

Process the data through the medallion architecture layers:

```bash
# Process known entities with NLP
python -m semantic_medallion_data_platform.silver.slv_02_transform_nlp_known_entities

# Process news articles with NLP (if you extracted news articles)
python -m semantic_medallion_data_platform.silver.slv_02_transform_nlp_newsapi

# Create entity mappings
python -m semantic_medallion_data_platform.silver.slv_03_transform_entity_to_entity_mapping
```

## Deployment Environments

The infrastructure can be deployed to different environments by setting the `environment` variable in `terraform.tfvars`:

- **Development**: `environment = "dev"`
- **Staging**: `environment = "staging"`
- **Production**: `environment = "prod"`

Each environment will have its own separate database cluster.

## Updating the Deployment

### Updating Infrastructure

To update the infrastructure after making changes to the Terraform files:

```bash
cd infrastructure/terraform
terraform plan -out=tfplan   # Preview changes
terraform apply tfplan       # Apply changes
```

### Updating Application

To update the application code:

1. Pull the latest code from the repository
2. Install any new dependencies:
   ```bash
   poetry install
   ```
3. Run any necessary database migrations or data processing scripts

## Monitoring and Maintenance

### Database Monitoring

You can monitor your database cluster through the Digital Ocean dashboard:

1. Log in to your Digital Ocean account
2. Navigate to Databases
3. Select your database cluster
4. View metrics such as CPU usage, memory usage, and disk usage

### Database Backups

Digital Ocean automatically creates daily backups of your database cluster. You can also create manual backups:

1. Log in to your Digital Ocean account
2. Navigate to Databases
3. Select your database cluster
4. Click on "Backups"
5. Click "Create Backup"

### Database Maintenance

Digital Ocean handles most database maintenance tasks automatically, including:

- Security patches
- Minor version upgrades
- Failover testing

For major version upgrades, you will need to create a new database cluster and migrate your data.

## Troubleshooting

### Common Deployment Issues

1. **Database connection issues**:
   - Verify that the database cluster is running
   - Check that you're using the correct connection details in your `.env` file
   - Ensure your firewall allows connections to the database port

2. **Application errors**:
   - Check the application logs for error messages
   - Verify that all required environment variables are set
   - Ensure that the database schemas and tables are properly created

3. **Infrastructure provisioning issues**:
   - Check the Terraform logs for error messages
   - Verify that your Digital Ocean API token has the necessary permissions
   - Ensure that you have sufficient quota in your Digital Ocean account

## Rollback Procedure

If you need to rollback to a previous version of the infrastructure:

1. Restore from a database backup (if necessary)
2. Use Terraform to revert to a previous state:
   ```bash
   cd infrastructure/terraform
   terraform plan -out=tfplan -target=<resource_to_rollback>
   terraform apply tfplan
   ```

If you need to completely destroy the infrastructure:

```bash
cd infrastructure/terraform
terraform destroy
```

Note that this will permanently delete all resources, including the database and its data.
