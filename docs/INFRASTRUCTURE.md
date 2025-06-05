# Infrastructure Documentation

## Overview

This document provides detailed information about the infrastructure setup for the Semantic Medallion Data Platform. The infrastructure is managed using Terraform and is hosted on Digital Ocean.

## Infrastructure Components

### Digital Ocean Resources

The following resources are provisioned on Digital Ocean:

#### PostgreSQL Database Cluster

- **Resource Name**: `digitalocean_database_cluster.postgres`
- **Engine**: PostgreSQL
- **Version**: 15
- **Size**: db-s-1vcpu-1gb (1 vCPU, 1GB RAM)
- **Region**: Configurable (default: London - lon1)
- **Node Count**: 1
- **Tags**: Environment tag (dev, staging, or prod)

#### Database

- **Resource Name**: `digitalocean_database_db.database`
- **Name**: semantic_data_platform
- **Cluster**: References the PostgreSQL cluster

#### Database User

- **Resource Name**: `digitalocean_database_user.user`
- **Name**: semantic_app_user
- **Cluster**: References the PostgreSQL cluster

## Terraform Configuration

### Directory Structure

```
infrastructure/terraform/
├── main.tf                # Provider configuration
├── database.tf            # Database resource definitions
├── variables.tf           # Variable definitions
├── outputs.tf             # Output definitions
├── terraform.tfvars.example  # Example variables file
└── terraform.tfvars       # Actual variables file (not in version control)
```

### Configuration Files

#### main.tf

This file configures the Digital Ocean provider:

```hcl
terraform {
  required_providers {
    digitalocean = {
      source  = "digitalocean/digitalocean"
      version = "~> 2.0"
    }
  }
}

# Configure the DigitalOcean Provider
provider "digitalocean" {
  token = var.do_token
}
```

#### database.tf

This file defines the PostgreSQL database resources:

```hcl
resource "digitalocean_database_cluster" "postgres" {
  name       = "${var.project_name}-db-${var.environment}"
  engine     = "pg"
  version    = "15"
  size       = "db-s-1vcpu-1gb"  # Smallest available size ($15/month)
  region     = var.region
  node_count = 1

  tags = [var.environment]
}

# Create a database
resource "digitalocean_database_db" "database" {
  cluster_id = digitalocean_database_cluster.postgres.id
  name       = "semantic_data_platform"
}

# Create a database user
resource "digitalocean_database_user" "user" {
  cluster_id = digitalocean_database_cluster.postgres.id
  name       = "semantic_app_user"
}
```

#### variables.tf

This file defines the variables used in the Terraform configuration:

```hcl
variable "do_token" {
  description = "Digital Ocean API Token"
  type        = string
  sensitive   = true
}

variable "region" {
  description = "Digital Ocean region"
  type        = string
  default     = "lon1"
}

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "Semantic Data Platform"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}
```

#### outputs.tf

This file defines the outputs from the Terraform configuration:

```hcl
output "postgres_host" {
  value     = digitalocean_database_cluster.postgres.host
  sensitive = false
}

output "postgres_port" {
  value     = digitalocean_database_cluster.postgres.port
  sensitive = false
}

output "postgres_user" {
  value     = digitalocean_database_user.user.name
  sensitive = false
}

output "postgres_password" {
  value     = digitalocean_database_user.user.password
  sensitive = true
}

output "postgres_database" {
  value     = digitalocean_database_db.database.name
  sensitive = false
}

output "postgres_uri" {
  value     = digitalocean_database_cluster.postgres.uri
  sensitive = true
}
```

## Setup and Management

### Initial Setup

1. Install Terraform (version 1.0.0 or later)
2. Create a Digital Ocean account and generate an API token
3. Navigate to the Terraform directory: `cd infrastructure/terraform`
4. Create a `terraform.tfvars` file from the example: `cp terraform.tfvars.example terraform.tfvars`
5. Edit the `terraform.tfvars` file to add your Digital Ocean API token
6. Initialize Terraform: `terraform init`
7. Plan the infrastructure changes: `terraform plan -out=tfplan`
8. Apply the infrastructure changes: `terraform apply tfplan`

### Updating Infrastructure

To update the infrastructure after making changes to the Terraform files:

```bash
terraform plan -out=tfplan   # Preview changes
terraform apply tfplan       # Apply changes
```

### Destroying Infrastructure

To destroy the infrastructure when no longer needed:

```bash
terraform destroy
```

## Connecting to the Database

After the infrastructure is provisioned, Terraform will output the connection details for the PostgreSQL database:

- **Host**: `postgres_host` output
- **Port**: `postgres_port` output
- **Database**: `postgres_database` output
- **Username**: `postgres_user` output
- **Password**: `postgres_password` output (sensitive)
- **URI**: `postgres_uri` output (sensitive)

You can use these details to connect to the database from your application or from a database client.

## Security Considerations

- The Digital Ocean API token is sensitive and should not be committed to version control
- Database passwords are also sensitive and should be handled securely
- The database is accessible over the internet, so ensure that your application uses secure connections

## Cost Considerations

The infrastructure provisioned by this Terraform configuration includes:

- PostgreSQL Database Cluster: db-s-1vcpu-1gb ($15/month)

Total estimated cost: $15/month

## Troubleshooting

### Common Issues

1. **Terraform initialization fails**:
   - Ensure you have the correct version of Terraform installed
   - Check your internet connection

2. **Digital Ocean API token issues**:
   - Verify that your API token is correct and has the necessary permissions
   - Ensure the token is properly set in the terraform.tfvars file

3. **Database connection issues**:
   - Check that the database cluster is running
   - Verify that you're using the correct connection details
   - Ensure your firewall allows connections to the database port
