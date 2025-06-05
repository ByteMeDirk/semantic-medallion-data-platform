# Main Terraform configuration file for the Semantic Medallion Data Platform
# This file configures the Terraform providers and sets up the Digital Ocean provider
# The actual infrastructure resources are defined in other files:
# - database.tf: PostgreSQL database resources
# - variables.tf: Variable definitions
# - outputs.tf: Output definitions

terraform {
  required_providers {
    digitalocean = {
      source  = "digitalocean/digitalocean"
      version = "~> 2.0"
    }
  }
}

# Configure the DigitalOcean Provider with the API token from variables
provider "digitalocean" {
  token = var.do_token
}
