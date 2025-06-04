# Variables for Digital Ocean Terraform configuration
# This file defines the variables used in the Terraform configuration for the Semantic Medallion Data Platform.
# These variables can be set in the terraform.tfvars file or passed as command-line arguments.
#
# Required variables:
# - do_token: Digital Ocean API Token (sensitive)
#
# Optional variables with defaults:
# - region: Digital Ocean region (default: lon1)
# - project_name: Name of the project (default: "Semantic Data Platform")
# - environment: Deployment environment (default: "dev", must be one of: dev, staging, prod)

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
