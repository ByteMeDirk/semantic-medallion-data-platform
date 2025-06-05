# Output connection details for the PostgreSQL database
# This file defines the outputs that will be displayed after Terraform applies the configuration.
# These outputs provide the necessary connection details for connecting to the PostgreSQL database.
# The sensitive outputs (password and URI) will be hidden in the Terraform output but can be
# accessed using the terraform output command with the -json flag.
#
# Usage:
# - To view all outputs: terraform output
# - To view a specific output: terraform output postgres_host
# - To view sensitive outputs: terraform output -json

# The hostname of the PostgreSQL database cluster
output "postgres_host" {
  description = "The hostname of the PostgreSQL database cluster"
  value       = digitalocean_database_cluster.postgres.host
  sensitive   = false
}

# The port number for connecting to the PostgreSQL database
output "postgres_port" {
  description = "The port number for connecting to the PostgreSQL database"
  value       = digitalocean_database_cluster.postgres.port
  sensitive   = false
}

# The username for connecting to the PostgreSQL database
output "postgres_user" {
  description = "The username for connecting to the PostgreSQL database"
  value       = digitalocean_database_user.user.name
  sensitive   = false
}

# The password for connecting to the PostgreSQL database (sensitive)
output "postgres_password" {
  description = "The password for connecting to the PostgreSQL database (sensitive)"
  value       = digitalocean_database_user.user.password
  sensitive   = true
}

# The name of the PostgreSQL database
output "postgres_database" {
  description = "The name of the PostgreSQL database"
  value       = digitalocean_database_db.database.name
  sensitive   = false
}

# The full URI for connecting to the PostgreSQL database (sensitive)
# Format: postgres://username:password@host:port/database
output "postgres_uri" {
  description = "The full URI for connecting to the PostgreSQL database (sensitive)"
  value       = digitalocean_database_cluster.postgres.uri
  sensitive   = true
}
