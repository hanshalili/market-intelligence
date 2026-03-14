# ==============================================================================
# variables.tf — Input variables for the Market Intelligence Pipeline
# ==============================================================================

variable "project_id" {
  description = "GCP project ID where all resources will be created."
  type        = string
}

variable "region" {
  description = "GCP region for GCS bucket and compute resources."
  type        = string
  default     = "us-central1"
}

variable "bq_location" {
  description = "BigQuery dataset location. Use 'US' for multi-region."
  type        = string
  default     = "US"
}

variable "bq_dataset" {
  description = "BigQuery dataset name for market analytics tables."
  type        = string
  default     = "market_analytics"
}

variable "environment" {
  description = "Deployment environment label (dev | staging | prod)."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}
