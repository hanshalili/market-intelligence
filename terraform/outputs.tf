# ==============================================================================
# outputs.tf — Exported values consumed by Airflow and dbt via .env
# ==============================================================================

output "gcs_bucket_name" {
  description = "GCS data lake bucket name. Set as GCS_BUCKET_NAME in .env."
  value       = google_storage_bucket.data_lake.name
}

output "bigquery_dataset_id" {
  description = "BigQuery dataset ID. Set as BQ_DATASET in .env."
  value       = google_bigquery_dataset.market_analytics.dataset_id
}

output "service_account_email" {
  description = "Email of the pipeline service account."
  value       = google_service_account.pipeline_sa.email
}

output "service_account_key_path" {
  description = "Local path to the generated service account JSON key."
  value       = local_file.sa_key_json.filename
  sensitive   = true
}
