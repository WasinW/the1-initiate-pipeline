// Project‑level infrastructure for The1 Initiate Pipeline
//
// This file provisions all core resources required by any table migration:
// - GCS bucket
// - BigQuery datasets
// - Service account and IAM bindings
// - BigLake connection
// - Dataplex lake, zone and asset
// - Spanner instance and database for Iceberg catalog
// - IAM bindings for Spanner
// - A template managed table creation (null_resource) for demonstration

terraform {
  required_version = ">= 1.3"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.60"
    }
  }
}

variable "project_id" { type = string }
variable "region" { type = string }

variable "bucket_name" { type = string }
variable "service_account_id" { type = string }
variable "dataset_staging" { type = string }
variable "dataset_raw" { type = string }
variable "connection_id" { type = string }
variable "lake_id" { type = string }
variable "zone_id" { type = string }
variable "asset_id" { type = string }
variable "spanner_instance" { type = string }
variable "spanner_database" { type = string }
variable "biglake_storage_prefix" { type = string }

provider "google" {
  project = var.project_id
  region  = var.region
}

// Enable required APIs
resource "google_project_service" "bigquery" {
  project = var.project_id
  service = "bigquery.googleapis.com"
  disable_on_destroy = false
}
resource "google_project_service" "bigquery_connection" {
  project = var.project_id
  service = "bigqueryconnection.googleapis.com"
  disable_on_destroy = false
}
resource "google_project_service" "dataplex" {
  project = var.project_id
  service = "dataplex.googleapis.com"
  disable_on_destroy = false
}
resource "google_project_service" "spanner" {
  project = var.project_id
  service = "spanner.googleapis.com"
  disable_on_destroy = false
}
resource "google_project_service" "storage" {
  project = var.project_id
  service = "storage.googleapis.com"
  disable_on_destroy = false
}

// Bucket to hold staging and managed data
resource "google_storage_bucket" "bucket" {
  name     = var.bucket_name
  location = var.region
  uniform_bucket_level_access = true
  force_destroy = false
  lifecycle_rule {
    action { type = "Delete" }
    condition { age = 365 }
  }
  lifecycle { prevent_destroy = true }
}

// Custom service account
resource "google_service_account" "pipeline_sa" {
  account_id   = var.service_account_id
  display_name = "Custom pipeline service account"
  project      = var.project_id
}

// IAM bindings for the custom service account
resource "google_project_iam_member" "sa_bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}
resource "google_project_iam_member" "sa_storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}
resource "google_project_iam_member" "sa_dataplex_admin" {
  project = var.project_id
  role    = "roles/dataplex.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}
resource "google_project_iam_member" "sa_connection_admin" {
  project = var.project_id
  role    = "roles/bigquery.connectionAdmin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

// Datasets
resource "google_bigquery_dataset" "staging" {
  dataset_id = var.dataset_staging
  project    = var.project_id
  location   = var.region
  description = "External staging dataset"
  lifecycle { prevent_destroy = true }
}
resource "google_bigquery_dataset" "raw" {
  dataset_id = var.dataset_raw
  project    = var.project_id
  location   = var.region
  description = "Managed raw dataset"
  lifecycle { prevent_destroy = true }
}

// BigQuery connection
resource "google_bigquery_connection" "connection" {
  connection_id  = var.connection_id
  project        = var.project_id
  location       = var.region
  connection_type = "CLOUD_RESOURCE"
  friendly_name  = "GCS connection"
  description    = "Connection used by BigQuery to access GCS"
  cloud_resource {}
}

// Grant the connection service account read access on the bucket
resource "google_storage_bucket_iam_member" "connection_bucket_viewer" {
  bucket = google_storage_bucket.bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_bigquery_connection.connection.cloud_resource[0].service_account_id}"
}

// Dataplex lake, zone and asset
resource "google_dataplex_lake" "lake" {
  lake_id      = var.lake_id
  project      = var.project_id
  location     = var.region
  display_name = "Demo Lake"
  description  = "Dataplex lake for raw data"
}
resource "google_dataplex_zone" "zone" {
  project      = var.project_id
  lake         = google_dataplex_lake.lake.name
  location     = var.region
  zone_id      = var.zone_id
  type         = "RAW"
  display_name = "Raw Zone"
  description  = "Raw data zone for staging files"
  resource_spec { location_type = "SINGLE_REGION" }
  discovery_spec { enabled = true include_patterns = ["**"] exclude_patterns = [] }
  depends_on = [google_dataplex_lake.lake]
}
resource "google_dataplex_asset" "asset" {
  project      = var.project_id
  lake         = google_dataplex_lake.lake.name
  zone         = google_dataplex_zone.zone.name
  location     = var.region
  asset_id     = var.asset_id
  display_name = "Staging asset"
  description  = "Asset pointing at staging bucket"
  discovery_spec { enabled = true }
  resource_spec {
    name = "projects/_/buckets/${google_storage_bucket.bucket.name}"
    type = "STORAGE_BUCKET"
  }
  depends_on = [google_dataplex_zone.zone]
}

// Cloud Spanner instance and database
resource "google_spanner_instance" "instance" {
  name          = var.spanner_instance
  project       = var.project_id
  config        = "regional-${var.region}"
  display_name  = "BigQuery Iceberg Catalog"
  processing_units = 100
}
resource "google_spanner_database" "database" {
  name     = var.spanner_database
  instance = google_spanner_instance.instance.name
  project  = var.project_id
}

// Compute BigQuery service agent email
data "google_project" "current" { project_id = var.project_id }
locals {
  bigquery_service_agent = "service-${data.google_project.current.number}@gcp-sa-bigquery.iam.gserviceaccount.com"
}

// Grant Spanner database user to service agents
resource "google_spanner_database_iam_member" "bq_agent" {
  project  = var.project_id
  instance = google_spanner_instance.instance.name
  database = google_spanner_database.database.name
  role     = "roles/spanner.databaseUser"
  member   = "serviceAccount:${local.bigquery_service_agent}"
}
resource "google_spanner_database_iam_member" "sa_user" {
  project  = var.project_id
  instance = google_spanner_instance.instance.name
  database = google_spanner_database.database.name
  role     = "roles/spanner.databaseUser"
  member   = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

// Example BigLake managed table creation for demonstration.  Create your
// tables in terraform/init_table/<table> modules instead.
resource "null_resource" "example_table" {
  provisioner "local-exec" {
    command = <<EOT
    bq --project_id=${var.project_id} --location=${var.region} query --nouse_legacy_sql "\
    CREATE SCHEMA IF NOT EXISTS \`${var.project_id}.${var.dataset_raw}\`;\
    CREATE TABLE IF NOT EXISTS \`${var.project_id}.${var.dataset_raw}.example\` (\
      id STRING\
    )\
    WITH CONNECTION \`${var.region}.${google_bigquery_connection.connection.connection_id}\`\
    OPTIONS (\
      file_format = 'PARQUET',\
      table_format = 'ICEBERG',\
      storage_uri = 'gs://${var.bucket_name}/${var.biglake_storage_prefix}'\
    );"
    EOT
  }
  triggers = {
    // ensure the command runs when variables change
    always_run = timestamp()
  }
  depends_on = [
    google_bigquery_connection.connection,
    google_storage_bucket_iam_member.connection_bucket_viewer,
    google_spanner_database_iam_member.bq_agent,
    google_spanner_database_iam_member.sa_user
  ]
}