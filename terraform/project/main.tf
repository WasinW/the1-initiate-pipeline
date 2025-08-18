// Project-level infrastructure for The1 Initiate Pipeline
//
// This file provisions all core resources required by any table migration:
// - GCS bucket
// - BigQuery datasets
// - Service account and IAM bindings
// - BigLake connection
// - Dataplex lake, zone and asset
// - Spanner instance and database for Iceberg catalog
// - IAM bindings for Spanner

terraform {
  required_version = ">= 1.3"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.28"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

variable "project_id" {
  type        = string
  description = "GCP Project ID"
}

variable "region" {
  type        = string
  description = "GCP Region"
}

variable "bucket_name" {
  type        = string
  description = "GCS Bucket name for staging and raw data"
}

variable "service_account_id" {
  type        = string
  description = "Service Account ID for pipeline"
}

variable "dataset_staging" {
  type        = string
  description = "BigQuery dataset for external staging tables"
}

variable "dataset_raw" {
  type        = string
  description = "BigQuery dataset for managed raw tables"
}

variable "connection_id" {
  type        = string
  description = "BigQuery connection ID"
}

variable "lake_id" {
  type        = string
  description = "Dataplex lake ID"
}

variable "zone_id" {
  type        = string
  description = "Dataplex zone ID"
}

variable "asset_id" {
  type        = string
  description = "Dataplex asset ID"
}

variable "spanner_instance" {
  type        = string
  description = "Spanner instance name"
}

variable "spanner_database" {
  type        = string
  description = "Spanner database name"
}

variable "biglake_storage_prefix" {
  type        = string
  description = "Storage prefix for BigLake tables"
}

// Data sources
data "google_project" "current" {
  project_id = var.project_id
}

locals {
  bigquery_service_agent = "service-${data.google_project.current.number}@gcp-sa-bigquery.iam.gserviceaccount.com"
}

// Enable required APIs
resource "google_project_service" "bigquery" {
  project            = var.project_id
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "bigquery_connection" {
  project            = var.project_id
  service            = "bigqueryconnection.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "dataplex" {
  project            = var.project_id
  service            = "dataplex.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "spanner" {
  project            = var.project_id
  service            = "spanner.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "storage" {
  project            = var.project_id
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "storage_transfer" {
  project            = var.project_id
  service            = "storagetransfer.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "secretmanager" {
  project            = var.project_id
  service            = "secretmanager.googleapis.com"
  disable_on_destroy = false
}

// Bucket to hold staging and managed data
resource "google_storage_bucket" "bucket" {
  name                        = var.bucket_name
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 365
    }
  }

  lifecycle {
    prevent_destroy = true
  }
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

resource "google_project_iam_member" "sa_sts_admin" {
  project = var.project_id
  role    = "roles/storagetransfer.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "sa_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

// Datasets
resource "google_bigquery_dataset" "staging" {
  dataset_id  = var.dataset_staging
  project     = var.project_id
  location    = var.region
  description = "External staging dataset"

  lifecycle {
    prevent_destroy = true
    ignore_changes = all  # Add this line

  }
}

resource "google_bigquery_dataset" "raw" {
  dataset_id  = var.dataset_raw
  project     = var.project_id
  location    = var.region
  description = "Managed raw dataset"

  lifecycle {
    prevent_destroy = true
    ignore_changes = all  # Add this line
  }
}

// BigQuery connection
resource "google_bigquery_connection" "connection" {
  project       = var.project_id
  location      = var.region
  connection_id = var.connection_id
  friendly_name = "GCS Iceberg Connection"
  description   = "Connection used by BigQuery to access GCS for BigLake tables"

  cloud_resource {}
}

// Grant the connection service account read access on the bucket
resource "google_storage_bucket_iam_member" "connection_bucket_viewer" {
  bucket = google_storage_bucket.bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_bigquery_connection.connection.cloud_resource[0].service_account_id}"
}

// Grant the connection service account write access for managed tables
resource "google_storage_bucket_iam_member" "connection_bucket_creator" {
  bucket = google_storage_bucket.bucket.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_bigquery_connection.connection.cloud_resource[0].service_account_id}"
}

// Dataplex lake
resource "google_dataplex_lake" "lake" {
  name     = var.lake_id
  location = var.region
  project  = var.project_id
  # lake_id  = var.lake_id    # ไม่มี underscore นำหน้า

  # display_name = "The1 Data Lake"
  # description  = "Dataplex lake for The1 data migration"
}

// Dataplex zone
resource "google_dataplex_zone" "zone" {
  name     = var.zone_id
  location = var.region
  lake     = google_dataplex_lake.lake.id
  project  = var.project_id
  type     = "RAW"

  # display_name = "Raw Zone"
  # description  = "Raw data zone for staging files"

  resource_spec {
    location_type = "SINGLE_REGION"
  }

  discovery_spec {
    enabled = true
    # include_patterns = ["data-zone/**"]
    # exclude_patterns = ["*.tmp"]
  }
}

// Dataplex asset
# resource "google_dataplex_asset" "asset" {
#   name          = var.asset_id
#   location      = var.region
#   lake          = google_dataplex_lake.lake.id
#   dataplex_zone = google_dataplex_zone.zone.id
#   project       = var.project_id

#   # display_name = "Staging Asset"
#   # description  = "Asset pointing at staging bucket"

#   discovery_spec {
#     enabled = true
#     # include_patterns = ["data-zone/staging/**", "data-zone/raw/**"]
#   }

#   resource_spec {
#     name = "projects/_/buckets/${google_storage_bucket.bucket.name}"
#     type = "STORAGE_BUCKET"
#   }
# }

// Cloud Spanner instance and database
resource "google_spanner_instance" "instance" {
  name             = var.spanner_instance
  project          = var.project_id
  config           = "regional-${var.region}"
  display_name     = "BigQuery Iceberg Catalog"
  processing_units = 100

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_spanner_database" "database" {
  name     = var.spanner_database
  instance = google_spanner_instance.instance.name
  project  = var.project_id

  lifecycle {
    prevent_destroy = true
  }
}

// Grant Spanner database user to BigQuery service agent
# resource "google_spanner_database_iam_member" "bq_agent" {
#   project  = var.project_id
#   instance = google_spanner_instance.instance.name
#   database = google_spanner_database.database.name
#   role     = "roles/spanner.databaseUser"
#   member   = "serviceAccount:${local.bigquery_service_agent}"
# }

# // Grant Spanner database user to custom service account
# resource "google_spanner_database_iam_member" "sa_user" {
#   project  = var.project_id
#   instance = google_spanner_instance.instance.name
#   database = google_spanner_database.database.name
#   role     = "roles/spanner.databaseUser"
#   member   = "serviceAccount:${google_service_account.pipeline_sa.email}"
# }

// Create directory structure in bucket
resource "google_storage_bucket_object" "staging_folder" {
  name    = "data-zone/staging/.keep"
  bucket  = google_storage_bucket.bucket.name
  content = "# Staging folder for external tables"
}

resource "google_storage_bucket_object" "raw_folder" {
  name    = "data-zone/raw/.keep"
  bucket  = google_storage_bucket.bucket.name
  content = "# Raw folder for managed tables"
}

resource "google_storage_bucket_object" "biglake_folder" {
  name    = "biglake/.keep"
  bucket  = google_storage_bucket.bucket.name
  content = "# BigLake managed tables storage"
}

// Output important values
output "bucket_name" {
  value = google_storage_bucket.bucket.name
}

output "connection_name" {
  value = "${var.region}.${google_bigquery_connection.connection.connection_id}"
}

output "service_account_email" {
  value = google_service_account.pipeline_sa.email
}

output "bigquery_service_agent" {
  value = local.bigquery_service_agent
}

output "spanner_instance" {
  value = google_spanner_instance.instance.name
}

output "spanner_database" {
  value = google_spanner_database.database.name
}