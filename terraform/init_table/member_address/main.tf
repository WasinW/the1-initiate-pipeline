// Table‑specific Terraform for member_address
//
// This module relies on the project‑level infrastructure already
// provisioned in terraform/project.  It creates a managed BigLake
// Iceberg table with the precise schema required for the
// `member_address` dataset.

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.60"
    }
  }
}

variable "project_id" { type = string }
variable "region" { type = string }
variable "dataset_raw" { type = string }
variable "bucket_name" { type = string }
variable "connection_id" { type = string }
variable "storage_prefix" { type = string }

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "null_resource" "create_member_address" {
  provisioner "local-exec" {
    command = <<EOT
bq --project_id=${var.project_id} --location=${var.region} query --nouse_legacy_sql "\
CREATE SCHEMA IF NOT EXISTS \`${var.project_id}.${var.dataset_raw}\`;\
CREATE TABLE IF NOT EXISTS \`${var.project_id}.${var.dataset_raw}.member_address\`(\
  MEMBER_ID STRING,\
  MEMBER_NUMBER STRING,\
  ADDRESS_ID STRING,\
  ADDRESS_TYPE STRING,\
  HOUSING_TYPE_NAME STRING,\
  COUNTRY STRING,\
  SUB_DISTRICT STRING,\
  POSTAL_CODE STRING,\
  DISTRICT STRING,\
  CITY STRING,\
  MOBILE_AREA_CODE STRING,\
  HOME_AREA_CODE STRING,\
  INVALID_DATA STRING,\
  HOME_NO STRING,\
  BUILDING STRING,\
  FLOOR STRING,\
  MOO STRING,\
  SOI STRING,\
  YAK STRING,\
  ROAD STRING,\
  MOBILE_PHONE STRING,\
  HOME_PHONE STRING,\
  MAIN_ADDRESS STRING,\
  HOME_PHONE_EXTENSION STRING,\
  CREATED_DATE TIMESTAMP,\
  UPDATED_DATE TIMESTAMP,\
  etl_created_by STRING,\
  tbl_name STRING\
)\
WITH CONNECTION \`${var.region}.${var.connection_id}\`\
OPTIONS (\
  file_format = 'PARQUET',\
  table_format = 'ICEBERG',\
  storage_uri = 'gs://${var.bucket_name}/${var.storage_prefix}'\
);"
EOT
  }
  triggers = {
    always_run = timestamp()
  }
}