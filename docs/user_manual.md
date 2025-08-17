# User Manual

This guide explains how to deploy and run the **The1 Initiate Pipeline** in your own environment.  It assumes that you have cloned or downloaded this repository and have the necessary permissions in your Google Cloud project.

## 1 Prerequisites

1. A Google Cloud project with billing enabled.
2. The [Google Cloud CLI](https://cloud.google.com/sdk) installed locally.
3. [Terraform](https://developer.hashicorp.com/terraform/downloads) installed locally.
4. [SBT](https://www.scala-sbt.org/) installed if you plan to build the Scala pipeline.
5. The gcloud alpha component installed for Dataproc Serverless.
6. AWS access key and secret stored in Secret Manager under the names `aws-access-key-id` and `aws-secret-access-key` for STS.

## 2 Deploy infrastructure

### 2.1 Clone the repository

```bash
git clone https://github.com/WasinW/the1-initiate-pipeline.git
cd the1-initiate-pipeline
```

### 2.2 Project‑level infrastructure

Navigate to the project‑level Terraform folder and apply:

```bash
cd terraform/project
terraform init
terraform apply \
  -var="project_id=YOUR_PROJECT_ID" \
  -var="region=asia-southeast1" \
  -var="bucket_name=demo-central-the1" \
  -var="service_account_id=sa-demo-the1" \
  -var="dataset_staging=demo_the1_staging" \
  -var="dataset_raw=demo_the1_raw" \
  -var="connection_id=demo_gcs_iceberg_connection" \
  -var="lake_id=demo-lake" \
  -var="zone_id=unmanaged" \
  -var="asset_id=stagingassetdemothe1" \
  -var="spanner_instance=bigquery" \
  -var="spanner_database=demo-the1-datazone" \
  -var="biglake_storage_prefix=biglake/raw_member_address"
```

This will create the bucket, datasets, connection, Dataplex lake/zone/asset, Spanner catalog and IAM policies.  If any of these resources already exist, import them into your state before applying.  The bucket and datasets are protected from accidental deletion via `prevent_destroy`.

### 2.3 Table‑specific infrastructure

For each table you wish to initialise, apply the Terraform in its folder.  For the `member_address` table:

```bash
cd ../../terraform/init_table/member_address
terraform init
terraform apply \
  -var="project_id=YOUR_PROJECT_ID" \
  -var="region=asia-southeast1" \
  -var="dataset_raw=demo_the1_raw" \
  -var="bucket_name=demo-central-the1" \
  -var="connection_id=demo_gcs_iceberg_connection" \
  -var="storage_prefix=biglake/raw_member_address"
```

This creates the managed BigLake table `member_address` with the schema described in the module.  It uses `CREATE IF NOT EXISTS` so running it multiple times is safe.  For additional tables, replicate this module with the appropriate schema and storage prefix.

## 3 Configure the pipeline

### 3.1 Edit job configuration

The YAML file under `config/member_address/job.yaml` defines where the data comes from and where it goes.  Update the following fields:

  * `projectId` – your GCP project ID.
  * `datasetExternal` – the dataset where the external (BigLake) table resides (`demo_the1_staging` in our example).
  * `datasetFinal` – the dataset containing the managed table (`demo_the1_raw`).
  * `gcsBucket` – the destination bucket (`demo-central-the1`).
  * `source.s3Bucket` and `source.prefix` – the AWS S3 bucket and prefix of the source data.
  * `destination.gcsPrefix` – the GCS prefix where files will be copied (should match the BigLake external table location).

### 3.2 Edit the mapping file

The JSON file `config/member_address/mapping.json` contains two arrays:

  * `selectedColumns` – a list of column names to read from the source.  If the source table has additional columns, they will be ignored.
  * `columnMapping` – a list of objects with `expr` (source column or expression) and `as` (target column name).  This is used when loading into the managed table.  For example, you could cast types or rename columns here.

You may add or remove columns in these arrays to match your requirements.

## 4 Build and run the pipeline

### 4.1 Build

In the repository root, run:

```bash
sbt assembly
```

This compiles the Scala code and produces a fat JAR in `target/scala-2.12/the1-initiate-pipeline-assembly-0.1.0-SNAPSHOT.jar`.

### 4.2 Submit to Dataproc Serverless

Use the gcloud CLI to submit a Dataproc Serverless batch:

```bash
gcloud dataproc batches submit \
  --project=YOUR_PROJECT_ID \
  --region=asia-southeast1 \
  --subnet=YOUR_VPC_SUBNET \
  --service-account=sa-demo-the1@YOUR_PROJECT_ID.iam.gserviceaccount.com \
  --jars=gs://YOUR_BUCKET/the1-initiate-pipeline-assembly-0.1.0-SNAPSHOT.jar \
  --class=the1.initiate.Main \
  --args=gs://YOUR_BUCKET/config/member_address/job.yaml
```

Replace `YOUR_VPC_SUBNET` with your Dataproc subnet and `YOUR_BUCKET` with a bucket where you uploaded the JAR and config files.  The pipeline will copy data from S3 to GCS, refresh the external table, load data into the managed table and validate row counts.

## 5 Operational notes

  * **Idempotency** – Both the STS copy and the BigQuery DDL/DML are designed to be idempotent.  You can re‑run a job for the same table and date range without creating duplicate data.  Make sure to set `overwriteWhenDifferent` on STS jobs.
  * **Error handling** – Extend the stubbed methods in `Main.scala` to handle transient errors (e.g. network failures) and implement retry logic.  Consider logging progress and metrics.
  * **Security** – Store AWS credentials securely in Secret Manager.  Use the principle of least privilege when granting IAM roles.  The sample Terraform grants broad roles for simplicity; tighten them for production.
  * **Monitoring** – Enable Cloud Logging and Cloud Monitoring on your Dataproc jobs.  Consider emitting metrics on transferred bytes, row counts and errors.

## 6 Extending for new tables

To migrate another table:

  1. Add a folder under `config/<table_name>` with `job.yaml` and `mapping.json` customised for that table.
  2. Copy `terraform/init_table/member_address` to `terraform/init_table/<table_name>` and adjust the schema and `storage_prefix`.
  3. Run `terraform apply` in the new folder to create the managed table.
  4. Add an entry to the `tables` list in your YAML job file.  The pipeline will pick it up automatically.

## 7 Troubleshooting

  * **Duplicate resources** – If Terraform fails because a resource already exists, import it into the state with `terraform import` before re‑applying.  Resources such as buckets, datasets and Spanner instances are often shared across projects.
  * **Permission errors** – Ensure that the custom service account and BigQuery service agent have `spanner.sessions.create` and other required permissions【143566545041957†L6185-L6203】.
  * **STS job hangs** – Check the STS transfer logs in Cloud Logging.  Ensure that the S3 prefix and GCS prefix are correct and that the AWS credentials have the necessary permissions.
