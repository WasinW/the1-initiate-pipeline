# The1 Initiate Pipeline – Extended

This repository provides an **end‑to‑end framework** for performing one‑off or backfilled
data migrations from an AWS data lake into Google Cloud.  It builds upon
the original skeleton pipeline and incorporates infrastructure provisioning
via Terraform, a modular table‑initialisation layer, a configurable Spark
pipeline and per‑table configuration files.  The goal is to enable
**idempotent**, repeatable and maintainable data migrations using best
practices and modern coding patterns.

## Repository layout

The codebase is organised into four major sections:

| Path | Purpose |
|-----|---------|
| `terraform/project` | Provision all core infrastructure for the project.  This includes GCS buckets, BigQuery datasets, BigLake connections, Dataplex lakes/zones/assets, Cloud Spanner catalog and IAM bindings. |
| `terraform/init_table/<table>` | Table‑specific infrastructure.  Each folder contains Terraform or helper scripts that create the managed BigQuery table for the corresponding source table.  This separation allows you to initialise additional tables independently. |
| `src/main/scala/the1/initiate` | Scala code implementing the main initiation pipeline.  It reads the per‑table configuration, orchestrates the Storage Transfer Service (STS), creates/refreshes external tables, loads into managed tables and performs validation.  You can run it on Dataproc Serverless or locally for testing. |
| `config/<table>` | Per‑table configuration.  Each folder contains a `job.yaml` describing the source, destination and engine settings, and a `mapping.json` listing the columns to select and any renamings. |
| `docs` | Additional documentation, including a user manual for deployment and operation. |

The **project‑level Terraform** in `terraform/project` sets up everything
needed by any table: buckets, datasets, connections, Dataplex, Spanner and
IAM.  Once applied, you can provision individual managed tables by running
the Terraform in each `terraform/init_table/<table>` folder.  The pipeline
assumes these resources exist when it runs.

## Key components

### Infrastructure (Terraform)

* **Buckets and datasets** – A regional GCS bucket and two BigQuery datasets
  (external staging and final/raw) are created.  The bucket uses
  [uniform bucket‑level access](https://cloud.google.com/storage/docs/uniform-bucket-level-access).
* **BigLake connection** – A [`CLOUD_RESOURCE` connection](https://cloud.google.com/bigquery/docs/connection-properties)
  is created so that BigQuery can read and write files in GCS.  The
  connection’s service account is granted `storage.objectViewer` on the
  bucket so that external tables can read from it.
* **Dataplex lake/zone/asset** – A Dataplex lake and raw zone register the
  GCS bucket as an asset, enabling centralised metadata and governance.
* **Cloud Spanner catalog** – A Spanner instance and database store the
  Iceberg catalog used by BigLake managed tables.  BigQuery’s service
  agent and the custom service account are granted the
  `roles/spanner.databaseUser` role, which includes the
  `spanner.sessions.create` permission.
* **Managed table creation** – A `null_resource` runs a `bq` command
  with `CREATE TABLE … WITH CONNECTION … OPTIONS(file_format='PARQUET',
  table_format='ICEBERG', storage_uri='gs://…')` to create a BigLake
  managed Iceberg table.

The project‑level Terraform is idempotent; it uses
`prevent_destroy = true` on critical resources and `CREATE IF NOT EXISTS`
statements so that you can safely re‑apply without accidental deletions.
If a resource already exists, import it into Terraform state before
applying.

### Table‑specific initialisation

Each folder under `terraform/init_table` contains a single file that
executes a BigQuery DDL to create a managed table with the correct
schema for that table.  For example, `terraform/init_table/member_address`
creates the `member_address` table in the final dataset with the exact
column names and types you provide.

### Pipeline

The core pipeline is implemented in Scala in `src/main/scala/the1/initiate`.
It reads a YAML configuration describing your tables, copies data from S3
to GCS via STS, refreshes external tables, loads data into managed
tables and performs validation.  You should extend the stubbed methods
(`readSourceSchema`, `createOrRefreshExternalTable`, `runStsCopy`,
`loadIntoFinalTable` and `validateSourceAndTarget`) to integrate with
Glue/Athena, BigQuery and your validation logic.

### Configuration

For each table you migrate, create a folder under `config/` with two
files:

1. **`job.yaml`** – Describes the source location in S3, the destination
   GCS prefix, the BigQuery datasets/tables, and engine/validation options.
2. **`mapping.json`** – Contains two sections: `selectedColumns` lists
   the columns to select from the source, and `columnMapping` defines
   renamings or casting expressions for loading into the final table.  The
   pipeline will read this file and build the `columnMapping` section of
   the YAML at run time.

## Usage

1. **Deploy infrastructure**
   1. Install Terraform and authenticate to your GCP project.
   2. Navigate to `terraform/project` and run:
      ```bash
      terraform init
      terraform apply -var="project_id=YOUR_PROJECT_ID" -var="region=YOUR_REGION"
      ```
   3. Repeat for each table‑specific folder under `terraform/init_table` to
      create the managed table:
      ```bash
      cd terraform/init_table/member_address
      terraform init
      terraform apply -var="project_id=YOUR_PROJECT_ID" -var="region=YOUR_REGION"         -var="dataset_raw=demo_the1_raw" -var="bucket_name=demo-central-the1"
      ```

2. **Prepare your config and mapping**
   1. Copy `config/member_address/job.yaml` and edit `projectId`,
      `datasetExternal`, `datasetFinal`, `gcsBucket`, `s3Bucket` and other
      fields to match your environment.
   2. Edit `config/member_address/mapping.json` to list the columns you
      require and specify any renamings or casts.
   3. When you run the pipeline, the code will merge these definitions
      into the YAML configuration at runtime.

3. **Build and run the pipeline**
   1. Install SBT.
   2. Run `sbt assembly` to build a fat JAR.
   3. Submit the job to Dataproc Serverless:
      ```bash
      gcloud dataproc batches submit         --project=YOUR_PROJECT_ID --region=YOUR_REGION         --subnet=YOUR_SUBNET --service-account=sa-demo-the1@YOUR_PROJECT_ID.iam.gserviceaccount.com         --jars=gs://your-bucket/path/to/the1-initiate-pipeline-assembly.jar         --class=the1.initiate.Main         --args=gs://your-bucket/path/to/config/member_address/job.yaml
      ```

Refer to `docs/user_manual.md` for a more detailed step‑by‑step guide.
