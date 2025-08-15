# the1‑initiate‑pipeline

This repository contains a skeleton implementation of a **data‑initiation pipeline**
for migrating tables from an AWS EMR/S3/Redshift environment into Google Cloud
services.  It is based on the design discussed in our chat and is intended as a
starting point for your own implementation.  The goal of the pipeline is to
support one‑off initial loads as well as idempotent re‑runs/backfills
for individual partitions or time ranges.

The pipeline is written in **Scala** and is designed to run on
**Dataproc Serverless for Spark** or a regular Dataproc cluster.  It uses
Google Cloud's **Storage Transfer Service (STS)** to copy data from S3 to
Google Cloud Storage, then creates BigLake external tables, loads data into
managed BigQuery tables, and finally performs row‑count and value validation
against the source.  Configuration is driven by a YAML file so that new
tables can be added without code changes.

## Design overview

The pipeline performs the following steps for each table defined in your
configuration file:

1. **Read the schema from AWS**
   * If `schemaSource` is set to `glue`, the pipeline reads the table
     definition from AWS Glue/Athena.
   * If it is set to `infer`, the pipeline samples a few files in S3 and
     uses Apache Parquet/Arrow to infer the schema.

2. **Create a BigLake external table** pointing at the target GCS prefix.
   You can optionally enable Hive partition discovery by specifying
   `hivePartitioning` in the config.

3. **Copy data from S3 to GCS** using the Storage Transfer Service.  The
   pipeline creates a one‑off STS job for each partition/time range you
   specify in the job arguments.  STS takes care of parallelism,
   checksums and retry logic.  We recommend using `overwriteWhenDifferent` on
   the STS job to make re‑runs idempotent.

4. **Refresh the external table**.  Although BigQuery automatically
   discovers new objects, you can force a metadata refresh by re‑issuing
   `CREATE OR REPLACE EXTERNAL TABLE`.

5. **Load into the managed table**.  Use a `MERGE` or `INSERT` query to
   load data into your final dataset.  You can specify a list of column
   expressions in the config to select or cast columns.

6. **Validate**.  The pipeline computes simple statistics (row counts,
   checksums, min/max) from both the BigLake external table and the source
   system (Athena/Redshift) and reports any mismatches.

Everything is orchestrated from a single Spark driver written in Scala.
We do not actually use Spark's distributed processing for the copy
operation itself; STS handles that.  Spark is simply a convenient
environment for running Scala code on Dataproc Serverless.

## Files in this repository

* `init_job.yaml` – Example configuration file.  You can define multiple
  tables here, each with its source and destination settings.  This file
  drives the entire pipeline.
* `build.sbt` – SBT build definition listing dependencies for BigQuery,
  Storage Transfer Service, AWS Glue/Athena clients and logging.
* `src/main/scala/the1/initiate/Main.scala` – Entry point for the pipeline.
  It loads the YAML config, loops through tables, orchestrates STS jobs
  and executes BigQuery DDL/DML statements.  Function bodies are
  intentionally left as stubs for you to implement according to your
  environment and requirements.

## How to use

1. **Configure secrets**.  The pipeline assumes that AWS credentials (for
   S3 and Glue/Athena) are stored in Google Secret Manager.  The name of the
   secrets must be provided when instantiating the STS job.  Likewise,
   the pipeline uses Application Default Credentials to authenticate with
   Google Cloud services.

2. **Edit `init_job.yaml`**.  Define the tables you want to migrate,
   including their S3 prefixes, target GCS prefixes, BigQuery dataset/table
   names and column mappings.  You can also specify per‑table engine
   overrides if you need to fall back to Dataflow or Dataproc for special
   cases (e.g. files > 5 TiB).

3. **Build the project**.  Use SBT to download dependencies and build a
   JAR:

   ```bash
   sbt assembly
   ```

4. **Submit to Dataproc Serverless**.  Use the `gcloud` CLI to run the
   pipeline on Dataproc Serverless:

   ```bash
   gcloud dataproc batches submit --project=YOUR_PROJECT --region=YOUR_REGION \
     --subnet=YOUR_VPC_SUBNET --service-account=YOUR_SA \
     --jars=gs://path/to/built.jar --class=the1.initiate.Main \
     --args=gs://path/to/init_job.yaml
   ```

   Alternatively, you can run the JAR locally for testing:

   ```bash
   sbt run
   ```

This scaffold is provided as a starting point.  You will need to
implement the functions in `Main.scala` to suit your environment,
including proper error handling, partitioned backfill logic and detailed
validation.  Feel free to extend or reorganise the code as needed.