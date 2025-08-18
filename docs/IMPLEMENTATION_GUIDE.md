# The1 Initiate Pipeline - Implementation Guide

## 🚀 Quick Start

### Prerequisites
1. GCP Project with billing enabled
2. Service Account `sa-demo-the1` with required permissions
3. AWS credentials in Secret Manager (`aws-access-key-id`, `aws-secret-access-key`)
4. BigQuery datasets created (`demo_the1_staging`, `demo_the1_raw`)
5. GCS bucket created (`demo-central-the1`)
6. SBT installed locally

### File Structure
```
the1-initiate-pipeline/
├── src/main/scala/the1/initiate/
│   ├── Main.scala                    # Main entry point (updated)
│   ├── services/
│   │   ├── StsService.scala          # Storage Transfer Service
│   │   ├── SchemaService.scala       # Schema from mapping.json
│   │   ├── BigQueryService.scala     # BigQuery operations
│   │   └── ValidationService.scala   # Data validation
│   └── logging/
│       └── GcsLogger.scala           # Logging to GCS
├── config/
│   └── member_address/
│       ├── job.yaml                  # Job configuration
│       └── mapping.json              # Column mappings
├── build.sbt                         # Build configuration
├── deploy.sh                         # Build and deploy script
└── run_pipeline.sh                   # Run pipeline script
```

## 📦 Setup Instructions

### 1. Copy Service Files

Create the directory structure and copy all the service files:

```bash
# Create directories
mkdir -p src/main/scala/the1/initiate/services
mkdir -p src/main/scala/the1/initiate/logging

# Copy the service files from artifacts above:
# - StsService.scala → src/main/scala/the1/initiate/services/
# - SchemaService.scala → src/main/scala/the1/initiate/services/
# - BigQueryService.scala → src/main/scala/the1/initiate/services/
# - ValidationService.scala → src/main/scala/the1/initiate/services/
# - GcsLogger.scala → src/main/scala/the1/initiate/logging/
# - Main.scala → src/main/scala/the1/initiate/ (replace existing)
```

### 2. Update build.sbt

Replace your existing `build.sbt` with the updated version above.

### 3. Add Scripts

```bash
# Make scripts executable
chmod +x deploy.sh
chmod +x run_pipeline.sh
```

### 4. Update Configuration

Edit `config/member_address/job.yaml`:

```yaml
projectId: ntt-test-data-bq-looker
datasetExternal: demo_the1_staging
datasetFinal: demo_the1_raw
gcsBucket: demo-central-the1
connectionId: demo_gcs_iceberg_connection  # Add this
region: asia-southeast1                     # Add this
```

## 🏃 Running the Pipeline

### Option 1: Build and Deploy

```bash
# Build, upload JAR and configs, optionally run
./deploy.sh

# Or specify table
./deploy.sh member_address
```

### Option 2: Run Specific Table

```bash
# Run pipeline for a table
./run_pipeline.sh member_address

# Run with specific mode
./run_pipeline.sh member_address MERGE
```

### Option 3: Manual Commands

```bash
# Build
sbt clean assembly

# Upload JAR
gsutil cp target/scala-2.12/*.jar gs://demo-central-the1/jars/

# Upload configs
gsutil -m rsync -r config/ gs://demo-central-the1/config/

# Submit job
gcloud dataproc batches submit spark \
  --project=ntt-test-data-bq-looker \
  --region=asia-southeast1 \
  --subnet=default \
  --service-account=sa-demo-the1@ntt-test-data-bq-looker.iam.gserviceaccount.com \
  --jar=gs://demo-central-the1/jars/the1-initiate-pipeline-1.0.0.jar \
  --class=the1.initiate.Main \
  -- gs://demo-central-the1/config/member_address/job.yaml
```

## 📊 Monitoring

### Check Logs
```bash
# View logs in GCS
gsutil cat "gs://demo-central-the1/data-platform/logs/$(date +%Y/%m/%d)/*"

# View Dataproc job logs
gcloud dataproc batches describe JOB_ID --region=asia-southeast1
```

### Verify Data
```bash
# Check row counts
bq query --use_legacy_sql=false "
  SELECT 
    'external' as table_type,
    COUNT(*) as row_count 
  FROM demo_the1_staging.member_address_ext
  UNION ALL
  SELECT 
    'managed' as table_type,
    COUNT(*) as row_count 
  FROM demo_the1_raw.member_address
"
```

## 🔧 Troubleshooting

### Common Issues

1. **STS Transfer Fails**
   - Check AWS credentials in Secret Manager
   - Verify S3 bucket and prefix exist
   - Check IAM permissions

2. **BigQuery Table Creation Fails**
   - Verify BigLake connection exists
   - Check Spanner instance/database
   - Verify IAM permissions for connection service account

3. **Validation Fails**
   - Check for NULL values in key columns
   - Verify column mappings in mapping.json
   - Check data types compatibility

### Debug Mode

Add logging verbosity:
```scala
// In Main.scala
logger.info(s"Debug: ${someVariable}")
```

## 🎯 What the Pipeline Does

1. **Reads Configuration** - Loads job.yaml and mapping.json
2. **Schema Discovery** - Gets schema from mapping.json
3. **Creates Tables** - Creates managed Iceberg table if not exists
4. **STS Transfer** - Copies data from S3 to GCS with monitoring
5. **External Table** - Creates/refreshes BigLake external table
6. **Data Loading** - Loads data into managed table (TRUNCATE/MERGE)
7. **Validation** - Validates row counts and checksums
8. **Logging** - Writes logs to GCS for audit trail

## 📈 Performance Tips

1. **Parallel Tables** - Run multiple tables in parallel
2. **Partitioning** - Use partitioned tables for large datasets
3. **Clustering** - Add clustering on frequently filtered columns
4. **Batch Size** - Adjust STS transfer batch size for large files

## 🔐 Security Notes

- AWS credentials stored in Secret Manager
- Service account with minimal required permissions
- Audit logs written to GCS
- No credentials in code or configs

## 📝 Next Steps

1. **Add More Tables** - Copy config folder structure
2. **Incremental Loads** - Modify to support delta loads
3. **Scheduling** - Add Cloud Scheduler/Airflow
4. **Monitoring** - Add Cloud Monitoring metrics
5. **Testing** - Add unit tests for services

## 💡 Tips

- Always test with small dataset first
- Use `TRUNCATE` mode for initial loads
- Use `MERGE` mode for updates
- Check logs immediately if job fails
- Monitor STS job in Storage Transfer console

## 🆘 Support

For issues:
1. Check logs in `gs://demo-central-the1/data-platform/logs/`
2. Review Dataproc batch job details
3. Verify all prerequisites are met
4. Check IAM permissions