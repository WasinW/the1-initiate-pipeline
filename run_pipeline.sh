#!/bin/bash

# The1 Initiate Pipeline - Run Script

set -e  # Exit on error

# Configuration
PROJECT_ID="ntt-test-data-bq-looker"
REGION="asia-southeast1"
BUCKET="demo-central-the1"
SERVICE_ACCOUNT="sa-demo-the1@${PROJECT_ID}.iam.gserviceaccount.com"
SUBNET="default"  # Change this to your VPC subnet
JAR_VERSION="1.0.0"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check arguments
if [ $# -lt 1 ]; then
    echo -e "${RED}Usage: $0 <table_name> [mode]${NC}"
    echo -e "Example: $0 member_address"
    echo -e "Example: $0 member_address TRUNCATE"
    echo ""
    echo "Available tables:"
    ls -1 config/ 2>/dev/null | grep -v README || echo "No tables configured"
    exit 1
fi

TABLE_NAME=$1
MODE=${2:-"TRUNCATE"}  # Default to TRUNCATE mode

echo -e "${GREEN}═══════════════════════════════════════════════${NC}"
echo -e "${GREEN}     The1 Initiate Pipeline - Run Job${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════${NC}"
echo ""
echo -e "${BLUE}Configuration:${NC}"
echo -e "  Project ID:      $PROJECT_ID"
echo -e "  Region:          $REGION"
echo -e "  Bucket:          $BUCKET"
echo -e "  Table:           $TABLE_NAME"
echo -e "  Load Mode:       $MODE"
echo ""

# Check if config exists
CONFIG_PATH="config/${TABLE_NAME}/job.yaml"
MAPPING_PATH="config/${TABLE_NAME}/mapping.json"

if [ ! -f "$CONFIG_PATH" ]; then
    echo -e "${RED}Error: Configuration not found: $CONFIG_PATH${NC}"
    exit 1
fi

if [ ! -f "$MAPPING_PATH" ]; then
    echo -e "${RED}Error: Mapping not found: $MAPPING_PATH${NC}"
    exit 1
fi

echo -e "${YELLOW}Found configuration files:${NC}"
echo -e "  ✓ $CONFIG_PATH"
echo -e "  ✓ $MAPPING_PATH"
echo ""

# Generate job ID
JOB_ID="the1-${TABLE_NAME}-$(date +%Y%m%d-%H%M%S)"
JAR_PATH="gs://${BUCKET}/data-platform/framework/initiate/jars/the1-initiate-pipeline-${JAR_VERSION}.jar"

echo -e "${YELLOW}Checking JAR file...${NC}"
gsutil -q stat $JAR_PATH
if [ $? -ne 0 ]; then
    echo -e "${RED}JAR not found at: $JAR_PATH${NC}"
    echo -e "Please run deploy.sh first"
    exit 1
fi
echo -e "  ✓ JAR found: $JAR_PATH"
echo ""

# Confirm before running
echo -e "${YELLOW}Ready to submit job${NC}"
read -p "Continue? (y/n) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${RED}Cancelled${NC}"
    exit 0
fi

# Submit the job
echo -e "${YELLOW}Submitting Dataproc batch job...${NC}"
echo ""

gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch=$JOB_ID \
    --subnet=$SUBNET \
    --service-account=$SERVICE_ACCOUNT \
    --jar=$JAR_PATH \
    --class=the1.initiate.Main \
    --properties="spark.executor.memory=4g,spark.executor.cores=2,spark.dynamicAllocation.enabled=true" \
    -- gs://${BUCKET}/data-platform/bu/config/${TABLE_NAME}/job.yaml

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}═══════════════════════════════════════════════${NC}"
    echo -e "${GREEN}          Job Submitted Successfully!${NC}"
    echo -e "${GREEN}═══════════════════════════════════════════════${NC}"
    echo ""
    echo -e "${BLUE}Job Details:${NC}"
    echo -e "  Job ID:     $JOB_ID"
    echo -e "  Table:      $TABLE_NAME"
    echo ""
    echo -e "${BLUE}Monitor Progress:${NC}"
    echo -e "  Console:    https://console.cloud.google.com/dataproc/batches/${JOB_ID}?project=${PROJECT_ID}"
    echo -e "  Logs:       gs://${BUCKET}/data-platform/logs/$(date +%Y/%m/%d)/${TABLE_NAME}_*"
    echo ""
    echo -e "${BLUE}Check Results:${NC}"
    echo -e "  bq query --use_legacy_sql=false \"SELECT COUNT(*) FROM demo_the1_raw.${TABLE_NAME}\""
    echo ""
else
    echo -e "${RED}Job submission failed!${NC}"
    exit 1
fi