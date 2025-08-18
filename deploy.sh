#!/bin/bash

# The1 Initiate Pipeline - Build and Deploy Script

set -e  # Exit on error

# Configuration
PROJECT_ID="ntt-test-data-bq-looker"
REGION="asia-southeast1"
BUCKET="demo-central-the1"
SERVICE_ACCOUNT="sa-demo-the1@${PROJECT_ID}.iam.gserviceaccount.com"
SUBNET="default"  # Change this to your subnet

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}The1 Initiate Pipeline - Build & Deploy${NC}"
echo "========================================"

# Step 1: Clean and build
echo -e "${YELLOW}Step 1: Building JAR...${NC}"
sbt clean assembly

if [ $? -ne 0 ]; then
    echo -e "${RED}Build failed!${NC}"
    exit 1
fi

JAR_FILE=$(ls target/scala-2.12/*-1.0.0.jar | head -n 1)
echo -e "${GREEN}Built: $JAR_FILE${NC}"

# Step 2: Upload JAR to GCS
echo -e "${YELLOW}Step 2: Uploading JAR to GCS...${NC}"
JAR_GCS_PATH="gs://${BUCKET}/data-platform/framework/initiate/jars/$(basename $JAR_FILE)"
gsutil cp $JAR_FILE $JAR_GCS_PATH

if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to upload JAR!${NC}"
    exit 1
fi
echo -e "${GREEN}Uploaded to: $JAR_GCS_PATH${NC}"

# Step 3: Upload configs to GCS
echo -e "${YELLOW}Step 3: Uploading configs to GCS...${NC}"
gsutil -m rsync -r config/ gs://${BUCKET}/data-platform/bu/config/

if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to upload configs!${NC}"
    exit 1
fi
echo -e "${GREEN}Configs uploaded to: gs://${BUCKET}/data-platform/bu/config/${NC}"

# Step 4: Create service directories in GCS if not exists
echo -e "${YELLOW}Step 4: Creating service directories...${NC}"
gsutil -q stat gs://${BUCKET}/data-platform/logs/ || gsutil mkdir -p gs://${BUCKET}/data-platform/logs/

# Step 5: Submit job (optional - can be run separately)
read -p "Do you want to submit the job now? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    TABLE_NAME="${1:-member_address}"
    echo -e "${YELLOW}Submitting job for table: $TABLE_NAME${NC}"
    
    JOB_ID="the1-initiate-$(date +%Y%m%d-%H%M%S)"
    
    gcloud dataproc batches submit spark \
        --project=$PROJECT_ID \
        --region=$REGION \
        --batch=$JOB_ID \
        --subnet=$SUBNET \
        --service-account=$SERVICE_ACCOUNT \
        --jar=$JAR_GCS_PATH \
        --class=the1.initiate.Main \
        -- gs://${BUCKET}/data-platform/bu/config/${TABLE_NAME}/job.yaml

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Job submitted successfully!${NC}"
        echo -e "Job ID: ${JOB_ID}"
        echo -e "Monitor at: https://console.cloud.google.com/dataproc/batches/${JOB_ID}?project=${PROJECT_ID}"
    else
        echo -e "${RED}Job submission failed!${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}Build and upload completed!${NC}"
    echo ""
    echo "To run the pipeline manually:"
    echo -e "${YELLOW}gcloud dataproc batches submit spark \\"
    echo "    --project=$PROJECT_ID \\"
    echo "    --region=$REGION \\"
    echo "    --subnet=$SUBNET \\"
    echo "    --service-account=$SERVICE_ACCOUNT \\"
    echo "    --jar=$JAR_GCS_PATH \\"
    echo "    --class=the1.initiate.Main \\"
    echo "    -- gs://${BUCKET}/data-platform/bu/config/${TABLE_NAME}/job.yaml${NC}"
fi

echo ""
echo -e "${GREEN}Done!${NC}"