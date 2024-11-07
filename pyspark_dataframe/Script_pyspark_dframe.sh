#!/bin/bash

# Configurable parameters
REGION="europe-west1"
ZONE="europe-west1-b"
PROJECT="projectpagerank"
CLUSTER_NAME="pysparkpagerank-cluster"
BUCKET="gs://bucket_projet_pagerank_lahad_kikia"
INPUT_FILE="gs://public_lddm_data/small_page_links.nt"
OUTPUT_DIR="${BUCKET}/out/pagerank_data_1"
ITERATIONS=10
DAMPING_FACTOR=0.85
NUM_WORKERS=0 # 1 Noeud
#NUM_WORKERS=1 # 2 Noeud
#NUM_WORKERS=3 # 4 Noeud


echo "Starting PageRank processing script"

# Clean the output directory
echo "Cleaning output directory..."
gsutil -m rm -rf ${OUTPUT_DIR} || true

# Create Dataproc cluster
echo "Creating Dataproc cluster..."
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --enable-component-gateway \
    --region ${REGION} \
    --zone ${ZONE} \
    --master-machine-type n1-standard-4 \
    --master-boot-disk-size 500 \
    --num-workers ${NUM_WORKERS} \
    --worker-machine-type n1-standard-4 \
    --worker-boot-disk-size 500 \
    --image-version 2.0-debian10 \
    --project ${PROJECT}



# Record start time
start_time=$(date +%s)

# Submit PySpark job
echo "Submitting PySpark job..."
gcloud dataproc jobs submit pyspark \
    --region ${REGION} \
    --cluster ${CLUSTER_NAME} \
    --properties spark.executor.memory=4g,spark.driver.memory=4g \
    --files ${BUCKET}/pyspark_dframe.py \
    ${BUCKET}/pyspark_dframe.py \
    -- ${INPUT_FILE} ${OUTPUT_DIR} ${ITERATIONS} ${DAMPING_FACTOR}

# Record end time
end_time=$(date +%s)

# Calculate execution time
execution_time=$((end_time - start_time))

# Create execution info file
echo "Creating execution info file..."
echo "Execution time: ${execution_time} seconds" > execution_info.txt
echo "Number of nodes: $((NUM_WORKERS + 1))" >> execution_info.txt

# Upload execution info file to bucket
gsutil cp execution_info.txt ${OUTPUT_DIR}/execution_info.txt

# Check if job was successful
if [ $? -eq 0 ]; then
    echo "PySpark job succeeded. Displaying results:"
    gsutil cat ${OUTPUT_DIR}/part-00000
else
    echo "PySpark job failed. Check logs for more information."
fi

# Delete cluster
echo "Deleting cluster..."
gcloud dataproc clusters delete ${CLUSTER_NAME} --region ${REGION} --quiet

echo "Script completed."