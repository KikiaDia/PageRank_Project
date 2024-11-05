#!/bin/bash

# Argument pour spécifier le nombre de workers
NUM_WORKERS=$1

# Variables générales
PROJECT_ID="lahad-436008"
BUCKET_NAME="output_bucket"
REGION="us-central1"
ZONE="us-central1-a"
CLUSTER_NAME="pagerank-cluster-${NUM_WORKERS}-nodes"

# Créer le cluster Dataproc
if [ "$NUM_WORKERS" -eq 0 ]; then
    gcloud dataproc clusters create $CLUSTER_NAME \
        --region=$REGION --zone=$ZONE --single-node \
        --master-machine-type=n1-standard-2 --master-boot-disk-size=50GB
else
    gcloud dataproc clusters create $CLUSTER_NAME \
        --region=$REGION --zone=$ZONE --num-workers=$NUM_WORKERS \
        --master-machine-type=n1-standard-2 --master-boot-disk-size=50GB \
        --worker-machine-type=n1-standard-2 --worker-boot-disk-size=50GB
fi

# Fichier pour stocker les temps d'exécution
TIMING_FILE="execution_times_${NUM_WORKERS}_nodes.txt"

# Exécution des différents scripts PageRank
for script in pagerank_rdd.py pagerank_rdd_partitioned.py pagerank_dataframe.py pagerank_dataframe_partitioned.py
do
    echo "Exécution de $script" >> $TIMING_FILE
    { time gcloud dataproc jobs submit pyspark gs://$BUCKET_NAME/scripts/$script \
        --cluster=$CLUSTER_NAME --region=$REGION \
        -- gs://public_lddm_data/graph_data.csv gs://$BUCKET_NAME/${script%.*}_output_${NUM_WORKERS}_nodes; } 2>> $TIMING_FILE
    echo "" >> $TIMING_FILE
done

# Supprimer le cluster après exécution
gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION --quiet

# Afficher les temps d'exécution
echo "Temps d'exécution pour chaque script :"
cat $TIMING_FILE

# Copier le fichier de timing dans le bucket
gsutil cp $TIMING_FILE gs://$BUCKET_NAME/execution_times/