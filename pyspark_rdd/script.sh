#!/bin/bash

# Argument pour spécifier le nombre de workers
NUM_WORKERS=$1

# Variables générales
PROJECT_ID="kikia50727"
BUCKET_NAME="kikia_lahad_bucket"
REGION="europe-west1"
ZONE="europe-west1-c"
CLUSTER_NAME="pagerank-cluster-${NUM_WORKERS}-nodes"
INPUT_FILE="gs://${BUCKET_NAME}/page_links_en.nt.bz2"
SCRIPT_NAME="pyspark_rdd.py"

# Activer l'API Dataproc si elle n'est pas activée
# gcloud services enable dataproc.googleapis.com

# Créer le cluster Dataproc
# echo "Création du cluster Dataproc..."
# if ! gcloud dataproc clusters create ${CLUSTER_NAME} \
#     --enable-component-gateway \
#     --region ${REGION} \
#     --zone ${ZONE} \
#     --master-machine-type n1-standard-4 \
#     --master-boot-disk-size 500 \
#     --num-workers ${NUM_WORKERS} \
#     --worker-machine-type n1-standard-4 \
#     --worker-boot-disk-size 500 \
#     --image-version 2.0-debian10 \
#     --project ${PROJECT_ID}; then
#     echo "Échec de la création du cluster. Sortie."
#     exit 1
# fi

# if ! gcloud dataproc clusters create ${CLUSTER_NAME} \
#     --enable-component-gateway \
#     --region ${REGION} \
#     --zone ${ZONE} \
#     --single-node \
#     --master-machine-type n1-standard-4 \
#     --master-boot-disk-size 500 \
#     --worker-boot-disk-size 500 \
#     --image-version 2.0-debian10 \
#     --project ${PROJECT_ID}; then
#     echo "Échec de la création du cluster. Sortie."
#     exit 1
# fi


# Fichier pour stocker les temps d'exécution
TIMING_FILE="execution_time_${NUM_WORKERS}_nodes.txt"

# Enregistrer l'heure de début
start_time=$(date +%s)

# Exécution du script PySpark `pyspark_rdd.py`
echo "Exécution de $SCRIPT_NAME" >> $TIMING_FILE
{ time gcloud dataproc jobs submit pyspark gs://$BUCKET_NAME/scripts/$SCRIPT_NAME \
    --cluster=$CLUSTER_NAME --region=$REGION \
    --properties=spark.executor.memory=8g,spark.executor.memoryOverhead=2048, \
    -- ${INPUT_FILE} gs://$BUCKET_NAME/${SCRIPT_NAME%.*}_output_${NUM_WORKERS}_nodes; } 2>> $TIMING_FILE
echo "" >> $TIMING_FILE

# Enregistrer l'heure de fin
end_time=$(date +%s)

# Calculer le temps d'exécution
execution_time=$((end_time - start_time))

# Créer un fichier d'information d'exécution
echo "Création du fichier d'information d'exécution..."
echo "Temps d'exécution : ${execution_time} secondes" 
# echo "Nombre de nœuds : $((NUM_WORKERS))" >> execution_info.txt


# Afficher les temps d'exécution
echo "Temps d'exécution pour $SCRIPT_NAME :"
cat $TIMING_FILE

# Copier le fichier de timing dans le bucket
gsutil cp $TIMING_FILE gs://$BUCKET_NAME/execution_times/

# Supprimer le cluster
echo "Suppression du cluster..."

gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION --quiet
echo "Script terminé."

