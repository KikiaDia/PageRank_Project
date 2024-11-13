#!/bin/bash

# Paramètres configurables
REGION="europe-west1"
ZONE="europe-west1-c"
PROJECT="projectpagerank"
CLUSTER_NAME="pagerank-cluster-1"
BUCKET="gs://bucket_projet_pagerank_lahad_kikia"
#INPUT_FILE="${BUCKET}/small_page_links.nt"
INPUT_FILE="${BUCKET}/page_links_en.nt.bz2"
OUTPUT_DIR="${BUCKET}/out/pagerank_data_1"
NUM_WORKERS=2

echo "Démarrage du script de traitement PageRank"

# Nettoyer le répertoire de sortie
echo "Nettoyage du répertoire de sortie..."
gsutil -m rm -rf ${OUTPUT_DIR} || true

# Créer le cluster Dataproc
echo "Création du cluster Dataproc..."
if ! gcloud dataproc clusters create ${CLUSTER_NAME} \
    --enable-component-gateway \
    --region ${REGION} \
    --zone ${ZONE} \
    --master-machine-type n1-standard-4 \
    --master-boot-disk-size 500 \
    --num-workers ${NUM_WORKERS} \
    --worker-machine-type n1-standard-4 \
    --worker-boot-disk-size 500 \
    --image-version 2.0-debian10 \
    --project ${PROJECT}; then
    echo "Échec de la création du cluster. Sortie."
    exit 1
fi

# Enregistrer l'heure de début
start_time=$(date +%s)

# Soumettre le job PySpark
echo "Soumission du job PySpark..."
if ! gcloud dataproc jobs submit pyspark \
    --region ${REGION} \
    --cluster ${CLUSTER_NAME} \
    --files ${BUCKET}/pyspark_rdd.py \
    ${BUCKET}/pyspark_rdd.py \
    -- ${INPUT_FILE} ${OUTPUT_DIR}; then
    echo "Échec de la soumission du job. Suppression du cluster et sortie."
    gcloud dataproc clusters delete ${CLUSTER_NAME} --region ${REGION} --quiet
    exit 1
fi

# Enregistrer l'heure de fin
end_time=$(date +%s)

# Calculer le temps d'exécution
execution_time=$((end_time - start_time))

# Créer un fichier d'information d'exécution
echo "Création du fichier d'information d'exécution..."
echo "Temps d'exécution : ${execution_time} secondes" > execution_info.txt
echo "Nombre de nœuds : ${NUM_WORKERS}" >> execution_info.txt

# Télécharger le fichier d'information d'exécution dans le bucket
gsutil cp execution_info.txt ${OUTPUT_DIR}/execution_info.txt

# Afficher les top 5 résultats de PageRank
echo "Top 5 pages par PageRank :"
gsutil cat ${OUTPUT_DIR}/part-* | sort -rnk2 | head -n 5

# Supprimer le cluster
echo "Suppression du cluster..."
gcloud dataproc clusters delete ${CLUSTER_NAME} --region ${REGION} --quiet

echo "Script terminé."