# PageRank Project Implementation (2024-2025)
## Auteur :  `Kikia Dia | Baye Lahad Mbacke`

## Détails d'Implémentation
Nous avons implémenté PageRank en utilisant deux approches différentes :
1. PySpark DataFrame
2. PySpark RDD

Chaque implémentation a été testée avec et sans partitionnement d'URL (comme référencé dans la présentation NSDI 2012).

## Configuration Expérimentale
- Source des données : `gs:///public_lddm_data/`
- Configurations du cluster : 
  - 2 nœuds
  - 4 nœuds
  - Mêmes spécifications matérielles (CPU/RAM) maintenues sur tous les nœuds pour des résultats comparables
## Structure du Projet
```plaintext
PageRank_Project/
├── pyspark_dataframe/
│   ├── pyspark_dframe_part.py            # Implémentation DataFrame avec partitionnement
│   ├── pyspark_dframe.py                 # Implémentation DataFrame sans partitionnement
│   ├── Script_pyspark_dframe_part.sh     # Script exécution DataFrame avec partitionnement
│   └── Script_pyspark_dframe.sh          # Script exécution DataFrame sans partitionnement
├── pyspark_rdd/
│   ├── pyspark_rdd_partitioned.py        # Implémentation RDD avec partitionnement
│   ├── pyspark_rdd.py                    # Implémentation RDD sans partitionnement
│   ├── script_rdd_part.sh                # Script exécution RDD avec partitionnement
│   └── script_rdd.sh                     # Script exécution RDD sans partitionnement
└── README.md
```
## 1. Création d'un cluster Dataproc
```
gcloud dataproc clusters create pagerank-cluster-1 \
    --enable-component-gateway \
    --region europe-west1 \
    --zone europe-west1-c \
    --master-machine-type n1-standard-4 \
    --master-boot-disk-size 500 \
    --num-workers 4 \
    --worker-machine-type n1-standard-4 \
    --worker-boot-disk-size 500 \
    --image-version 2.0-debian10 \
    --project projectpagerank
```
## 2. Lancer une tâche PySpark sur le cluster
```
gcloud dataproc jobs submit pyspark \
    --region europe-west1 \
    --cluster pagerank-cluster-1 \
    --files gs://bucket_projet_pagerank_lahad_kikia/pyspark_rdd_partitioned.py \
    gs://bucket_projet_pagerank_lahad_kikia/pyspark_rdd_partitioned.py \
    -- gs://bucket_projet_pagerank_lahad_kikia/page_links_en.nt.bz2 \
    gs://bucket_projet_pagerank_lahad_kikia/out/pagerank_data_1
```
## 3. Suppression du cluster
```
gcloud dataproc clusters delete pagerank-cluster-1 --region europe-west1 --quiet
```
## Comparaison des Performances

*Les temps dexécution ont été mesurés en effectuant une moyenne sur plusieurs exécutions pour chaque configuration.*

| Nombre de nœuds | PySpark DataFrame sans partition | PySpark DataFrame avec partition | PySpark RDD sans partition | PySpark RDD avec partition |
|-----------------|----------------------------------|----------------------------------|---------------------------|---------------------------|
| 2 nœuds         | 48m 10s                         | 32m 26s                         | 45m 33s                   | -                         |
| 4 nœuds         | 32m 30s                         | 20m 8s                          | 24m 22s                   | -                         |
/// Graph pour plot les res sur histogram
### Analyse des Performances
//// Comment les res
1. **Impact du Partitionnement (DataFrame)**
   - Sur 2 nœuds : amélioration de 32.7% (de 48m 10s à 32m 26s)
   - Sur 4 nœuds : amélioration de 38.1% (de 32m 30s à 20m 8s)

2. **Impact du Nombre de Nœuds**
   - DataFrame sans partition : amélioration de 32.5% (de 48m 10s à 32m 30s)
   - DataFrame avec partition : amélioration de 37.9% (de 32m 26s à 20m 8s)
   - RDD sans partition : amélioration de 46.5% (de 45m 33s à 24m 22s)

3. **Comparaison DataFrame vs RDD**
   - Sur 2 nœuds sans partition : RDD plus rapide de 5.4%
   - Sur 4 nœuds sans partition : RDD plus rapide de 25%

3. **Efficacité** :
   - Les deux implémentations ont montré de meilleures performances avec 4 nœuds
   - Les versions partitionnées ont systématiquement surpassé les versions non partitionnées

## Top 1 des Entités par PageRank

### Implémentation DataFrame
1. Living_people (31001.24)

### Implémentation RDD
1. Living_people (38525.85)
