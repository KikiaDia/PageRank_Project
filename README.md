# PageRank Project  (2024-2025)
## Auteur :  `Kikia Dia | Baye Lahad Mbacke`

## Détails d'Implémentation
Nous avons implémenté PageRank en utilisant deux approches différentes :
1. PySpark DataFrame
2. PySpark RDD

Chaque implémentation a été testée avec et sans partitionnement d'URL (comme référencé dans la présentation NSDI 2012).

## Configuration Expérimentale
- Source des données : `gs:///public_lddm_data/`
- Configurations du cluster :
  - 1 noeud 
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
ici on teste avec le fichier:*pyspark_rdd_partitioned.py*

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
| 1 nœuds         | 2h 14m                           | 1h 56m                         |1h 18 m                 | 1h 10 m                      |
| 2 nœuds         | 48m 10s                         | 32m 26s                         | 45m 33s                   | 43 min 26 s                        |
| 4 nœuds         | 32m 30s                         | 20m 8s                          | 24m 22s                   | 30 min 37s                        |

![performance_comparison_all_nodes](https://github.com/user-attachments/assets/f7706f76-5723-4fcd-9ebb-52576423937f)

# Analyse des Performances

Le graphique ci-dessus compare les performances de différentes configurations de données en fonction du nombre de nœuds et du partitionnement. Voici les points clés :

## 1. Impact du Partitionnement

- **Avec partition** : Les configurations avec partition (DataFrame et RDD) sont plus performantes que celles sans partition, surtout avec un nombre de nœuds élevé.
- **Sans partition** : Les durées sont globalement plus longues, car le manque de partitionnement limite la répartition efficace des données.

## 2. Comparaison entre DataFrames et RDDs

- **RDDs** : Pour 1 ou 2 nœuds, les RDDs (en particulier avec partition) sont compétitifs et parfois plus rapides que les DataFrames sans partition.
- **DataFrames avec partition** : À 4 nœuds, cette configuration offre la meilleure performance (20.13 minutes), surpassant les RDDs.

## 3. Scalabilité avec le Nombre de Nœuds

- **Réduction de la durée** : La durée d'exécution diminue avec l'augmentation du nombre de nœuds pour toutes les configurations, mais les configurations avec partition profitent davantage de cette scalabilité.
- **Limite des RDDs** : La configuration "RDD avec partition" est moins performante que "DataFrame avec partition" à 4 nœuds (30.62 minutes vs. 20.13 minutes), suggérant une scalabilité inférieure pour les RDDs dans ce cas.

1. **Impact du Partitionnement (DataFrame)**
   - Sur 1 nœud : amélioration de 13.4% (de 2h14m  à 1h56m )
   - Sur 2 nœuds : amélioration de 32.7% (de 48m 10s à 32m 26s)
   - Sur 4 nœuds : amélioration de 38.1% (de 32m 30s à 20m 8s)

3. **Impact du Nombre de Nœuds**
   - DataFrame sans partition : amélioration de 32.5% (de 48m 10s à 32m 30s)
   - DataFrame avec partition : amélioration de 37.9% (de 32m 26s à 20m 8s)
   - RDD sans partition : amélioration de 46.5% (de 45m 33s à 24m 22s)

4. **Comparaison DataFrame vs RDD**
   - Sur 2 nœuds sans partition : RDD plus rapide de 5.4%
   - Sur 4 nœuds sans partition : RDD plus rapide de 25%

3. **Efficacité** :
   - Les deux implémentations ont montré de meilleures performances avec 4 nœuds
   - Les versions partitionnées ont systématiquement surpassé les versions non partitionnées sur le PySpark DataFrame

## Top 1 des Entités par PageRank

### Implémentation DataFrame
1. Living_people (31001.24)

### Implémentation RDD
1. Living_people (38525.85)




