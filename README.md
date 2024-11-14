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

## Comparaison des Performances

*Les temps dexécution ont été mesurés en effectuant une moyenne sur plusieurs exécutions pour chaque configuration.*

| Nombre de nœuds | PySpark DataFrame sans partition | PySpark DataFrame avec partition | PySpark RDD sans partition | PySpark RDD avec partition |
|-----------------|----------------------------------|----------------------------------|---------------------------|---------------------------|
| 2 nœuds         | 48m 10s                         | 32m 26s                         | 45m 33s                   | -                         |
| 4 nœuds         | 32m 30s                         | 20m 8s                          | 24m 22s                   | -                         |

////// Graph to show results in histogram
### Analyse des Performances
// We have to comment the results of the pic
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


## Exécution du Code

### Implémentation DataFrame
```bash
# Sans partitionnement
./Script_pyspark_dframe.sh

# Avec partitionnement
./Script_pyspark_dframe_part.sh



