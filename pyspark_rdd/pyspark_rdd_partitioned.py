# from pyspark.sql import SparkSession
# from operator import add
# import re
# import hashlib
# import sys

# # Initialisation de la session Spark
# spark = SparkSession.builder.appName("PageRank").getOrCreate()

# def parse_neighbors(urls):
#     """Parses a URLs pair string into URLs pair."""
#     parts = re.split(r'\s+', urls)
#     return parts[0], parts[2]

# def compute_contribs(urls, rank):
#     """Calculates URL contributions to the rank of other URLs."""
#     num_urls = len(urls)
#     for url in urls:
#         yield (url, rank / num_urls)

# def hash_partition(url, num_partitions):
#     """Hash function for partitioning."""
#     return int(hashlib.md5(url.encode()).hexdigest(), 16) % num_partitions

# def initialize_links(input_file, num_partitions):
#     """Loads all URLs from input file and initializes their neighbors with partitioning."""
#     lines = spark.read.text(input_file).rdd.map(lambda r: r[0])
#     links = lines.map(lambda urls: parse_neighbors(urls)) \
#                  .distinct() \
#                  .partitionBy(num_partitions, lambda x: hash_partition(x[0], num_partitions)) \
#                  .groupByKey() \
#                  .cache()
#     return links

# def initialize_ranks(links):
#     """Initializes ranks for each URL to 1.0."""
#     return links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

# def run_pagerank(links, iterations, num_partitions):
#     """Runs the PageRank algorithm for a given number of iterations with partitioning."""
#     ranks = initialize_ranks(links)

#     for iteration in range(iterations):
#         contribs = links.join(ranks).flatMap(
#             lambda url_urls_rank: compute_contribs(url_urls_rank[1][0], url_urls_rank[1][1])
#         )
#         ranks = contribs.reduceByKey(add, numPartitions=num_partitions) \
#                         .mapValues(lambda rank: rank * 0.85 + 0.15)

#     return ranks

# def main(input_file, output_file):
#     # Définir le nombre de partitions
#     num_partitions = spark.sparkContext.defaultParallelism

#     # Initialisation des liens et des voisins avec partitionnement
#     links = initialize_links(input_file, num_partitions)

#     # Exécution de l'algorithme PageRank avec partitionnement
#     ranks = run_pagerank(links, iterations=10, num_partitions=num_partitions)

#     # Trier les pages par rank décroissant
#     sorted_ranks = ranks.sortBy(lambda x: -x[1])

#     # Récupérer les 5 premiers résultats
#     top_5 = sorted_ranks.take(5)

#     # Affichage des 5 premiers résultats
#     print("Top 5 PageRanks:")
#     for i, (link, rank) in enumerate(top_5, 1):
#         print(f"{i}. {link} has rank: {rank}")

#     # Récupérer l'entité avec le plus grand PageRank
#     max_rank = sorted_ranks.first()

#     # Affichage du résultat maximum
#     print("\nL'entité avec le plus grand PageRank est :")
#     print(f"{max_rank[0]} avec un score de : {max_rank[1]}")

#     # Sauvegarder les résultats
#     # sorted_ranks.saveAsTextFile(output_file)

# if __name__ == "__main__":
#     if len(sys.argv) != 3:
#         print("Usage: pagerank <input_file> <output_file>")
#         sys.exit(-1)

#     input_file = sys.argv[1]
#     output_file = sys.argv[2]
#     main(input_file, output_file)

# # N'oubliez pas d'arrêter la session Spark à la fin
# spark.stop()

from pyspark.sql import SparkSession
from pyspark.accumulators import AccumulatorParam
from operator import add
import re
import hashlib
import sys
import time  # Importer la bibliothèque time

# Création d'un accumulateur personnalisé pour suivre la progression
class ProgressAccumulator(AccumulatorParam):
    def zero(self, value):
        return 0
    def addInPlace(self, v1, v2):
        return v1 + v2

# Initialisation de la session Spark avec configuration du parallélisme
spark = SparkSession.builder \
    .appName("PageRank") \
    .config("spark.default.parallelism", "100") \
    .getOrCreate()

# Création de l'accumulateur
progress = spark.sparkContext.accumulator(0, ProgressAccumulator())

def parse_neighbors(urls):
    """Parses a URLs pair string into URLs pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[2]

def compute_contribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def hash_partition(url, num_partitions):
    """Hash function for partitioning."""
    return int(hashlib.md5(url.encode()).hexdigest(), 16) % num_partitions

def initialize_links(input_file, num_partitions):
    """Loads all URLs from input file and initializes their neighbors with partitioning."""
    lines = spark.read.text(input_file).rdd.map(lambda r: r[0])
    links = lines.map(lambda urls: parse_neighbors(urls)) \
                 .distinct() \
                 .partitionBy(num_partitions, lambda x: hash_partition(x[0], num_partitions)) \
                 .groupByKey() \
                 .cache()
    return links

def initialize_ranks(links):
    """Initializes ranks for each URL to 1.0."""
    return links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

def run_pagerank(links, iterations, num_partitions):
    """Runs the PageRank algorithm for a given number of iterations with partitioning."""
    ranks = initialize_ranks(links)

    for iteration in range(iterations):
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: compute_contribs(url_urls_rank[1][0], url_urls_rank[1][1])
        )
        ranks = contribs.reduceByKey(add, numPartitions=num_partitions) \
                        .mapValues(lambda rank: rank * 0.85 + 0.15)
        
        # Mise à jour de la progression
        progress.add(1)
        print(f"Itération {iteration + 1}/{iterations} terminée. Progression : {progress.value}/{iterations}")

    return ranks

def main(input_file, output_file):
    # Définir le nombre de partitions
    num_partitions = spark.sparkContext.defaultParallelism
    print(f"Nombre de partitions : {num_partitions}")

    # Démarrer le chronomètre
    start_time = time.time()

    # Initialisation des liens et des voisins avec partitionnement
    print("Initialisation des liens...")
    links = initialize_links(input_file, num_partitions)
    print("Liens initialisés.")

    # Exécution de l'algorithme PageRank avec partitionnement
    print("Démarrage de l'algorithme PageRank...")
    ranks = run_pagerank(links, iterations=10, num_partitions=num_partitions)
    print("Algorithme PageRank terminé.")

    # Arrêter le chronomètre
    end_time = time.time()

    # Calculer le temps d'exécution en secondes
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")

    print("Sorting results...")
    sorted_ranks = ranks.sortBy(lambda x: -x[1])

    print("Collecting top 5 results...")
    top_5 = sorted_ranks.take(5)

    print("\nTop 5 PageRanks:")
    for i, (link, rank) in enumerate(top_5, 1):
        print(f"{i}. {link} has rank: {rank:.6f}")

    print("\nFinding maximum rank...")
    max_rank = sorted_ranks.first()
    print(f"Page with highest PageRank: {max_rank[0]} (rank: {max_rank[1]:.6f})")

    print(f"\nSaving results to {output_file}...")
    sorted_ranks.saveAsTextFile(output_file)

    print(f"Total iterations completed: {progress.value}")


    # Récupération des 5 meilleurs résultats
    # print("Récupération des meilleurs résultats...")
    # top_5 = ranks.takeOrdered(5, key=lambda x: -x[1])

    # # Affichage des 5 meilleurs résultats
    # print("\nTop 5 PageRanks:")
    # for i, (link, rank) in enumerate(top_5, 1):
    #     print(f"{i}. {link} has rank: {rank}")

    # # Récupérer l'entité avec le plus grand PageRank
    # max_rank = top_5[0]

    # # Affichage du résultat maximum
    # print("\nL'entité avec le plus grand PageRank est :")
    # print(f"{max_rank[0]} avec un score de : {max_rank[1]}")

    # Sauvegarder tous les résultats
    # print(f"Sauvegarde des résultats dans {output_file}...")
    # ranks.saveAsTextFile(output_file)
    # print("Sauvegarde terminée.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: pagerank <input_file> <output_file>")
        sys.exit(-1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    main(input_file, output_file)

# N'oubliez pas d'arrêter la session Spark à la fin
spark.stop()