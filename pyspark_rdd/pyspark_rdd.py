# from pyspark.sql import SparkSession
# from operator import add
# import re
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

# def initialize_links(input_file):
#     """Loads all URLs from input file and initializes their neighbors."""
#     lines = spark.read.text(input_file).rdd.map(lambda r: r[0])
#     links = lines.map(lambda urls: parse_neighbors(urls)).distinct().groupByKey().cache()
#     return links

# def initialize_ranks(links):
#     """Initializes ranks for each URL to 1.0."""
#     return links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

# def run_pagerank(links, iterations):
#     """Runs the PageRank algorithm for a given number of iterations."""
#     ranks = initialize_ranks(links)

#     for iteration in range(iterations):
#         contribs = links.join(ranks).flatMap(
#             lambda url_urls_rank: compute_contribs(url_urls_rank[1][0], url_urls_rank[1][1])
#         )
#         ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

#     return ranks

# def main(input_file, output_file):
#     # Initialisation des liens et des voisins
#     links = initialize_links(input_file)

#     # Exécution de l'algorithme PageRank
#     ranks = run_pagerank(links, iterations=10)

#     # Trier les pages par rank décroissant
#     sorted_ranks = ranks.sortBy(lambda x: -x[1])

#     # Récupérer les 5 premiers résultats
#     top_5 = sorted_ranks.take(5)

#     # Affichage des 5 premiers résultats
#     print("Top 5 PageRanks:")
#     for i, (link, rank) in enumerate(top_5, 1):
#         print(f"{i}. {link} has rank: {rank}")

#     # Trouver le maximum
#     max_rank = sorted_ranks.first()

#     # Affichage du maximum
#     print("\nPage avec le plus grand PageRank:")
#     print(f"{max_rank[0]} has the highest rank: {max_rank[1]}")

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
from operator import add
import re
import sys
from pyspark import AccumulatorParam
import time  # Importer la bibliothèque time

# Initialisation de la session Spark avec configuration optimisée
spark = SparkSession.builder \
    .appName("PageRank") \
    .config("spark.default.parallelism", "100") \
    .getOrCreate()

# Création d'un accumulateur personnalisé pour suivre la progression
class ProgressAccumulator(AccumulatorParam):
    def zero(self, value):
        return 0
    def addInPlace(self, v1, v2):
        return v1 + v2

progress = spark.sparkContext.accumulator(0, ProgressAccumulator())

def parse_neighbors(urls):
    parts = re.split(r'\s+', urls)
    return parts[0], parts[2]

def compute_contribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def initialize_links(input_file):
    lines = spark.read.text(input_file).rdd.map(lambda r: r[0])
    links = lines.map(lambda urls: parse_neighbors(urls)).distinct().groupByKey().cache()
    return links

def initialize_ranks(links):
    return links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

def run_pagerank(links, iterations):
    ranks = initialize_ranks(links)

    for iteration in range(iterations):
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: compute_contribs(url_urls_rank[1][0], url_urls_rank[1][1])
        )
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
        
        # Forcer l'évaluation et mettre à jour l'accumulateur
        count = ranks.count()
        progress.add(1)
        print(f"Iteration {iteration + 1} completed. Processed {count} records.")

    return ranks

def main(input_file, output_file):

    # Démarrer le chronomètre
    start_time = time.time()

    print("Initializing links...")
    links = initialize_links(input_file)
    
    print("Running PageRank algorithm...")
    ranks = run_pagerank(links, iterations=10)

    # Arrêter le chronomètre
    end_time = time.time()

    # Calculer le temps d'exécution en secondes
    execution_time = end_time - start_time
    print(f"PageRank Execution time: {execution_time:.2f} seconds")

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

    # Arrêter le chronomètre
    end_time = time.time()

    # Calculer le temps d'exécution en secondes
    execution_time = end_time - start_time
    print(f"Total Execution time: {execution_time:.2f} seconds")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: pagerank <input_file> <output_file>")
        sys.exit(-1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    try:
        main(input_file, output_file)
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        print("Stopping Spark session...")
        spark.stop()

print("Job completed.")