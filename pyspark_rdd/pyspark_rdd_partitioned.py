from pyspark.sql import SparkSession
from operator import add
import re
import hashlib
import sys

# Initialisation de la session Spark
spark = SparkSession.builder.appName("PageRank").getOrCreate()

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

    return ranks

def main(input_file, output_file):
    # Définir le nombre de partitions
    num_partitions = spark.sparkContext.defaultParallelism

    # Initialisation des liens et des voisins avec partitionnement
    links = initialize_links(input_file, num_partitions)

    # Exécution de l'algorithme PageRank avec partitionnement
    ranks = run_pagerank(links, iterations=10, num_partitions=num_partitions)

    # Trier les pages par rank décroissant
    sorted_ranks = ranks.sortBy(lambda x: -x[1])

    # Récupérer les 20 premiers résultats
    top_20 = sorted_ranks.take(20)

    # Affichage des 20 premiers résultats
    print("Top 20 PageRanks:")
    for i, (link, rank) in enumerate(top_20, 1):
        print(f"{i}. {link} has rank: {rank}")

    # Récupérer l'entité avec le plus grand PageRank
    max_rank = sorted_ranks.first()

    # Affichage du résultat maximum
    print("\nL'entité avec le plus grand PageRank est :")
    print(f"{max_rank[0]} avec un score de : {max_rank[1]}")

    # Sauvegarder les résultats
    sorted_ranks.saveAsTextFile(output_file)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: pagerank <input_file> <output_file>")
        sys.exit(-1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    main(input_file, output_file)

# N'oubliez pas d'arrêter la session Spark à la fin
spark.stop()