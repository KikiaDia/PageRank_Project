from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_col, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import sys

def compute_pagerank_partion(input_file, output_file, max_iterations=10, damping_factor=0.85):
    # Créez une session Spark
    spark = SparkSession.builder.appName("PageRankExample").getOrCreate()

    # Définir le schéma des données
    schema = StructType([
        StructField("source", StringType(), nullable=True),
        StructField("predicate", StringType(), nullable=True),
        StructField("target", StringType(), nullable=True)
    ])

    # Charger les données à partir du fichier
    data = spark.read.option("delimiter", " ").csv(input_file, header=False, schema=schema)

    # Extraire le domaine et le protocole des URLs
    data = data.withColumn("domain", regexp_extract(col("source"), "^(?:https?:\/\/)?(?:www\.)?([^\/]+)", 1))
    data = data.withColumn("protocol", regexp_extract(col("source"), "^(https?):\/\/", 1))

    # Calculer le nombre de liens sortants pour chaque page
    outdegrees = data.groupBy("source").count().withColumnRenamed("source", "page").withColumnRenamed("count", "outDegree")

    # Initialisation du PageRank en attribuant à chaque page une valeur de départ de 1.0
    pagerank = outdegrees.withColumn("pagerank", col("outDegree").cast(DoubleType()) * 0 + 1.0)

    # Créer un DataFrame des liens
    links = data.select("source", "target", "domain", "protocol").distinct()

    # Itérations de PageRank
    for i in range(max_iterations):
        # Calculer les contributions de PageRank
        contributions = links.join(pagerank, links.source == pagerank.page) \
            .withColumn("contribution", col("pagerank") / col("outDegree"))

        # Agréger les contributions par page cible
        aggregated = contributions.groupBy("target") \
            .agg(sum_col("contribution").alias("sum_contributions"))

        # Calculer le nouveau PageRank
        new_pagerank = aggregated.withColumnRenamed("target", "page") \
            .withColumn("pagerank", (1 - damping_factor) + damping_factor * col("sum_contributions"))

        # Mettre à jour le PageRank
        pagerank = new_pagerank.join(outdegrees, "page", "outer").na.fill(0)

    # Ajouter les colonnes domain et protocol au DataFrame final
    pagerank_with_url_info = pagerank.join(links, pagerank.page == links.source, "left_outer") \
        .select(pagerank.page, pagerank.pagerank, links.domain, links.protocol)

    # Sauvegarder le DataFrame PageRank final dans un fichier, partitionné par domaine et protocole
    pagerank_with_url_info.write.partitionBy("domain", "protocol") \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(output_file)

    # Afficher les 5 premières lignes pour vérification
    pagerank_with_url_info.select("page", "pagerank", "domain", "protocol") \
        .orderBy(col("pagerank").desc()) \
        .show(5, truncate=False)

    # Arrêter la session Spark
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: pyspark_dframe.py <input_file> <output_file> [max_iterations] [damping_factor]")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    max_iterations = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    damping_factor = float(sys.argv[4]) if len(sys.argv) > 4 else 0.85

    compute_pagerank_partion(input_file, output_file, max_iterations, damping_factor)
