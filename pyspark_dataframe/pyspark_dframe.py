from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def compute_pagerank(input_file, output_file, max_iterations=10, damping_factor=0.85):
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

    # Calculer le nombre de liens sortants pour chaque page
    outdegrees = data.groupBy("source").count().withColumnRenamed("source", "page").withColumnRenamed("count", "outDegree")

    # Initialisation du PageRank en attribuant à chaque page une valeur de départ de 1.0
    pagerank = outdegrees.withColumn("pagerank", col("outDegree").cast(DoubleType()) * 0 + 1.0)

    # Créer un DataFrame des liens
    links = data.select("source", "target").distinct()

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

    # Sauvegarder le DataFrame PageRank final dans un fichier
    pagerank.select("page", "pagerank").write.csv(output_file, header=True, mode="overwrite")

    # Afficher les 5 premières lignes pour vérification
    pagerank.select("page", "pagerank").orderBy(col("pagerank").desc()).show(5, truncate=False)

    # Arrêter la session Spark
    spark.stop()

# Exemple d'utilisation de la fonction
compute_pagerank("small_page_links.nt", "output_pagerank")
