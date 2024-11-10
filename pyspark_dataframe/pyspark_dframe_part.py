from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_col, concat, lit, regexp_extract, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import sys

def compute_pagerank_partition(input_file, output_file, max_iterations=10, damping_factor=0.85):
    # Create a Spark session
    spark = SparkSession.builder.appName("PageRankPartitioned").getOrCreate()

    # Define the schema of the data
    schema = StructType([
        StructField("source", StringType(), nullable=True),
        StructField("predicate", StringType(), nullable=True),
        StructField("target", StringType(), nullable=True)
    ])

    # Load the data directly from the input file
    data = spark.read.csv(input_file, header=False, schema=schema, sep=" ")

    # Extract domain and protocol from URLs
    data = data.withColumn("domain", regexp_extract(col("source"), "^(?:https?:\/\/)?(?:www\.)?([^\/]+)", 1))
    data = data.withColumn("protocol", regexp_extract(col("source"), "^(https?):\/\/", 1))

    # Calculate the number of outgoing links for each page
    outdegrees = data.groupBy("source").count().withColumnRenamed("source", "page").withColumnRenamed("count", "outDegree")

    # Initialize PageRank by assigning each page a starting value of 1.0
    pagerank = outdegrees.withColumn("pagerank", lit(1.0))

    # Create a DataFrame of links
    links = data.select("source", "target").distinct()

    # PageRank iterations
    for i in range(max_iterations):
        # Calculate PageRank contributions
        contributions = links.join(pagerank, links.source == pagerank.page) \
            .withColumn("contribution", col("pagerank") / col("outDegree"))

        # Aggregate contributions by target page
        aggregated = contributions.groupBy("target") \
            .agg(sum_col("contribution").alias("sum_contributions"))

        # Calculate new PageRank
        new_pagerank = aggregated.withColumnRenamed("target", "page") \
            .withColumn("pagerank", 
                lit(1 - damping_factor) + damping_factor * when(col("sum_contributions").isNotNull(), col("sum_contributions")).otherwise(0))

        # Update PageRank
        pagerank = new_pagerank.join(outdegrees, "page", "outer").na.fill(0)

    # Add domain and protocol information to final DataFrame
    pagerank_with_url_info = pagerank.join(data, pagerank.page == data.source, "left_outer") \
        .select(pagerank.page, pagerank.pagerank, data.domain, data.protocol) \
        .distinct()

    # Save the final PageRank DataFrame to a file, partitioned by domain and protocol
    pagerank_with_url_info.write \
        .partitionBy("domain", "protocol") \
        .mode("overwrite") \
        .csv(output_file)

    # Display the top 5 rows for verification
    print("\nTop 5 pages by PageRank:")
    pagerank_with_url_info.select("page", "pagerank") \
        .orderBy(col("pagerank").desc()) \
        .show(5, truncate=False)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: pyspark_dframe_part.py <input_file> <output_file> [max_iterations] [damping_factor]")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    max_iterations = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    damping_factor = float(sys.argv[4]) if len(sys.argv) > 4 else 0.85

    compute_pagerank_partition(input_file, output_file, max_iterations, damping_factor)