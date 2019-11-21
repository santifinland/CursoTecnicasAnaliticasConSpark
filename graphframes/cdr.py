# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from graphframes.graphframe import GraphFrame
from common.logger_configuration import logger


def main():
    logger.info(u"Read csv files")

    spark = SparkSession.builder.appName("Spark Course").getOrCreate()

    # Load CDR
    cdr = spark.read.load("data/cdr")
    cdr.show()

    # Vertex DataFrame
    v_out = cdr.select(col("PHONE_1_ID").alias("id"))
    v_in = cdr.select(col("PHONE_2_ID").alias("id"))
    vertex = v_out.union(v_in)
    vertex.show()

    # Edge DataFrame
    edges = cdr.select(
        col("PHONE_1_ID").alias("src"),
        col("PHONE_2_ID").alias("dst"))
    # Create a GraphFrame
    g = GraphFrame(vertex, edges)

    # Extract graph info
    g.inDegrees.show()
    g.outDegrees.show()
    page_rank = g.pageRank(resetProbability=0.15, maxIter=1)
    page_rank.edges.show()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
