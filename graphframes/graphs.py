# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from graphframes.graphframe import GraphFrame
from common.logger_configuration import logger


def main():
    logger.info(u"Read csv files")

    spark = SparkSession.builder.appName("Spark Course").getOrCreate()

    # Vertex DataFrame
    v = spark.createDataFrame([
        ("a", "Alice", 34),
        ("b", "Bob", 36),
        ("c", "Charlie", 30),
        ("d", "David", 29),
        ("e", "Esther", 32),
        ("f", "Fanny", 36),
        ("g", "Gabby", 60)
    ], ["id", "name", "age"])
    # Edge DataFrame
    e = spark.createDataFrame([
        ("a", "b", "friend"),
        ("b", "c", "follow"),
        ("c", "b", "follow"),
        ("f", "c", "follow"),
        ("e", "f", "follow"),
        ("e", "d", "friend"),
        ("d", "a", "friend"),
        ("a", "e", "friend")
    ], ["src", "dst", "relationship"])
    # Create a GraphFrame
    g = GraphFrame(v, e)
    g.vertices.show()
    g.edges.show()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
