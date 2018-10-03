# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

from common.logger_configuration import logger


def main():
    logger.info(u"RDD from collections")

    # Create Spark Session and
    spark = SparkSession.builder.appName("Spark Course. RDD from collections").getOrCreate()

    # Create an RDD from a collection
    primes = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]
    parallelized_primes = spark.sparkContext.parallelize(primes)

    # Get number of partitions
    prime_partitions = parallelized_primes.getNumPartitions()
    logger.info("Parallelized primes partitions: {}".format(prime_partitions))

    # Create an RDD setting the num of partitions
    capitals = [("Spain", "Madrid"), ("Portugal", "Lisbon")]
    parallelized_capitals = spark.sparkContext.parallelize(capitals, 5)
    logger.info(parallelized_capitals.collect())
    capitals_partitions = parallelized_capitals.getNumPartitions()
    logger.info("Parallelized capitals partitions: {}".format(capitals_partitions))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
