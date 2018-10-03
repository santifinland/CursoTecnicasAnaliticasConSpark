# -*- coding: utf-8 -*-

from pyspark.sql import Row, SparkSession

from common.logger_configuration import logger


def main():
    logger.info(u"Dataframe from collections")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course. Dataframe from collections").getOrCreate()

    # Create a Dataframe from a collection
    primes = [Row(2), Row(3), Row(5), Row(7), Row(11), Row(13), Row(17), Row(19), Row(23), Row(29)]
    parallelized_primes = spark.createDataFrame(primes)
    parallelized_primes.show()

    # Get number of partitions
    prime_partitions = parallelized_primes.rdd.getNumPartitions()
    logger.info("Parallelized primes partitions: {}".format(prime_partitions))

    # Create an RDD setting the num of partitions
    capitals = [("Spain", "Madrid"), ("Portugal", "Lisbon")]
    parallelized_capitals = spark.createDataFrame(capitals, ["Country", "Capital"])
    parallelized_capitals.show()
    capitals_partitions = parallelized_capitals.rdd.getNumPartitions()
    logger.info("Parallelized capitals partitions: {}".format(capitals_partitions))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
