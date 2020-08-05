# -*- coding: utf-8 -*-

from typing import List, Tuple

from pyspark.sql import DataFrame, Row, SparkSession


def main():

    print('Spark create DataFrame from collection')

    # Create Spark Session
    spark: SparkSession = SparkSession.builder.appName('Spark Course').getOrCreate()

    # Create a DataFrame from a collection
    primes: List[Row] = [Row(2), Row(3), Row(5), Row(7), Row(11), Row(13), Row(17), Row(19), Row(23), Row(29)]
    parallelize_primes: DataFrame = spark.createDataFrame(primes)
    parallelize_primes.show()

    # Get number of partitions
    prime_partitions = parallelize_primes.rdd.getNumPartitions()
    print('Parallelize primes partitions: {}'.format(prime_partitions))

    # Create an RDD setting the num of partitions
    capitals: List[Tuple[str, str]] = [('Spain', 'Madrid'), ('Portugal', 'Lisbon')]
    parallelize_capitals: DataFrame = spark.createDataFrame(capitals, ['Country', 'Capital'])
    parallelize_capitals.show()
    capitals_partitions = parallelize_capitals.rdd.getNumPartitions()
    print('Parallelize capitals partitions: {}'.format(capitals_partitions))


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
