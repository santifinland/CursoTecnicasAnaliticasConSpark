# -*- coding: utf-8 -*-

from typing import List, Tuple

from pyspark.sql import DataFrame, Row, SparkSession


def main():

    print('Spark create DataFrame from collection')

    # Create Spark Session
    spark: SparkSession = SparkSession.builder.appName('Spark Course').getOrCreate()

    # Create a DataFrame from a collection of Tuples
    capitals: List[Tuple[str, str]] = [('Spain', 'Madrid'), ('Portugal', 'Lisbon')]
    parallelize_capitals: DataFrame = spark.createDataFrame(capitals, schema=['Country', 'Capital'])
    parallelize_capitals.show()

    # Create a DataFrame from a collection of Rows
    primes: List[int] = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]
    row_primes: List[Row] = list(map(lambda x: Row(x), primes))
    parallelize_primes: DataFrame = spark.createDataFrame(row_primes)
    parallelize_primes.show()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
