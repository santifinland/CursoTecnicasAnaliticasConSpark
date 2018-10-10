# -*- coding: utf-8 -*-

import os

from pyspark import RDD, Row
from pyspark.sql import SparkSession

from common.logger_configuration import logger


def main():
    logger.info(u"RDD to Dataframe")

    # Create Spark Session and
    spark = SparkSession.builder.appName("Spark Course. RDD to Dataframe").getOrCreate()

    # Read text using spark context and convert the rdd to Dataframe
    path = os.path.join("data", "pirata.txt")
    pirata: RDD = spark.read.text(path).rdd
    pirata_df = pirata.map(lambda r: Row(r)).toDF()
    pirata_df.show()

    # Create an RDD from a collection and transform in Dataframe
    primes = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]
    parallelized_primes = spark.sparkContext.parallelize(primes)
    parallelized_primes_df = parallelized_primes.map(lambda r: Row(r)).toDF()
    parallelized_primes_df.show()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
