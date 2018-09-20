# -*- coding: utf-8 -*-

import os

from pyspark.sql import SparkSession

from common.logger_configuration import logger


def main():
    logger.info(u"Accumulators combined usage")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course. Accumulators combined usage").getOrCreate()

    # Read csv
    path = os.path.join("data", "CatastroMadrid2014.csv")
    cadastre = spark.read.csv(path, header=True, inferSchema=True)

    # Compute number of districts with average land price about 100 and 150 euros / squared meter
    initial_value = 0
    districts_above_100 = spark.sparkContext.accumulator(initial_value)
    districts_above_150 = spark.sparkContext.accumulator(initial_value)
    condition_above_100 = lambda d: True if d > 100.0 else False
    condition_above_150 = lambda d: True if d > 150.0 else False
    conditions = {condition_above_100: districts_above_100, condition_above_150: districts_above_150}
    cadastre.select('ValorMedio').foreach(lambda d: acc(d[0], conditions))
    logger.info('Districts above 100 euros / squared meter: {}'.format(districts_above_100.value))
    logger.info('Districts above 100 euros / squared meter: {}'.format(districts_above_150.value))


def acc(x, condition):
    for k, v in condition.iteritems():
        if k(x):
            v.add(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e.message), exc_info=True)
