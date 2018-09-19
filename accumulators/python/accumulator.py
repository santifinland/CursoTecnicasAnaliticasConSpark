# -*- coding: utf-8 -*-

import logging
import os

from pyspark.sql import SparkSession

from common.logger_configuration import LoggerManager


# Get application logger
LoggerManager()
logger = logging.getLogger()
logger_spark = logging.getLogger('py4j')
logger_spark.setLevel(logging.INFO)


def main():
    logger.info(u"Accumulator basic usage")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course. Accumulator basic usage").getOrCreate()

    # Read csv
    path = os.path.join("..", "..", "data", "CatastroMadrid2014.csv")
    cadastre = spark.read.csv(path, header=True, inferSchema=True)

    # Compute number of districts with average land price about 100 euros / squared meter
    initial_value = 0
    districts_above_100 = spark.sparkContext.accumulator(initial_value)
    cadastre.show()
    cadastre.select('ValorMedio').foreach(lambda d: districts_above_100.add(1 if d[0] > 100.0 else 0))
    logger.info('Districts above 100 euros / squared meter: {}'.format(districts_above_100.value))


if __name__ == "__main__":
    try:
        main()
    except Exception, e:
        logger.error('Failed to execute process: {}'.format(e.message), exc_info=True)
