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
    logger.info(u"Read csv files")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course. Collect data").getOrCreate()

    # Read csv
    path = os.path.join("..", "..", "data", "EleccionesMadrid2016.csv")
    elections = spark.read.csv(path, header=True, inferSchema=True)

    # Show data
    elections.show(10, truncate=False)

    # Take data
    e1 = elections.take(5)
    print(type(e1))
    print(len(e1))
    for item in e1:
        print(item)

    # Collect all data
    e2 = elections.collect()
    print(type(e2))
    print(len(e2))
    for item in e2:
        print(item)


if __name__ == "__main__":
    try:
        main()
    except Exception, e:
        logger.error('Failed to execute process: {}'.format(e.message), exc_info=True)
