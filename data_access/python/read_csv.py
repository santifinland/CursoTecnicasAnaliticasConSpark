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
    spark = SparkSession.builder.appName("Spark Course").getOrCreate()

    # Read csv
    path = os.path.join("..", "..", "data", "EleccionesMadrid2016.csv")
    elections = spark.read.csv(path)
    elections.show()
    elections.printSchema()


if __name__ == "__main__":
    try:
        main()
    except Exception, e:
        logger.error('Failed to execute process: {}'.format(e.message), exc_info=True)