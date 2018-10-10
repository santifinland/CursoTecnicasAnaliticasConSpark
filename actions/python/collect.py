# -*- coding: utf-8 -*-

import os

from pyspark.sql import SparkSession

from common.logger_configuration import logger


def main():
    logger.info(u"Read csv files")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course. Collect data").getOrCreate()

    # Read csv
    path = os.path.join("data", "EleccionesMadrid2016.csv")
    elections = spark.read.csv(path, header=True, inferSchema=True)

    # Show data
    elections.show(10, truncate=False)

    # Take data
    e1 = elections.take(5)
    logger.info("Tipo de dato obtenido con take: {}".format(type(e1)))
    logger.info("Daatos recogidos: {}".format(len(e1)))
    for item in e1:
        logger.info(item)

    # Collect all data
    e2 = elections.collect()
    logger.info("Tipo de dato obtenido con collect: {}".format(type(e2)))
    logger.info("Daatos recogidos: {}".format(len(e2)))
    for item in e2:
        logger.info(item)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
