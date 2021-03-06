# -*- coding: utf-8 -*-

import os

from pyspark.sql import SparkSession

from common.logger_configuration import logger


def main():
    logger.info(u"DataFrame transformations")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course. Compute stats").getOrCreate()

    # Read csv
    path = os.path.join("data", "EleccionesMadrid2016.csv")
    elections = spark.read.csv(path, header=True, inferSchema=True)

    # Compute number of rows
    elections.count()
    elections.show()

    # Compute max of a selected column
    elections.describe().show()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
