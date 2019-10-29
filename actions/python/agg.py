# -*- coding: utf-8 -*-

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import max as pyspark_max

from common.logger_configuration import logger


def main():
    logger.info(u"DataFrame Transformations")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course. Compute stats").getOrCreate()

    # Read csv
    path = os.path.join("data", "EleccionesMadrid2016.csv")
    elections = spark.read.csv(path, header=True, inferSchema=True)

    # Compute number of rows
    elections.count()
    elections.show()

    # Compute max of a selected column
    max_agg = elections.agg(pyspark_max(elections.PP))
    max_agg.show()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
