# -*- coding: utf-8 -*-

import os

from pyspark.sql import SparkSession

from common.logger_configuration import logger


def main():
    logger.info(u"Read csv files with header")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course. Csv files infer schema").getOrCreate()

    # Read csv infering schema
    path = os.path.join("data", "EleccionesMadrid2016.csv")
    elections = spark.read.csv(path, header=True, inferSchema=True)
    elections.show()
    elections.printSchema()
    elections.explain()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e.message), exc_info=True)
