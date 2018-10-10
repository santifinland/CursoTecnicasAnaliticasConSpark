# -*- coding: utf-8 -*-

import os

from pyspark.sql import SparkSession

from common.logger_configuration import logger


def main():
    logger.info(u"Read csv files with header")

    # Create Spark Session
    spark = SparkSession \
        .builder \
        .appName("Spark Course. Csv files with header") \
        .config("spark.eventLog.enabled", "true") \
        .getOrCreate()

    # Read csv
    path = os.path.join("data", "EleccionesMadrid2016.csv")
    elections = spark.read.option("header", "true").csv(path)
    elections.show()
    elections.printSchema()

    # Actions in DataFrames
    elections.describe(elections.columns).show()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
