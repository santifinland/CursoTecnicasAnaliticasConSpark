# -*- coding: utf-8 -*-

import os

from pyspark.sql import SparkSession

from common.logger_configuration import logger


def main():
    logger.info(u"Exercise")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course").getOrCreate()

    # Read csv
    path = os.path.join("data", "income.csv")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
