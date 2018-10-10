# -*- coding: utf-8 -*-

import os

from pathlib import Path
from pyspark.sql import SparkSession

from common.logger_configuration import logger


def main():
    logger.info(u"Reduce action")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course. Reduce action").getOrCreate()

    # Read weblogs
    path = os.path.join(str(Path.home()), "data", "weblogs")
    weblogs = spark.read.csv(path, header=True, inferSchema=True)

    # Compute total "ca_vol_up"
    total_ca_vol_up = weblogs \
        .select("ca_vol_up") \
        .rdd \
        .map(lambda r: r[0]) \
        .reduce(lambda a, b: a + b)
    logger.info("Total ca_vol_up: {}".format(total_ca_vol_up))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
