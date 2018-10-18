# -*- coding: utf-8 -*-

import os

from pyspark.sql import SparkSession

from common.logger_configuration import logger


def main():
    logger.info(u"Spark joins")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course. Spark joins").getOrCreate()

    # Read cdr data
    path = os.path.join(os.environ["HOME"], "data", "cdr")
    cdr = spark.read.parquet(path) \
        .select("PHONE_1_ID", "SESSION_DURATION_QT") \
        .dropDuplicates(["PHONE_1_ID"])
    logger.info("Cdr partitions: {}".format(cdr.rdd.getNumPartitions()))
    logger.info("Cdr count: {}".format(cdr.count()))

    # Read weblogs data
    path = os.path.join(os.environ["HOME"], "data", "weblogs")
    weblogs = spark.read.csv(path, header=True, inferSchema=True) \
        .select("nu_telefono", "ca_vol_dw") \
        .dropDuplicates(["nu_telefono"])
    logger.info("Weblogs partitions: {}".format(weblogs.rdd.getNumPartitions()))
    logger.info("Weblogs count: {}".format(weblogs.count()))

    # Join cdr and weblogs data
    joined_data = cdr.join(weblogs, cdr.PHONE_1_ID == weblogs.nu_telefono)
    logger.info("Join partitions: {}".format(joined_data.rdd.getNumPartitions()))
    logger.info("Join count: {}".format(joined_data.count()))
    joined_data.show()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
