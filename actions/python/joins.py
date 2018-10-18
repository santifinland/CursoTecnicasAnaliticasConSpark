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
    cdr = spark.read.parquet(path).dropDuplicates(["PHONE_1_ID"])
    logger.info("Cdr columns: {}".format(cdr.columns))
    logger.info("Cdr records: {}".format(cdr.count()))
    cdr.select("PHONE_1_ID").sort("PHONE_1_ID").show()

    # Read weblogs data
    path = os.path.join(os.environ["HOME"], "data", "weblogs")
    weblogs = spark.read.csv(path, header=True, inferSchema=True).dropDuplicates(["nu_telefono"])
    logger.info("Weblogs columns: {}".format(weblogs.columns))
    logger.info("Weblogs records: {}".format(weblogs.count()))
    weblogs.select("nu_telefono").sort("nu_telefono").show()

    # Join cdr and weblogs data
    joined_data = cdr.join(weblogs, cdr.PHONE_1_ID == weblogs.nu_telefono)
    logger.info("Join columns: {}".format(joined_data.columns))
    logger.info("Join records: {}".format(joined_data.count()))
    joined_data.show(3)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
