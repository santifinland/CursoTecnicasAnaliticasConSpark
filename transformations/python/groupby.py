# -*- coding: utf-8 -*-

import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from common.logger_configuration import logger


def main():
    logger.info(u"DataFrame Transformations")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course. Group by").getOrCreate()

    # Read csv
    path = os.path.join(os.environ["HOME"], "data", "weblogs")
    weblogs = spark.read.csv(path, header=True, inferSchema=True)

    # Compute total number of http answers per typ
    answer_types = weblogs \
        .groupBy("co_estado_http") \
        .count() \
        .collect()
    for answer_type in answer_types:
        logger.info("{}".format(answer_type))
#
    # Compute avg and std of both data uploaded and downloaded per user
    up_down_stats = weblogs \
        .groupBy("in_https") \
        .agg(f.avg("ca_vol_up"), f.stddev("ca_vol_up"), f.avg("ca_vol_dw"), f.stddev("ca_vol_dw")) \
        .collect()
    for stat in up_down_stats:
        logger.info("HTTPS: {}, Up mean: {}, Down mean: {}, Up std: {}, Down std: {}"
                    .format(stat[0], stat[1], stat[2], stat[3], stat[4]))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
