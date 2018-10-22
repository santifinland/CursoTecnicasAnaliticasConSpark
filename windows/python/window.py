# -*- coding: utf-8 -*-

import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import avg, col, rank
from pyspark.sql.types import FloatType

from common.logger_configuration import logger


def main():
    logger.info(u"Window Functions")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course. Window Functions").getOrCreate()

    # Load CDR data
    path = os.path.join(os.environ["HOME"], "data", "cdr")
    cdr = spark.read.parquet(path) \
        .select("TRAFFIC_EVENT_TYPE_ID", col("SESSION_DURATION_QT").cast(FloatType()))
    cdr.show()

    # Create Window Function
    w1 = Window.partitionBy("TRAFFIC_EVENT_TYPE_ID")
    duration_avg = avg("SESSION_DURATION_QT").over(w1)
    cdr.withColumn("duration_avg", duration_avg).show()

    # Create another Window Function
    w2 = Window \
        .partitionBy("TRAFFIC_EVENT_TYPE_ID") \
        .orderBy("SESSION_DURATION_QT")
    rank_duration = rank().over(w2)
    cdr.withColumn("rank_duration", rank_duration).show()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
