# -*- coding: utf-8 -*-

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

from common.logger_configuration import logger


def main():
    logger.info(u"Spark Structured Streaming")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course. Spark Structured Streaming").getOrCreate()

    # Read text. Directory needed
    path = os.path.join("data", "streaming")
    pirata = spark.readStream.text(path)

    # Define Query: count frequency of words
    word_counts = (pirata.select(
        explode(split(pirata.value, ' '))
        .alias('word'))
        .groupBy("word")
        .count())

    # Start query
    query = (word_counts
             .writeStream
             .outputMode("complete")
             .format("console")
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
