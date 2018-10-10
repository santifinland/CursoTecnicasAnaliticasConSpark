# -*- coding: utf-8 -*-

import os

from pyspark import RDD, Row
from pyspark.rdd import PipelinedRDD
from pyspark.sql import SparkSession

from common.logger_configuration import logger


def main():
    logger.info(u"Read text files")

    # Create Spark Session and
    spark = SparkSession.builder.appName("Spark Course. Read text files").getOrCreate()

    # Read text
    path = os.path.join("data", "pirata.txt")
    pirata: RDD = spark.read.text(path).rdd
    lines: PipelinedRDD = pirata.map(lambda r: r[0])
    logger.info("Cancion del pirata: {}".format(lines.take(6)))

    # Count lines
    count = lines.count()
    logger.info("Líneas de la canción del pirata: {}".format(count))

    # Count word frequency
    counts = (lines
              .flatMap(lambda line: line.split(" "))
              .map(lambda word: (word, 1))
              .reduceByKey(lambda a, b: a + b)
              .sortBy(lambda r: -r[1]))

    logger.info("Frecuencia de palabras: {}".format(counts.collect()))

    # Map and filter data
    #pp_over_100 = elections_rdd.map(lambda Row(distrito, pp): pp).filter(lambda pp: pp > 100)
    #pp_over_100.take(30)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
