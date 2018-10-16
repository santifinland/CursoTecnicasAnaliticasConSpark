# -*- coding: utf-8 -*-

import os

from pyspark.sql import SparkSession

from common.logger_configuration import logger


def main():
    logger.info(u"Read text files")

    # Create Spark Session and
    spark = SparkSession.builder.appName("Spark Course. Read text files").getOrCreate()

    # Read text
    path = os.path.join("data", "pirata.txt")
    pirata = spark.read.text(path).rdd
    lines = pirata.map(lambda r: r[0])
    logger.info("Cancion del pirata: {}".format(lines.take(6)))

    # Count lines
    count = lines.count()
    logger.info("Líneas de la canción del pirata: {}".format(count))

    # Collect lines
    collected_lines = lines.collect()
    logger.info("Líneas de la canción del pirata: {}".format(len(collected_lines)))
    logger.info("Primera línea de la canción del pirata: {}".format(collected_lines[0]))

    # Take some lines
    taken_lines = lines.take(4)
    logger.info("Primera estrofa canción del pirata:")
    for line in taken_lines:
        logger.info("  {}".format(line))

    # Map and filter data
    filtered_lines = lines.filter(lambda l: "ñ" in l.lower())
    mapped_filtered_lines = filtered_lines.map(lambda l: l.upper())
    logger.info("Líneas transformadas de la canción del pirata:")
    for line in mapped_filtered_lines.collect():
        logger.info("  {}".format(line))

    # Count word frequency
    counts = (lines
              .flatMap(lambda line: line.split(" "))
              .map(lambda word: (word, 1))
              .reduceByKey(lambda a, b: a + b)
              .sortBy(lambda r: -r[1]))

    logger.info("Frecuencia de palabras: {}".format(counts.collect()))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
