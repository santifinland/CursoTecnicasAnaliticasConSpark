# -*- coding: utf-8 -*-
# REGEX patterns for web logs

import logging
import os

from pyspark.sql import SparkSession


from common.logger_configuration import LoggerManager

# Get application logger
LoggerManager()
logger = logging.getLogger()
logger_spark = logging.getLogger('py4j')
logger_spark.setLevel(logging.INFO)


def main():
    logger.info(u"Read films catalogue")

    # Create Spark Session
    spark = SparkSession.builder.appName("Edu").getOrCreate()

    base_dir = os.path.join('../data/peliculas')
    input_path = os.path.join('Catalogo_peliculas.csv')
    films_file = os.path.join(base_dir, input_path)

    films = spark.read.csv(films_file)
    print 'Films example %s' % films.show(5)

    films = spark.read.option("header", "true").csv(films_file)
    print 'Films example %s' % films.show(5)

    films.createOrReplaceTempView("films")
    spark.sql("select * from films").show()

    films.select("CONTENT_KIND").distinct().show()
    spark.sql("select distinct(CONTENT_KIND) from films").show()


if __name__ == "__main__":
    try:
        main()
    except Exception, e:
        logger.error('Failed to execute process: {}'.format(e.message), exc_info=True)
