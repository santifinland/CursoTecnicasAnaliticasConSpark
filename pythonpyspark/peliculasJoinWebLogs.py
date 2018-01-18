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

    base_dir = os.path.join('../data')
    input_films = os.path.join('peliculas')
    input_weblogs = os.path.join('web_logs.csv')
    films_file = os.path.join(base_dir, input_films)
    weblogs_file = os.path.join(base_dir, input_weblogs)

    films = spark.read.option("header", "true").csv(films_file)
    print 'Films example %s' % films.show(5)

    web_logs = spark.read.option("header", "true").csv(weblogs_file)
    print 'Web logs example %s' % web_logs.show(5)

    films.join(web_logs, on="EXTERNAL_ASSET_ID", how="inner").show()


if __name__ == "__main__":
    try:
        main()
    except Exception, e:
        logger.error('Failed to execute process: {}'.format(e.message), exc_info=True)
