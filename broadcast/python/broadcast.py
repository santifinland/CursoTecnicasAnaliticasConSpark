# -*- coding: utf-8 -*-

import random

from pyspark.sql import SparkSession

from common.logger_configuration import logger


def main():
    logger.info(u"Broadcast variable basic usage")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course. Broadcast variable basic usage").getOrCreate()

    # Get security token. i.e. oAuth token
    token = authorize()

    # Broadcast token from the master to the nodes
    broadcasted_token = spark.sparkContext.broadcast(token)

    # Use broadcast token to access resource from the nodes
    res = accessResource(broadcasted_token.value)
    logger.info(res)


def authorize():
    return random.randint(1, 10)


def accessResource(token):
    return "Resource accessed using token {}".format(token)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e.message), exc_info=True)
