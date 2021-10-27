# -*- coding: utf-8 -*-

import random

from pyspark.sql import SparkSession


def main():

    print('Broadcast variable basic usage')

    # Create Spark Session
    spark = SparkSession.builder.appName('Spark Course. Broadcast variable basic usage').getOrCreate()

    # Get security token. i.e. oAuth token
    token = authorize()

    # Broadcast token from the master to the nodes
    broadcasted_token = spark.sparkContext.broadcast(token)

    # Use broadcast token to access resource from the nodes
    res = access_resource(broadcasted_token.value)
    print(res)


def authorize():
    return random.randint(1, 10)


def access_resource(token):
    return 'Resource accessed using token {}'.format(token)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
