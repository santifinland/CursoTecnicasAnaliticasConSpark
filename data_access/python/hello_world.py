# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession


def main():

    print('Spark Hello World')

    # Create Spark Session
    spark: SparkSession = SparkSession.builder.appName("Spark Course").getOrCreate()

    # Log Spark Session configuration
    print('Spark session version: {}'.format(spark.version))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
