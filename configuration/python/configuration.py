# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession


def main():

    print('Spark configuration')

    # Create Spark Session
    spark: SparkSession = (SparkSession
                           .builder
                           .config(key='spark.dynamicAllocation.enabled', value='false')
                           .config(key='spark.eventLog.enabled', value='true')
                           .config(key='spark.eventLog.dir', value='/tmp')
                           .appName('Spark Course')
                           .getOrCreate())

    # Log Spark configuration
    configuration = spark.sparkContext.getConf().getAll()
    for c in configuration:
        print(c)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
