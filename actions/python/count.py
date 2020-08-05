# -*- coding: utf-8 -*-

from pyspark.sql import DataFrame, SparkSession


def main():

    print('Spark DataFrame count')

    # Create Spark Session
    spark: SparkSession = SparkSession.builder.appName('Spark Course').getOrCreate()

    # Read data
    cdr: DataFrame = spark.read.load(path='data/call_cdr/year=1924/month=04/day=19', format='csv')

    # Count data
    n: int = cdr.count()
    print('Cdr DataFrame size: {}'.format(n))


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
