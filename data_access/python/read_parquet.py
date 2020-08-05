# -*- coding: utf-8 -*-

from pyspark.sql import DataFrame, SparkSession


def main():

    print('Spark read DataFrames infer schema')

    # Create Spark Session
    spark: SparkSession = SparkSession.builder.appName('Spark Course').getOrCreate()

    # Read data
    cdr_a: DataFrame = spark.read.format('parquet').load('data/cdr')
    cdr_b: DataFrame = spark.read.parquet('data/cdr')
    cdr_a.show()
    cdr_b.show()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
