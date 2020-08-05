# -*- coding: utf-8 -*-

from pyspark.sql import DataFrame, SparkSession


def main():

    print('Spark read DataFrames glob')

    # Create Spark Session
    spark: SparkSession = SparkSession.builder.appName('Spark Course').getOrCreate()

    # Read data
    path: str = 'data/call_cdr/year=*/month=04/day=??'
    cdr: DataFrame = spark.read.load(path=path, format='csv', header=True, inferSchema=True)

    # Show data and schema
    cdr.show()
    cdr.printSchema()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
