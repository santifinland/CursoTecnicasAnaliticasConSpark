# -*- coding: utf-8 -*-

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *


def main():

    print('Spark DataFrame union')

    # Create Spark Session
    spark: SparkSession = SparkSession.builder.appName('Spark Course').getOrCreate()

    # Define schema of the data
    schema: StructType = StructType([
        StructField('CALLER', StringType(), False),
        StructField('CALLED', StringType(), False),
        StructField('DATE', DateType(), True),
        StructField('DURATION', IntegerType(), True),
        StructField('PRICE', DoubleType(), True),
        StructField('INTERNATIONAL', BooleanType(), True)])

    # Read csv injecting schema
    cdr_19: DataFrame = spark.read.csv('data/call_cdr/year=1924/month=04/day=19', header=True, schema=schema)
    cdr_20: DataFrame = spark.read.csv('data/call_cdr/year=1924/month=04/day=20', header=True, schema=schema)

    # Select CALLER column
    caller: DataFrame = cdr_19.select(['CALLER'])
    caller.show()

    # Select CALLED column
    called: DataFrame = cdr_19.select(['CALLED'])
    called.show()

    # Union CALLER and CALLED columns
    people: DataFrame = caller.union(called)
    people.show()

    # Union by name two DataFrames
    cdr: DataFrame = cdr_19.select(['DURATION', 'PRICE']).unionByName(cdr_20.select(['PRICE', 'DURATION']))
    cdr.show()

    # Union two DataFrames
    wrong_cdr: DataFrame = cdr_19.select(['PRICE', 'DURATION']).union(cdr_20.select(['DURATION', 'PRICE']))
    wrong_cdr.filter((col('DURATION') > 0) & (col('DURATION') < 1)).show()
    cdr.filter((col('DURATION') > 0) & (col('DURATION') < 1)).show()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
