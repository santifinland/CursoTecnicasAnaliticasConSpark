# -*- coding: utf-8 -*-

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *


def main():

    print(u"Exercise 7.1")

    # Create Spark Session
    spark: SparkSession = SparkSession.builder.appName('Spark Course').getOrCreate()

    # Define schema of the data
    cdr_schema: StructType = StructType([
        StructField('CALLER', StringType(), nullable=False),
        StructField('CALLED', StringType(), nullable=False),
        StructField('DATE', DateType(), nullable=True),
        StructField('DURATION', IntegerType(), nullable=True),
        StructField('PRICE', DoubleType(), nullable=True),
        StructField('INTERNATIONAL', BooleanType(), nullable=True)])

    # Read csv injecting schema
    cdr: DataFrame = spark.read.csv('data/call_cdr/year=1924/month=04/day=19', header=True, schema=cdr_schema)
    cdr.show()

    # Read parquet city data
    city: DataFrame = spark.read.load('data/city')
    city.show()

    # a) How many Calls are issued from Madrid?
    res_a: DataFrame = (cdr
                        .join(city, on=______)
                        ._________
                        .count())
    res_a.show()

    # b) How many people are issuing calls from Barcelona?
    res_b: DataFrame = (cdr
                        .select(______)
                        .distinct()
                        .join(city, on=____________________)
                        .groupBy(_____)
                        .count())
    res_b.show()

    # c) How many people are issuing calls from Avila?
    res_c: DataFrame = _____
    res_c.show()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
