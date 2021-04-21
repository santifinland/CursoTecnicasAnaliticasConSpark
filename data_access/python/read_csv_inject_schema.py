# -*- coding: utf-8 -*-

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *


def main():

    print('Spark read DataFrames inject schema')

    # Create Spark Session
    spark: SparkSession = SparkSession.builder.appName('Spark Course').getOrCreate()

    # Define schema of the data
    schema: StructType = StructType([
        StructField('CALLER', StringType(), nullable=False),
        StructField('CALLED', StringType(), nullable=False),
        StructField('DATE', DateType(), nullable=True),
        StructField('DURATION', IntegerType(), nullable=True),
        StructField('PRICE', DoubleType(), nullable=True),
        StructField('INTERNATIONAL', BooleanType(), nullable=True)])

    # Read csv injecting schema
    cdr: DataFrame = spark.read.csv('data/call_cdr/year=1924/month=04/day=19', header=True, schema=schema)
    cdr.show()
    cdr.printSchema()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
