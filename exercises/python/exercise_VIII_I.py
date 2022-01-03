# -*- coding: utf-8 -*-

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *


def main():

    print("Exercise 8.1")

    # Create Spark Session
    spark: SparkSession = SparkSession.builder.appName('Spark Course').getOrCreate()

    # Read data injecting schema
    schema: StructType = StructType([ # Define schema of the data
        StructField('CALLER', StringType(), False),
        StructField('CALLED', StringType(), False),
        StructField('DAY_DT', DateType(), True),
        StructField('DURATION', IntegerType(), True),
        StructField('PRICE', DoubleType(), True),
        StructField('INTERNATIONAL', BooleanType(), True)])
    cdr: DataFrame = spark.read.csv('data/call_cdr', header=True, schema=schema)

    # Write data
    (cdr
     .write
     .____('output_path'))


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
