# -*- coding: utf-8 -*-

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *


def main():

    print('Spark DataFrame select')

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
    cdr: DataFrame = spark.read.csv('data/call_cdr/year=1924/month=04/day=19', header=True, schema=schema)

    # Select 3 columns of the data
    selection_one: DataFrame = cdr.select(['CALLED', 'DURATION', 'INTERNATIONAL'])
    selection_one.show()

    # Select a single column of the data
    selection_two: DataFrame = cdr.select(['CALLER'])
    selection_two.show()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
