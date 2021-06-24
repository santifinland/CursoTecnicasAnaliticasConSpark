# -*- coding: utf-8 -*-

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *


def main():

    print('Spark DataFrame filter')

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
    print("Initial number of records: {}".format(cdr.count()))

    # Filter international calls
    international_cdr: DataFrame = cdr.filter(col('INTERNATIONAL'))
    print("Found {} records".format(international_cdr.count()))
    international_cdr.show()

    # Filter long calls
    long_cdr: DataFrame = cdr.filter(cdr.DURATION > 60)
    print("Found {} records".format(long_cdr.count()))
    long_cdr.show()

    # Filter free calls
    free_cdr: DataFrame = cdr.filter('PRICE = 0.0')
    print("Found {} records".format(free_cdr.count()))
    free_cdr.show()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
