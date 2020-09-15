# -*- coding: utf-8 -*-

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *


def main():

    print('Spark DataFrame summary')

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

    # Compute mean and 80 percentile
    summary: DataFrame = cdr.summary(['mean', '80%'])
    summary.show()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
