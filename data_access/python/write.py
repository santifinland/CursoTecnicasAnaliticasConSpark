# -*- coding: utf-8 -*-

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *


def main():

    print('Write data')

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

    # Modify data
    callers: DataFrame = (cdr
                          .filter(col('DURATION') > 60)        # Filter rows (narrow transformation)
                          .select('CALLER', 'INTERNATIONAL'))  # Select columns (narrow transformation)

    # Write data
    (callers
     .write
     .csv('output', header=True, mode="overwrite"))


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
