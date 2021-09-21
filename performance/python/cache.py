# -*- coding: utf-8 -*-

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *


def main():

    print('Spark cache')

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

    # Cache data to be used in different Spark jobs
    cdr.cache()

    # First Spark job
    long_cdr: DataFrame = cdr.filter(col('DURATION') > 60)  # Filter cdrs (narrow transformation: task)
    callers: DataFrame = long_cdr.select('CALLER').distinct()  # Compute distinct callers (wide transformation: stage)
    num_distinct_long_callers: int = callers.count()  # Count distinct long duration callers (action: job end)
    print('Long duration unique callers: {}'.format(num_distinct_long_callers))

    # Second Spark job
    prices: DataFrame = cdr.groupBy(col('INTERNATIONAL')).mean('PRICE')  # Aggregate data (wide transformation: stage)
    filtered_prices: DataFrame = prices.filter(col('INTERNATIONAL') == False)  # Filter (narrow transformation: task)
    national_price: int = filtered_prices.collect()[0]['avg(PRICE)']  # Collect national price (action: job end)
    print('Precio medio de las llamadas nacionales: {}'.format(national_price))


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
