# -*- coding: utf-8 -*-

from pyspark.sql import DataFrame, GroupedData, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *


def main():

    print('Spark DataFrame groupby')

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
    cdr.show()

    # Group calls by International flag
    international_groups: GroupedData = cdr.groupBy(col('INTERNATIONAL'))
    print(international_groups)

    # Aggregate mean price column in the International grouped calls
    international_prices: DataFrame = international_groups.mean('PRICE')
    international_prices.show()

    # Aggregate at the same time, mean price and mean duration in the International grouped calls
    international_duration_prices: DataFrame = international_groups.mean('DURATION', 'PRICE')
    international_duration_prices.show()

    # Group by CALLER and CALLED at the same time, and aggregate duration
    calls: DataFrame = cdr.groupBy([col('CALLER'), col('CALLED')]).max('DURATION')
    calls.show()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
