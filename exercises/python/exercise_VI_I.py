# -*- coding: utf-8 -*-

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *


def main():

    print(u"Exercise 6.1")

    # Create Spark Session
    spark: SparkSession = SparkSession.builder.appName('Spark Course').getOrCreate()

    # Define schema of the data
    schema: StructType = StructType([
        StructField('DISTRITO', StringType(), False),
        StructField('PRICE', DoubleType(), True)])

    # Read csv injecting schema
    catastro: DataFrame = spark.read.csv('data/CatastroMadrid2014.csv', header=True, schema=schema)
    catastro.show(200, truncate=False)

    # Compute max, min and 25 percentile
    summary: DataFrame = catastro._____('___', '___', '___')
    summary.show()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
