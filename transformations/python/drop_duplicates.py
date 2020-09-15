# -*- coding: utf-8 -*-

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, length
from pyspark.sql.types import *


def main():

    print('Spark DataFrame dropDuplicates')

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

    # Chain transformations
    transformed_cdr: DataFrame = (cdr
                                  .select('CALLER', 'INTERNATIONAL')
                                  .filter(col('INTERNATIONAL'))
                                  .withColumn('CALLER_NAME_LENGTH', length('CALLER')))
    transformed_cdr.show()

    # Drop duplicates for entire Rows
    unique_cdr: DataFrame = transformed_cdr.dropDuplicates()
    unique_cdr.show()

    # Drop duplicates for certain Columns
    unique_cdr_international: DataFrame = transformed_cdr.dropDuplicates(subset=['INTERNATIONAL'])
    unique_cdr_international.show()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
