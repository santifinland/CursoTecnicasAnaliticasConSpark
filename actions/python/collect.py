# -*- coding: utf-8 -*-

from typing import List

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import *


def main():

    print('Spark DataFrame collect')

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
    distributed_cdr: DataFrame = spark.read.csv('data/call_cdr/year=1924/month=04/day=19', header=True, schema=schema)

    # Collect all data
    cdr: List[Row] = distributed_cdr.collect()
    print('List of CDR Rows: {}'.format(cdr))
    print('A CDR: {}'.format(cdr[0]))
    print('A CDR caller: {}'.format(cdr[0]['CALLER']))


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
