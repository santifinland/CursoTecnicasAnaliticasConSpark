# -*- coding: utf-8 -*-

from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def main():

    print('Spark read DataFrames with header')

    # Create Spark Session
    spark: SparkSession = SparkSession.builder.appName('Spark Course').getOrCreate()

    # Read data
    cdr: DataFrame = (spark
                      .read
                      .option('header', 'true')
                      .option('delimiter', ',')
                      .csv('data/call_cdr/year=1924/month=04/day=19'))

    # Print DataFrame properties
    columns: List[str] = cdr.columns
    print('Cdr DataFrame columns: {}'.format(columns))
    schema: StructType = cdr.schema
    print('Cdr DataFrame schema: {}'.format(schema))

    # Show data
    cdr.show()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
