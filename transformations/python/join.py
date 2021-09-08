# -*- coding: utf-8 -*-

from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *


def main():

    print('Spark DataFrame join')

    # Create Spark Session
    spark: SparkSession = SparkSession.builder.appName('Spark Course').getOrCreate()

    # Define schema of the data
    cdr_schema: StructType = StructType([
        StructField('CALLER', StringType(), False),
        StructField('CALLED', StringType(), False),
        StructField('DATE', DateType(), True),
        StructField('DURATION', IntegerType(), True),
        StructField('PRICE', DoubleType(), True),
        StructField('INTERNATIONAL', BooleanType(), True)])

    # Read csv injecting schema
    cdr: DataFrame = spark.read.csv('data/call_cdr/year=1924/month=04/day=19', header=True, schema=cdr_schema)
    cdr.show()

    # Create DataFrame with CALLERs information
    people_schema: StructType = StructType([
        StructField('NAME', StringType(), False),
        StructField('CITY', StringType(), False),
        StructField('AGE', IntegerType(), False)])
    people_data: List[(str, str, int)] = [
        ('ana', 'Madrid', 23),
        ('pep', 'Toledo', 48),
        ('tom', 'Madrid', 36)]
    people: DataFrame = spark.createDataFrame(people_data, schema=people_schema)
    people.show()

    # Join CDR and people information
    caller: DataFrame = cdr.join(people, on=cdr['CALLER'] == people['NAME'], how='inner')
    caller.show()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
