# -*- coding: utf-8 -*-

from datetime import date
from random import random
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *


def main():

    print('Build cdr')

    # Create Spark Session
    spark: SparkSession = SparkSession.builder.appName('Spark Course').getOrCreate()

    # Different caller and called people
    people: List[str] = ['ada', 'ana', 'ben', 'bob', 'eli', 'eva', 'fox', 'gio', 'jim', 'joe', 'ken', 'kia', 'leo',
                         'luz', 'mac', 'mar', 'max', 'noa', 'pau', 'pol', 'rey', 'riu', 'sam', 'teo', 'tim', 'tom',
                         'van', 'zoe']

    # Build n rows with random data
    rows = []
    today: date = date(1924, 4, 20)
    for i in range(10000):
        r = [person(people), person(people), today, int(random() * 100), random() * 10, random() > 0.8]
        rows.append(r)

    # Parallelize data and store in disk
    schema: StructType = StructType([ # Define schema of the data
        StructField('CALLER', StringType(), False),
        StructField('CALLED', StringType(), False),
        StructField('DAY_DT', DateType(), True),
        StructField('DURATION', IntegerType(), True),
        StructField('PRICE', DoubleType(), True),
        StructField('INTERNATIONAL', BooleanType(), True)])

    # First Spark job
    cdr: DataFrame = spark.createDataFrame(rows, schema=schema)
    cdr.write.csv('data/call_cdr/year=1924/month=04/day=20', header=True)


def person(people: List[str]) -> str:
    return people[int(len(people) * random())]


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
