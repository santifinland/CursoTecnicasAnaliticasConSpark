# -*- coding: utf-8 -*-

import os

from pyspark.sql import SparkSession


def main():

    print(u"Exercise 5.1")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course").getOrCreate()

    # Read data
    path = os.path.join("data", "city")
    data = spark.read.l____(path)
    data.s____()
    print(data.c_____)
    data.pri____()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
