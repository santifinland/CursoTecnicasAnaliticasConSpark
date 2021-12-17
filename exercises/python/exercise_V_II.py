# -*- coding: utf-8 -*-

from pyspark.sql import Row, SparkSession


def main():

    print(u"Exercise 5.2")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course").getOrCreate()

    # Create data
    prices_raw = [
        Row(19.3),
        _________
        _________
        _________
    ]
    prices = spark.createDataFrame(pric_____, ["P_____"])
    prices.show()
    prices.pri____()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
