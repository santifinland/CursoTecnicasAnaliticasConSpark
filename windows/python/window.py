# -*- coding: utf-8 -*-

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import avg, rank


def main():

    print(u"Window Functions")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course. Window Functions").getOrCreate()

    # Read data
    cdr: DataFrame = spark.read.option('header', 'true').csv('data/call_cdr/year=1924/month=04/day=19')
    cdr.show()

    # Create Window Function
    w1 = Window.partitionBy("CALLER")
    duration_avg = avg("DURATION").over(w1)
    cdr.withColumn("DURATION_AVG", duration_avg).show()

    # Create another Window Function
    w2 = Window \
        .partitionBy("CALLER") \
        .orderBy("DURATION")
    rank_duration = rank().over(w2)
    cdr.withColumn("RANK_DURATION", rank_duration).show()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
