# -*- coding: utf-8 -*-

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


def main():

    print('User Defined Functions')

    # Create Spark Session
    spark = SparkSession.builder.appName('Spark Course').getOrCreate()

    # Create a Dataframe from a collection
    primes = [Row(2), Row(3), Row(5), Row(7), Row(11), Row(13), Row(17), Row(19), Row(23), Row(29)]
    parallelized_primes = spark.createDataFrame(primes, ["prime"])
    parallelized_primes.show()

    # Register a user defined function
    square_udf = udf(lambda x: x * x, IntegerType())

    # Add new column applying a user defined function
    doubled_primes = parallelized_primes.withColumn("squared", square_udf("prime"))
    doubled_primes.show()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
