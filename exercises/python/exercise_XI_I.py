# -*- coding: utf-8 -*-

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *


def main():

    print("Exercise 11.1")

    # Create Spark Session
    spark: SparkSession = SparkSession.builder.appName('Spark Course').getOrCreate()

    # Create a Dataframe from a collection
    words = [Row("madrid"), Row("valencia"), Row("murcia")]
    parallelized_words = spark.createDataFrame(words, ["WORD"])
    parallelized_words.show()

    # Register a user defined function to capitalize first letter of the word
    capitalize_udf = ____(lambda x: ________, ______Type())

    # Add new column applying a user defined function
    capitalized_words = parallelized_words.________("CAP_WORD", capitalize_udf("WORD"))
    capitalized_words.show()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
