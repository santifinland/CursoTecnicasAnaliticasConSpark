# -*- coding: utf-8 -*-

from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def dict_udf(d, default_value=None):
    """Select value within a dictionary

    :param d:              dictionary with allowed keys and corresponding values
    :param default_value:  default value to be returned if given key is not found in the given dictionary
    :return: Udf with the function dict_udf that will select the value of the corresponding key within a dictionary
    """
    return udf(lambda k: d.get(k, default_value), StringType())


def main():

    print('User Defined Functions')

    # Create Spark Session
    spark = SparkSession.builder.appName('Spark Course').getOrCreate()

    # Create a Dataframe from a collection
    parent: DataFrame = spark.createDataFrame([["foo"], ["bar"]], ["origin"])
    parent.show()

    # Build udf and use it to create a new column whose values are dependant on the original dataframe an provided udf
    my_dictionary: Dict = {"foo": "bar", "bar": "foo"}
    child: DataFrame = parent.withColumn("udf_from_dict", dict_udf(my_dictionary, default_value="NA")("origin"))
    child.show()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print('Failed to execute process: {}'.format(e))
