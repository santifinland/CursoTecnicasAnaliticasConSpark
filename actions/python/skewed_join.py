# -*- coding: utf-8 -*-

from pyspark.sql import Row, SparkSession

from common.logger_configuration import logger


def main():
    logger.info(u"Spark joins")

    parallelism = 10
    shuffle_partitions = 5
    coalesce_partitions = 3
    coalesce_higher_partitions = 8
    repartition_partitions = 3

    # Create Spark Session
    spark = SparkSession \
        .builder \
        .config("spark.default.parallelism", parallelism) \
        .config("spark.sql.shuffle.partitions", shuffle_partitions) \
        .appName("Spark Course. Spark skewed joins").getOrCreate()

    # Build city data
    print("\nCity data:")
    print("------------------------------")
    city_data = [
        Row("ana", "Madrid"),
        Row("bob", "Madrid"),
        Row("pep", "Toledo"),
        Row("tip", "Madrid"),
        Row("jon", "Madrid"),
        Row("tom", "Barcelona")
       ]
    city_df = spark.createDataFrame(city_data, ["name", "city"])
    print("Partitions: {}".format(city_df.rdd.getNumPartitions()))

    # Build age data
    print("\nAir Quality data:")
    print("------------------------------")
    air_quality_data = [
        Row("Madrid", "bad"),
        Row("Toledo", "good"),
        Row("Barcelona", "bad")
    ]
    air_quality_df = spark.createDataFrame(air_quality_data, ["city", "air"])
    print("Partitions: {}".format(air_quality_df.rdd.getNumPartitions()))

    # Skewed join
    print("\nJoin:")
    print("------------------------------")
    skewed = city_df.join(air_quality_df, on="city")
    print("Partitions: {}".format(skewed.rdd.getNumPartitions()))
    print(skewed.rdd.glom().map(len).collect())

    # Coalesce
    print("\nCoalesce:")
    print("------------------------------")
    coalesced = skewed.coalesce(coalesce_partitions)
    print("Partitions: {}".format(coalesced.rdd.getNumPartitions()))
    print(coalesced.rdd.glom().map(len).collect())

    # Repartition
    print("\nRepartition:")
    print("------------------------------\n")
    repartitioned = skewed.repartition(repartition_partitions)
    print("Partitions: {}".format(repartitioned.rdd.getNumPartitions()))
    print(repartitioned.rdd.glom().map(len).collect())

    # Coalesce higher
    print("\nCoalesce Higher:")
    print("------------------------------")
    coalesced_higher = skewed.coalesce(coalesce_higher_partitions)
    print("Partitions: {}".format(coalesced_higher.rdd.getNumPartitions()))
    print(coalesced_higher.rdd.glom().map(len).collect())


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
