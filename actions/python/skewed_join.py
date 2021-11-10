# -*- coding: utf-8 -*-

from pyspark.sql import Row, SparkSession


def main():

    print(u"Spark skewed joins")

    parallelism = 6
    shuffle_partitions = 10
    coalesce_partitions = 3
    coalesce_higher_partitions = 8
    repartition_partitions = 3

    # Create Spark Session
    spark = SparkSession \
        .builder \
        .config("spark.default.parallelism", parallelism) \
        .config("spark.sql.shuffle.partitions", shuffle_partitions) \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .appName("Spark Course. Spark skewed joins").getOrCreate()

    # Build city data
    print("\nCity data:")
    print("------------------------------")
    city_data = [
        Row("ana", "Madrid"),
        Row("bob", "Madrid"),
        Row("pep", "Toledo"),
        Row("tip", "Madrid"),
        Row("fox", "Madrid"),
        Row("lee", "Avila"),
        Row("noe", "Madrid"),
        Row("ivy", "Madrid"),
        Row("ben", "Barcelona"),
        Row("eli", "Madrid"),
        Row("jon", "Madrid"),
        Row("tom", "Barcelona")
       ]
    city_df = spark.createDataFrame(city_data, ["NAME", "CITY"])
    city_df.show()
    print("Partitions: {}".format(city_df.rdd.getNumPartitions()))
    print(city_df.rdd.glom().map(len).collect())

    # Build air data
    print("\nAir Quality data:")
    print("------------------------------")
    air_quality_data = [
        Row("Madrid", "bad"),
        Row("Toledo", "good"),
        Row("Avila", "good"),
        Row("Barcelona", "bad")
    ]
    air_quality_df = spark.createDataFrame(air_quality_data, ["CITY", "AIR"])
    air_quality_df.show()
    print("Partitions: {}".format(air_quality_df.rdd.getNumPartitions()))
    print(air_quality_df.rdd.glom().map(len).collect())

    # Skewed join
    print("\nJoin:")
    print("------------------------------")
    skewed = city_df.join(air_quality_df, on="CITY")
    skewed.show()
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
        print('Failed to execute process: {}'.format(e))
