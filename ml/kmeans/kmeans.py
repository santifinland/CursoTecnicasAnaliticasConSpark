# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler
from pyspark.sql import SparkSession

from common.LoadParties import LoadParties
from common.logger_configuration import LoggerManager


# Get application logger
LoggerManager()
logger = logging.getLogger()
logger_spark = logging.getLogger('py4j')
logger_spark.setLevel(logging.INFO)


def train(sql_context):

    # Read parties data
    parties_raw = LoadParties().train(sql_context)
    numeric_columns = parties_raw.columns[1:]
    parties = parties_raw.select(numeric_columns)
    parties.show()
    print(parties.schema)

    # Create Vector of features
    assembler = VectorAssembler(inputCols=parties.columns, outputCol="features")
    df = assembler.transform(parties)

    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures",
                            withStd=True, withMean=False)

    # Compute summary statistics by fitting the StandardScaler
    scaler_model = scaler.fit(df)

    # Normalize each feature to have unit standard deviation.
    scaled_data = scaler_model.transform(df)
    scaled_data.show()

    df = scaled_data \
        .drop("features") \
        .withColumn("features", scaled_data.scaledFeatures)
    df.show()

    # Trains a k-means model.
    kmeans = KMeans().setK(3)
    model = kmeans.fit(df)

    # Evaluate clustering by computing Within Set Sum of Squared Errors.
    wssse = model.computeCost(df)
    logger.info("Within Set Sum of Squared Errors = " + str(wssse))

    # Shows the result.
    centers = model.clusterCenters()
    logger.info("Cluster Centers: ")
    for center in centers:
        logger.info(center)

    predictions = model.transform(df)
    predictions.show()
    return model


def predict(sql_context, model):
    logger.info(u"Predicciones a partir de modelo de regresión logística")

    # Read Elections data and add binary column with PP winner or not by district
    parties = LoadParties().test(sql_context)
    numeric_columns = parties.columns[1:]

    # Create Vector of features
    assembler = VectorAssembler(inputCols=numeric_columns, outputCol="features")
    df = assembler.transform(parties)

    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures",
                            withStd=True, withMean=False)

    # Compute summary statistics by fitting the StandardScaler
    scaler_model = scaler.fit(df)

    # Normalize each feature to have unit standard deviation.
    scaled_data = scaler_model.transform(df)
    scaled_data.show()

    df = scaled_data \
        .drop("features") \
        .withColumn("features", scaled_data.scaledFeatures)
    df.show()

    predictions = model.transform(df)
    predictions.show()


if __name__ == "__main__":
    try:

        logger.info(u"Técnicas analíticas con Spark y modelado predictivo")

        # Create Spark Session
        spark = SparkSession.builder.appName("Edu").getOrCreate()

        model_trained = train(spark)
        predict(spark, model_trained)

    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
