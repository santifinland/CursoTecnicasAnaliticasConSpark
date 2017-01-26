# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging
from itertools import chain

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler

from common.LoadParties import LoadParties
from common.logger_configuration import LoggerManager


# Get application logger
LoggerManager()
logger = logging.getLogger()
logger_spark = logging.getLogger('py4j')
logger_spark.setLevel(logging.INFO)

parties_train_schema = StructType([StructField('Arganzuela', IntegerType(), True),
                                   StructField('Barajas', IntegerType(), True),
                                   StructField('Carabanchel', IntegerType(), True),
                                   StructField('Centro', IntegerType(), True),
                                   StructField('Chamberi', IntegerType(), True),
                                   StructField('CiudadLineal', IntegerType(), True),
                                   StructField('Fuencarral', IntegerType(), True),
                                   StructField('Latina', IntegerType(), True),
                                   StructField('Moncloa', IntegerType(), True),
                                   StructField('Moratalaz', IntegerType(), True),
                                   StructField('PuenteVallecas', IntegerType(), True),
                                   StructField('Salamanca', IntegerType(), True),
                                   StructField('SanBlas', IntegerType(), True),
                                   StructField('Tetuan', IntegerType(), True),
                                   StructField('Usera', IntegerType(), True),
                                   StructField('VillaVallecas', IntegerType(), True),
                                   StructField('Villaverde', IntegerType(), True)])


def train(sql_context):

    # Read Elections data and add binary column with PP winner or not by district
    parties_raw = LoadParties().train(sql_context)
    numeric_columns = parties_raw.columns[1:]
    parties = parties_raw.select(numeric_columns)
    parties.show()
    print(parties.schema)

    # Create Vector of features
    assembler = VectorAssembler(inputCols=parties.columns, outputCol="features")
    df = assembler.transform(parties)

    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures",
                            withStd=True, withMean=True)

    # Compute summary statistics by fitting the StandardScaler
    scalerModel = scaler.fit(df)

    # Normalize each feature to have unit standard deviation.
    scaledData = scalerModel.transform(df)
    scaledData.show()

    df = scaledData \
        .drop("features") \
        .withColumn("features", scaledData.scaledFeatures)
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
                           withStd=True, withMean=True)

    # Compute summary statistics by fitting the StandardScaler
    scalerModel = scaler.fit(df)

    # Normalize each feature to have unit standard deviation.
    scaledData = scalerModel.transform(df)
    scaledData.show()

    df = scaledData \
        .drop("features") \
        .withColumn("features", scaledData.scaledFeatures)
    df.show()

    predictions = model.transform(df)
    predictions.show()


def _sort_transpose_tuple(tup):
    x, y = tup
    return (x, zip(*sorted(y, key=lambda (v,k): k, reverse=False))[0])


def to_int(col):
    return int(col)


def transpose(X):
    """Transpose a PySpark DataFrame.

    Parameters
    ----------
    X : PySpark ``DataFrame``
        The ``DataFrame`` that should be tranposed.
    """
    # validate
    if not isinstance(X, DataFrame):
        raise TypeError('X should be a DataFrame, not a %s'
                        % type(X))

    cols = X.columns
    n_features = len(cols)

    # Sorry for this unreadability...
    return X.rdd.flatMap( # make into an RDD
        lambda xs: chain(xs)).zipWithIndex().groupBy( # zip index
        lambda (val,idx): idx % n_features).sortBy( # group by index % n_features as key
        lambda (grp,res): grp).map( # sort by index % n_features key
        lambda (grp,res): _sort_transpose_tuple((grp,res))).map( # maintain order
        lambda (key,col): col).toDF() # return to DF

if __name__ == "__main__":
    try:
        # Create Spark context
        conf = SparkConf().setMaster("local").setAppName("Esic")
        sc = SparkContext(conf=conf)
        sql_context = SQLContext(sc)

        logger.info(u"Técnicas analíticas con Spark y modelado predictivo")

        model = train(sql_context)
        predict(sql_context, model)

    except Exception, e:
        logger.error('Failed to execute process: {}'.format(e.message), exc_info=True)
