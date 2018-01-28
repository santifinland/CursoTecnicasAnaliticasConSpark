# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from LoadCatastro import LoadCatastro
from LoadElections import LoadElections
from logger_configuration import LoggerManager


# Get application logger
LoggerManager()
logger = logging.getLogger()
logger_spark = logging.getLogger('py4j')
logger_spark.setLevel(logging.INFO)


def train(spark):

    # Read Elections data and add binary column with PP winner or not by district
    elections_raw = LoadElections().train(spark)
    getmax = udf(maxvar)
    numeric_columns = elections_raw.columns[1:]
    elections_max = elections_raw \
        .withColumn("max", getmax(*[elections_raw[x] for x in numeric_columns]))
    elections =  elections_max \
        .withColumn("label", when(elections_max.max == elections_max.PP, 1).otherwise(0)) \
        .select("Distrito", "label")

    # Read Catastro data
    catastro = LoadCatastro().train(spark).select(
        expr("Distrito"),
        expr("ValorMedio"))

    # Join Elections and Catastro data
    df = elections.join(catastro, on="Distrito")
    df.show()

    # Create Vector of features
    assembler = VectorAssembler(inputCols=["ValorMedio"], outputCol="features")
    assembled_df = assembler.transform(df)

    # Create model
    lr = LogisticRegression(maxIter=100)
    model = lr.fit(assembled_df)
    logger.info(u"Elecciones. Regresión PP-Catastro: {}".format(model.intercept))
    logger.info(u"Elecciones. Regresión PP-Catastro: {}".format(model.coefficients))

    return model


def predict(spark, model):
    logger.info(u"Predicciones a partir de modelo de regresión logística")

    # Read Elections data and add binary column with PP winner or not by district
    elections_raw = LoadElections().test(spark)
    getmax = udf(maxvar)
    numeric_columns = elections_raw.columns[1:]
    elections_max = elections_raw \
        .withColumn("max", getmax(*[elections_raw[x] for x in numeric_columns]))
    elections =  elections_max \
        .withColumn("label", when(elections_max.max == elections_max.PP, 1).otherwise(0)) \
        .select("Distrito", "label")
    elections.show()

    catastro = LoadCatastro().test(spark)
    assembler = VectorAssembler(inputCols=["ValorMedio"], outputCol="features")
    catastro_df = assembler.transform(catastro)
    predictions = model.transform(catastro_df)
    predictions.show()


def maxvar(*cols):
    return reduce(lambda a, b: a if a > b else b, cols)


if __name__ == "__main__":
    try:

        logger.info(u"Técnicas analíticas con Spark y modelado predictivo")

        # Create Spark session
        spark = SparkSession.builder.appName("Educ").getOrCreate()

        model = train(spark)
        predict(spark, model)

    except Exception, e:
        logger.error('Failed to execute process: {}'.format(e.message), exc_info=True)
