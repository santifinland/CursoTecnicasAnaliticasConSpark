# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging
import functools

from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from common.LoadElections import LoadElections
from common.LoadCatastro import LoadCatastro
from common.logger_configuration import LoggerManager


# Get application logger
LoggerManager()
logger = logging.getLogger()
logger_spark = logging.getLogger('py4j')
logger_spark.setLevel(logging.INFO)


def train(spark):

    logger.info(u"Linear regression")

    # Read Elections data and add PP percentage of votes by district
    elections_raw = LoadElections().train(spark)
    add = udf(sumvar)
    numeric_columns = elections_raw.columns[1:]
    elections_total = elections_raw \
        .withColumn("total", add(*[elections_raw[x] for x in numeric_columns]))
    elections = elections_total \
        .withColumn("label", elections_total.PP / elections_total.total) \
        .select("Distrito", "label")
    elections.show()

    # Read Catastro data
    catastro = LoadCatastro().train(spark).select(
        expr("Distrito"),
        expr("ValorMedio"))

    # Join Elections and Catastro data
    df = elections.join(catastro, on="Distrito")

    # Create Vector of features
    assembler = VectorAssembler(inputCols=["ValorMedio"], outputCol="features")
    assembled_df = assembler.transform(df)
    assembled_df.show()

    # Create model
    lr = LinearRegression(maxIter=5, regParam=0.0, solver="normal")
    model = lr.fit(assembled_df)
    logger.info(u"Elecciones. Regresión PP-Catastro. Intercept: {}".format(model.intercept))
    logger.info(u"Elecciones. Regresión PP-Catastro. Coefficients: {}".format(model.coefficients))

    return model


def predict(spark, model):
    logger.info(u"Predicciones a partir de modelo de regresión")

    elections_raw = LoadElections().test(spark)
    add = udf(sumvar)
    numeric_columns = elections_raw.columns[1:]
    elections_total = elections_raw \
        .withColumn("total", add(*[elections_raw[x] for x in numeric_columns]))
    elections = elections_total \
        .withColumn("label", elections_total.PP / elections_total.total) \
        .select("Distrito", "label")
    elections.show()

    catastro = LoadCatastro().test(spark)
    assembler = VectorAssembler(inputCols=["ValorMedio"], outputCol="features")
    catastro_df = assembler.transform(catastro)
    predictions = model.transform(catastro_df)
    predictions.show()


def sumvar(*cols):
    return functools.reduce(lambda a, b: a + b, cols)


if __name__ == "__main__":
    try:

        logger.info(u"Técnicas analíticas con Spark y modelado predictivo")

        # Create Spark session
        spark = SparkSession.builder.appName("Spark Course. Linear regression").getOrCreate()
        model_trained = train(spark)
        predict(spark, model_trained)

    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
