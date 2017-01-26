# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

from common.LoadElections import LoadElections
from common.LoadCatastro import LoadCatastro
from common.logger_configuration import LoggerManager


# Get application logger
LoggerManager()
logger = logging.getLogger()
logger_spark = logging.getLogger('py4j')
logger_spark.setLevel(logging.INFO)


def train(sql_context):

    logger.info(u"Entrenamiento de modelo de regresión")

    # Read Elections data and add PP percentage of votes by district
    elections_raw = LoadElections().train(sql_context)
    add = udf(sumvar)
    numeric_columns = elections_raw.columns[1:]
    elections_total = elections_raw \
        .withColumn("total", add(*[elections_raw[x] for x in numeric_columns]))
    elections =  elections_total \
        .withColumn("label", elections_total.PP / elections_total.total) \
        .select("Distrito", "label")

    # Read Catastro data
    catastro = LoadCatastro().train(sql_context).select(
        expr("Distrito"),
        expr("ValorMedio"))

    # Join Elections and Catastro data
    df = elections.join(catastro, on="Distrito")

    # Create Vector of features
    assembler = VectorAssembler(inputCols=["ValorMedio"], outputCol="features")
    assembled_df = assembler.transform(df)

    # Create model
    lr = LinearRegression(maxIter=5, regParam=0.0, solver="normal")
    model = lr.fit(assembled_df)
    logger.info(u"Elecciones. Regresión PP-Catastro: {}".format(model.intercept))
    logger.info(u"Elecciones. Regresión PP-Catastro: {}".format(model.coefficients))

    return model


def predict(sql_context, model):
    logger.info(u"Predicciones a partir de modelo de regresión")

    elections_raw = LoadElections().test(sql_context)
    add = udf(sumvar)
    numeric_columns = elections_raw.columns[1:]
    elections_total = elections_raw \
        .withColumn("total", add(*[elections_raw[x] for x in numeric_columns]))
    elections =  elections_total \
        .withColumn("label", elections_total.PP / elections_total.total) \
        .select("Distrito", "label")
    elections.show()

    catastro = LoadCatastro().test(sql_context)
    assembler = VectorAssembler(inputCols=["ValorMedio"], outputCol="features")
    catastro_df = assembler.transform(catastro)
    predictions = model.transform(catastro_df)
    predictions.show()


def sumvar(*cols):
    return reduce(lambda a, b: a + b, cols)

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
