# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import PCA

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
    numeric_columns = elections_raw.columns[1:]
    assembler = VectorAssembler(inputCols=numeric_columns, outputCol="features")
    elections = assembler.transform(elections_raw)

    pca = PCA(k=2, inputCol="features", outputCol="pcaFeatures")
    model = pca.fit(elections)

    result = model.transform(elections).select(["Distrito","pcaFeatures"])
    result.show(truncate=False)


if __name__ == "__main__":
    try:

        # Create Spark context
        conf = SparkConf().setMaster("local").setAppName("Esic")
        sc = SparkContext(conf=conf)
        sql_context = SQLContext(sc)

        logger.info(u"Técnicas analíticas con Spark y modelado predictivo")

        model = train(sql_context)

    except Exception, e:
        logger.error('Failed to execute process: {}'.format(e.message), exc_info=True)
