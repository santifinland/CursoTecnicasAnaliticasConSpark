# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging

import numpy as np

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.sql import SparkSession

from common.LoadElections import LoadElections
from common.logger_configuration import LoggerManager


# Get application logger
LoggerManager()
logger = logging.getLogger()
logger_spark = logging.getLogger('py4j')
logger_spark.setLevel(logging.INFO)


def main():

    logger.info(u"Exploratory analysis")

    # Create Spark session
    spark = SparkSession.builder.appName("Spark Course. Exploratory analysis").getOrCreate()

    # Read data
    elections = LoadElections().all(spark)
    elections.show()

    # Describe data
    elections_count = elections.count()
    logger.info(u"Elecciones. Número de observaciones: {}".format(elections_count))
    elections_columns = elections.columns
    logger.info(u"Elecciones. Variables observadas: {}".format(elections_columns))

    # Describe data
    elections.describe("Distrito", "PP", "PSOE", "PodemosIU", "Ciudadanos").show()

    # Describe schema
    elections.printSchema()

    # Compute aggregations
    elections.select("PP").agg({"PP": "mean"}).show()
    elections.select("PP").groupBy().mean("PP").show()
    elections.select("PP").agg({"PP": "max"}).show()
    elections.select("PP").agg({"PP": "min"}).show()

    # Compute variance
    variance_pp = elections.cov("PP", "PP")
    logger.info(u"Elecciones. Varianza PP: {}".format(variance_pp))

    # Relations between variables
    covariance_pp_ciudadanos = elections.cov("PP", "Ciudadanos")
    correlation_pp_ciudadanos = elections.corr("PP", "Ciudadanos")
    logger.info(u"Elecciones. Covarianza PP-Ciudadanos: {}".format(covariance_pp_ciudadanos))
    logger.info(u"Elecciones. Correlación PP-Ciudadanos: {}".format(correlation_pp_ciudadanos))
    covariance_pp_podemos = elections.cov("PP", "PodemosIU")
    correlation_pp_podemos = elections.corr("PP", "PodemosIU")
    logger.info(u"Elecciones. Covarianza PP-PodemosIU: {}".format(covariance_pp_podemos))
    logger.info(u"Elecciones. Correlation PP-PodemosIU: {}".format(correlation_pp_podemos))
    correlation_psoe_podemos = elections.corr("PSOE", "PodemosIU")
    logger.info(u"Elecciones. Correlation PSOE-PodemosIU: {}".format(correlation_psoe_podemos))

    # Covariance matrix
    elections_rdd = elections \
        .drop("Distrito") \
        .rdd \
        .map(lambda data: Vectors.dense([float(c) for c in data]))
    s = RowMatrix(elections_rdd).computeCovariance()
    spark.sparkContext.parallelize(s.toArray()) \
        .map(lambda x: [int(i) for i in x]) \
        .toDF(elections.drop("Distrito").columns) \
        .show()

    # Correlation matrix
    numeric_columns = elections.columns[1:]
    assembler = VectorAssembler(inputCols=numeric_columns, outputCol="features")
    elections = assembler.transform(elections)
    elections.show()
    r1 = Correlation.corr(elections, 'features', 'pearson').head()[0]
    spark.sparkContext.parallelize(r1.toArray()) \
        .map(lambda x: [float(i) for i in x]) \
        .toDF(numeric_columns) \
        .show()


if __name__ == "__main__":
    try:
        np.random.seed(0)
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
