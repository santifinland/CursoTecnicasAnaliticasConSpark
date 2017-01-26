# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging

import math
from numpy.linalg import inv
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import numpy as np; np.random.seed(0)
import pandas as pd
import seaborn as sns; sns.set()

from common.LoadElections import LoadElections
from common.LoadCovariance import LoadCovariance
from common.logger_configuration import LoggerManager


# Get application logger
LoggerManager()
logger = logging.getLogger()
logger_spark = logging.getLogger('py4j')
logger_spark.setLevel(logging.INFO)


def main():

    logger.info(u"Técnicas analíticas con Spark y modelado predictivo")

    # Create Spark context
    conf = SparkConf().setMaster("local").setAppName("Esic")
    sc = SparkContext(conf=conf)
    sql_context = SQLContext(sc)

    # Read data
    elections = LoadElections().train(sql_context)
    #elections.show()

    # Describe data
    #elections_count = elections.count()
    #logger.info(u"Elecciones. Número de observaciones: {}".format(elections_count))
    #elections_columns = elections.columns
    #logger.info(u"Elecciones. Variables observadas: {}".format(elections_columns))

    # Describe data
    elections.describe("Distrito", "PP", "PSOE", "PodemosIU", "Ciudadanos").show()

    # Describe data
    elections.printSchema()

    elections.select("PP").agg({"PP": "mean"}).show()
    elections.select("PP").groupBy().mean("PP").show()
    elections.select("PP").agg({"PP": "max"}).show()
    elections.select("PP").agg({"PP": "min"}).show()
    variance_PP = elections.cov("PP", "PP")
    logger.info(u"Elecciones. Varianza PP: {}".format(variance_PP))

    # Relations between variables
    covariance_PP_Ciudadanos = elections.cov("PP", "Ciudadanos")
    correlation_PP_Ciudadanos = elections.corr("PP", "Ciudadanos")
    logger.info(u"Elecciones. Covarianza PP-Ciudadanos: {}".format(covariance_PP_Ciudadanos))
    logger.info(u"Elecciones. Correlación PP-Ciudadanos: {}".format(correlation_PP_Ciudadanos))
    covariance_PP_Podemos = elections.cov("PP", "PodemosIU")
    correlation_PP_Podemos = elections.corr("PP", "PodemosIU")
    logger.info(u"Elecciones. Covarianza PP-PodemosIU: {}".format(covariance_PP_Podemos))
    logger.info(u"Elecciones. Correlation PP-PodemosIU: {}".format(correlation_PP_Podemos))
    correlation_PSOE_Podemos = elections.corr("PSOE", "PodemosIU")
    logger.info(u"Elecciones. Correlation PSOE-PodemosIU: {}".format(correlation_PSOE_Podemos))

    # Covariance matrix
    s = LoadCovariance().run(sql_context)
    s_pandas = s.toPandas()
    #s_plot = sns.heatmap(s_pandas)
    #s_plot.set_xticklabels(rotation=90, labels=s.columns)
    #s_plot.set_yticklabels(rotation=0, labels=s.columns[::-1])
    #s_plot.get_figure().savefig("/tmp/cov.png")

    # Correlation matrix
    #d = pd.DataFrame(0, index=np.arange(len(s_pandas.columns)))
    d = pd.DataFrame(np.zeros((len(s_pandas.columns), len(s_pandas.columns))))
    for i in range(len(s_pandas.columns)):
        for j in range(len(s_pandas.columns)):
            if i == j:
                d.loc[i, j] = math.sqrt(np.diag(s_pandas)[i])
    d_inv = inv(d)
    #r = d_inv * s_pandas.as_matrix() * d_inv
    r = d_inv.dot(s_pandas).dot(d_inv)
    r_plot = sns.heatmap(r)
    r_plot.set_xticklabels(rotation=90, labels=s.columns)
    r_plot.set_yticklabels(rotation=0, labels=s.columns[::-1])
    r_plot.get_figure().savefig("/tmp/corr.png")



if __name__ == "__main__":
    try:
        main()
    except Exception, e:
        logger.error('Failed to execute process: {}'.format(e.message), exc_info=True)
