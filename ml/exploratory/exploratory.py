# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging

import numpy as np; np.random.seed(0)
import pandas as pd
#import seaborn as sns; sns.set()
from numpy.linalg import inv
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.mllib.linalg import Vectors, DenseVector
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from common.LoadElections import LoadElections
from common.logger_configuration import LoggerManager


# Get application logger
LoggerManager()
logger = logging.getLogger()
logger_spark = logging.getLogger('py4j')
logger_spark.setLevel(logging.INFO)


def main():

    logger.info(u"Técnicas analíticas con Spark y modelado predictivo")

    # Create Spark session
    spark = SparkSession.builder.appName("Educ").getOrCreate()

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
    tt = elections.drop("Distrito")
    rdd = tt.rdd.map(lambda data: Vectors.dense([float(c) for c in data]))
    r = RowMatrix(rdd)
    s = r.computeCovariance()
    print(s)
    rdd1 = spark.sparkContext.parallelize(s.toArray())
    rdd2 = rdd1.map(lambda x: [int(i) for i in x])
    df = rdd2.toDF(elections.drop("Distrito").columns)
    df.show()

    # Correlation matrix
    numeric_columns = elections.columns[1:]
    assembler = VectorAssembler(inputCols=numeric_columns, outputCol="features")
    elections = assembler.transform(elections)
    #rdd = tt.rdd.map(lambda data: Vectors.sparse([float(c) for c in data]))
    #df = spark.createDataFrame(rdd, ["features"])
    r1 = Correlation.corr(elections, 'features', 'pearson').collect()[0][0]
    print(str(r1).replace('nan', 'NaN'))
    r1 = Correlation.corr(elections, 'features', 'pearson').head()[0]
    rdd1 = spark.sparkContext.parallelize(r1.toArray())
    rdd2 = rdd1.map(lambda x: [float(i) for i in x])
    df = rdd2.toDF(numeric_columns)
    #df = rdd2.toDF()
    df.show()



def plotCov(s_pandas):
    s_plot = sns.heatmap(s_pandas)
    s_plot.set_xticklabels(rotation=90, labels=s.columns)
    s_plot.set_yticklabels(rotation=0, labels=s.columns[::-1])
    s_plot.get_figure().savefig("/tmp/cov.png")

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
