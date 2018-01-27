# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging
from itertools import chain

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.mllib.linalg.distributed import *
from pyspark.sql import DataFrame
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

from common.LoadElections import LoadElections
from common.logger_configuration import LoggerManager


# Get application logger
LoggerManager()
logger = logging.getLogger()
logger_spark = logging.getLogger('py4j')
logger_spark.setLevel(logging.INFO)


def main():

    logger.info(u"Técnicas analíticas con Spark y modelado predictivo")

    # Create Spark Session
    spark = SparkSession.builder.appName("Edu").getOrCreate()

    # Read data
    elections = LoadElections().train(spark)

    # Covariance matrix
    elections.show()


    h = centered_matrix(elections).drop("Distrito")
    ht = transpose(h)
    s = as_block_matrix(ht).multiply(as_block_matrix(h))
    s_np = s.toLocalMatrix().toArray()
    s = spark.sql.createDataFrame(pd.DataFrame(s_np, columns=elections.columns[1:]))
    s.repartition(1) \
        .write \
        .option("header", "true") \
        .option("delimiter", "|") \
        .csv("/tmp/cov/")


def covariance_matrix(df):
    df.show()
    h = centered_matrix(df)
    ht = transpose(h)
    s = as_block_matrix(ht).multiply(as_block_matrix(h))
    return s


def centered_matrix(df):
    def loop(df, acc, key):
        if len(df.columns) == 1:
            return acc
        column = df.columns[1]
        mean = df.select(column).agg({column: "mean"}).first().asDict().get("avg(" + column + ")")
        return loop(df.drop(column), acc.join(df.select(key, (expr(column) - mean).alias(column)), on=key), key)
    key = df.columns[0]
    return loop(df, df.select(key), key)


def _sort_transpose_tuple(tup):
    x, y = tup
    return (x, zip(*sorted(y, key=lambda (v,k): k, reverse=False))[0])


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


def as_block_matrix(df, rowsPerBlock=1, colsPerBlock=1):
    #rdd = df.rdd.map(lambda data: Vectors.dense([float(int(c)) for c in data]))
    assembler = VectorAssembler(
        inputCols=df.columns, outputCol="features"
    )
    rdd = assembler.transform(df)
    return IndexedRowMatrix(
        rdd.zipWithIndex().map(lambda xi: IndexedRow(xi[1], xi[0]))
    ).toBlockMatrix(rowsPerBlock, colsPerBlock)


if __name__ == "__main__":
    try:
        main()
    except Exception, e:
        logger.error('Failed to execute process: {}'.format(e.message), exc_info=True)
