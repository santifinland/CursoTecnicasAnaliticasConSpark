# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging

from itertools import chain
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import PCA
from pyspark.ml.feature import StandardScaler
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from common.LoadElections import LoadElections
from common.logger_configuration import LoggerManager


# Get application logger
LoggerManager()
logger = logging.getLogger()
logger_spark = logging.getLogger('py4j')
logger_spark.setLevel(logging.INFO)


def train(spark):

    logger.info(u"Entrenamiento de modelo de regresión")

    # Read Elections data and add PP percentage of votes by district
    elections_raw = LoadElections().all(spark, "true").drop("Distrito").drop("Blanco").drop("Abstencion").drop("Nulo")
    party_columns = elections_raw.columns[:]
    print(party_columns)
    elections_raw = transpose(elections_raw)

    elections_raw.show()
    numeric_columns = elections_raw.columns[:]

    assembler = VectorAssembler(inputCols=numeric_columns, outputCol="features")
    elections = assembler.transform(elections_raw)

    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures",
                            withStd=True, withMean=True)

    # Compute summary statistics by fitting the StandardScaler
    scaler_model = scaler.fit(elections)

    # Normalize each feature to have unit standard deviation.
    scaled_data = scaler_model.transform(elections)
    scaled_data.show()

    pca = PCA(k=2, inputCol="scaledFeatures", outputCol="pcaFeatures")
    model = pca.fit(scaled_data)

    result = model.transform(scaled_data).select("pcaFeatures")
    result.show(truncate=False)


def _sort_transpose_tuple(tup):
    x, y = tup
    return x, tuple(zip(*sorted(y, key=lambda v_k: v_k[1], reverse=False)))[0]


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
        lambda val_idx: val_idx[1] % n_features).sortBy( # group by index % n_features as key
        lambda grp_res: grp_res[0]).map( # sort by index % n_features key
        lambda grp_res: _sort_transpose_tuple(grp_res)).map( # maintain order
        lambda key_col: key_col[1]).toDF() # return to DF


if __name__ == "__main__":
    try:

        logger.info(u"Técnicas analíticas con Spark y modelado predictivo")

        # Create Spark session
        spark_session = SparkSession.builder.appName("Edu").getOrCreate()

        train(spark_session)

    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e.message), exc_info=True)
