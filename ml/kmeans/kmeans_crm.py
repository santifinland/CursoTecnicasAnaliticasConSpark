# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SparkSession

from common.LoadCrm import LoadCrm
from common.logger_configuration import LoggerManager


# Get application logger
LoggerManager()
logger = logging.getLogger()
logger_spark = logging.getLogger('py4j')
logger_spark.setLevel(logging.INFO)


def run(sql_context):

    # Read CRM
    crm_raw = LoadCrm().run(sql_context)
    crm = crm_raw.select("edad", "DE_TARIFA", "nro_lineas_cli")
    crm.show()

    # Categorical into indexed
    indexer = StringIndexer(inputCol="DE_TARIFA", outputCol="tarifa")
    indexed = indexer.fit(crm).transform(crm)
    indexed.show()

    # Create Vector of features
    assembler = VectorAssembler(inputCols=["edad", "nro_lineas_cli"], outputCol="features")
    df = assembler.transform(indexed)

    # Trains a k-means model.
    kmeans = KMeans().setK(5)
    model = kmeans.fit(df)

    # Evaluate clustering by computing Within Set Sum of Squared Errors.
    wssse = model.computeCost(df)
    logger.info("Within Set Sum of Squared Errors = " + str(wssse))

    # Shows the result.
    centers = model.clusterCenters()
    logger.info("Cluster Centers: ")
    for center in centers:
        logger.info(center)

    # People in each cluster
    res = model.transform(df)
    res.groupBy(res.prediction).count().show()


if __name__ == "__main__":
    try:
        logger.info(u"Técnicas analíticas con Spark y modelado predictivo")

        # Create Spark Session
        spark = SparkSession.builder.appName("Edu").getOrCreate()

        run(spark)

    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
