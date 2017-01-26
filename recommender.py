# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging
import math

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS

from common.LoadRatings import LoadRatings
from common.logger_configuration import LoggerManager


# Get application logger
LoggerManager()
logger = logging.getLogger()
logger_spark = logging.getLogger('py4j')
logger_spark.setLevel(logging.INFO)


def run(sql_context):

    ratings = LoadRatings().run(sql_context)
    (training, test) = ratings.randomSplit([0.8, 0.2])

    # Build the recommendation model using ALS on the training data
    als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating")
    model = als.fit(training)

    # Evaluate the model by computing the RMSE on the test data
    predictions = model.transform(test)
    predictions_no_nan = predictions.filter(predictions.prediction != float('nan')) # SPARK-14489
    rmsd = predictions_no_nan \
        .withColumn("diff",
                    (predictions_no_nan.prediction - predictions_no_nan.rating) *
                    (predictions_no_nan.prediction - predictions_no_nan.rating)) \
        .agg({"diff": "sum"}) \
        .withColumnRenamed("sum(diff)", "sum_diff") \
        .head() \
        .sum_diff
    print("Root-mean-square error:" + str(math.sqrt(rmsd/predictions_no_nan.count())))

    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions_no_nan)
    print("Root-mean-square error: " + str(rmse))

if __name__ == "__main__":
    try:
        # Create Spark context
        conf = SparkConf().setMaster("local").setAppName("Esic")
        sc = SparkContext(conf=conf)
        sql_context = SQLContext(sc)

        logger.info(u"Técnicas analíticas con Spark y modelado predictivo")

        model = run(sql_context)

    except Exception, e:
        logger.error('Failed to execute process: {}'.format(e.message), exc_info=True)
