# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession

from LoadRatings import LoadRatings
from logger_configuration import LoggerManager


# Get application logger
LoggerManager()
logger = logging.getLogger()
logger_spark = logging.getLogger('py4j')
logger_spark.setLevel(logging.INFO)


def run(spark_session):

    ratings = LoadRatings().run(spark_session)
    (training, test) = ratings.randomSplit([0.8, 0.2])

    # Build the recommendation model using ALS on the training data
    als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating")
    model = als.fit(training)

    # Evaluate the model by computing the RMSE on the test data
    predictions = model.transform(test)
    predictions_no_nan = predictions.filter(predictions.prediction != float('nan'))  # SPARK-14489
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions_no_nan)
    print("Root-mean-square error: " + str(rmse))


if __name__ == "__main__":
    try:
        logger.info(u"Técnicas analíticas con Spark y modelado predictivo")

        # Create Spark Session
        spark = SparkSession.builder.appName("Edu").getOrCreate()

        run(spark)

    except Exception, e:
        logger.error('Failed to execute process: {}'.format(e.message), exc_info=True)
