# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession

from common.LoadRatings import LoadRatings
from common.logger_configuration import LoggerManager


# Get application logger
LoggerManager()
logger = logging.getLogger()
logger_spark = logging.getLogger('py4j')
logger_spark.setLevel(logging.INFO)


def run(spark_session):

    ratings = LoadRatings().run(spark_session)
    (training, test) = ratings.randomSplit([0.8, 0.2])
    training = spark.read \
        .format("com.databricks.spark.csv") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("inferSchema", "true") \
        .load("../data/ratingsppttrain.csv")
    test = spark.read \
        .format("com.databricks.spark.csv") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("inferSchema", "true") \
        .load("../data/ratingsppttest.csv")

    # Build the recommendation model using ALS on the training data
    als = ALS(maxIter=5,
              implicitPrefs=True,
              regParam=0.01,
              #coldStartStrategy="drop",
              userCol="userId",
              itemCol="movieId",
              ratingCol="rating")
    model = als.fit(training)

    # Evaluate the model by computing the RMSE on the test data
    predictions = model.transform(test)
    predictions.show()
    #predictions_no_nan = predictions.filter(predictions.prediction != float('nan'))  # SPARK-14489
    #evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    #rmse = evaluator.evaluate(predictions_no_nan)
    #print("Root-mean-square error: " + str(rmse))

    # Generate top 10 movie recommendations for each user
    userRecs = model.recommendForAllUsers(10)
    userRecs.show(truncate=False)
    # Generate top 10 user recommendations for each movie
    movieRecs = model.recommendForAllItems(10)
    movieRecs.show(truncate=False)


if __name__ == "__main__":
    try:
        logger.info(u"Técnicas analíticas con Spark y modelado predictivo")

        # Create Spark Session
        spark = SparkSession.builder.appName("Edu").getOrCreate()

        run(spark)

    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
