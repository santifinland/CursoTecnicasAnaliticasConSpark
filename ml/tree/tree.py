# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession

from common.LoadElections import LoadElections
from common.logger_configuration import LoggerManager


# Get application logger
LoggerManager()
logger = logging.getLogger()
logger_spark = logging.getLogger('py4j')
logger_spark.setLevel(logging.INFO)


def train(spark):

    # Read Elections data in libsvm format
    data = LoadElections().libsvm(spark)

    # Index labels, adding metadata to the label column.
    # Fit on whole dataset to include all labels in index.
    label_indexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)

    # Automatically identify categorical features, and index them.
    # We specify maxCategories so features with > 4 distinct values are treated as continuous.
    feature_indexer = \
        VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])
    print(trainingData.count())
    print(testData.count())

    # Train a DecisionTree model.
    dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

    # Chain indexers and tree in a Pipeline
    pipeline = Pipeline(stages=[label_indexer, feature_indexer, dt])

    # Train model.  This also runs the indexers.
    model = pipeline.fit(trainingData)

    # Make predictions.
    predictions = model.transform(testData)

    # Select example rows to display.
    predictions.select("prediction", "indexedLabel", "features").show()

    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print("Test Error = %g " % (1.0 - accuracy))

    tree_model = model.stages[2].toDebugString
    print(tree_model)


if __name__ == "__main__":
    try:

        logger.info(u"Técnicas analíticas con Spark y modelado predictivo")

        # Create Spark Session
        spark_session = SparkSession.builder.appName("Edu").getOrCreate()

        train(spark_session)

    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
