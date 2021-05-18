# -*- coding: utf-8 -*-

import os

from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier

from common.logger_configuration import logger


def main():
    logger.info(u"Exercise")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course").getOrCreate()

    # Read csv
    path = os.path.join("data", "income.csv")
    data = spark.read.csv(path, header=True, inferSchema=True)
    data.show()

    # Indexed data
    assembler = VectorAssembler(inputCols=["education-num"], outputCol="features")
    data_assembled = assembler.transform(data)
    data_assembled.show()

    assembler = VectorAssembler(inputCols=["income"], outputCol="label")

    # Join data
    df = elections.join(catastro, on="Distrito")

    # Train a DecisionTree model.
    dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="features")
    dt.fit(data_assembled)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
