# -*- coding: utf-8 -*-

import os

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from common.logger_configuration import logger


def main():
    logger.info(u"Read csv files with header")

    # Create Spark Session
    spark = SparkSession.builder.appName("Spark Course. Csv files injecting schema").getOrCreate()

    # Define schema of the data
    schema = StructType([
        StructField("Distrito", StringType(), False),
        StructField("PP", IntegerType(), True),
        StructField("PodemosIU", DoubleType(), True),
        StructField("PSOE", FloatType(), True),
        StructField("Ciudadanos", IntegerType(), True),
        StructField("Pacma", IntegerType(), True),
        StructField("Vox", IntegerType(), True),
        StructField("UPyD", IntegerType(), True),
        StructField("RecortesCero", IntegerType(), True),
        StructField("FEJONS", IntegerType(), True),
        StructField("PCPE", IntegerType(), True),
        StructField("PH", IntegerType(), True),
        StructField("SAIn", IntegerType(), True),
        StructField("PLIB", IntegerType(), True),
        StructField("Abstencion", IntegerType(), True),
        StructField("Nulo", IntegerType(), True),
        StructField("Blanco", IntegerType(), True)])

    # Read csv injecting schema
    path = os.path.join("data", "EleccionesMadrid2016.csv")
    elections = spark.read.csv(path, header=True, schema=schema)
    elections.show()
    elections.printSchema()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error('Failed to execute process: {}'.format(e), exc_info=True)
