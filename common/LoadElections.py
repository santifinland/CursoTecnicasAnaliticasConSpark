# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging


class LoadElections(object):

    logger = logging.getLogger()

    electionsTrainPath = "data/EleccionesTrainMadrid2016.csv"
    electionsTrainPathLibSvm = "data/EleccionesLibSvm.txt"
    electionsTestPath = "data/EleccionesTestMadrid2016.csv"

    def train(self, sql_context):
        return self.run(sql_context, self.electionsTrainPath)

    def test(self, sql_context):
        return self.run(sql_context, self.electionsTestPath)

    def run(self, sql_context, path):

        self.logger.info(u"Process Load Elections")

        return sql_context \
            .read \
            .format("com.databricks.spark.csv") \
            .option("header", "true") \
            .option("delimiter", ",") \
            .option("inferSchema", "true") \
            .load(path)

    def libsvm(self, sql_context ):

        return sql_context \
            .read \
            .format("libsvm") \
            .load(self.electionsTrainPathLibSvm)
