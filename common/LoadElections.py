# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging


class LoadElections(object):

    logger = logging.getLogger()

    electionsTrainPath = "../../data/EleccionesTrainMadrid2016.csv"
    electionsTrainPathLibSvm = "../../data/EleccionesLibSvm.txt"
    electionsTestPath = "../../data/EleccionesTestMadrid2016.csv"
    electionsPath = "../../data/EleccionesMadrid2016.csv"

    def train(self, spark):
        return self.run(spark, self.electionsTrainPath)

    def test(self, spark):
        return self.run(spark, self.electionsTestPath)

    def all(self, spark, header="true"):
        return self.run(spark, self.electionsPath, header)

    def run(self, spark, path, header="true"):

        self.logger.info(u"Process Load Elections")

        return spark \
            .read \
            .option("header", header) \
            .option("delimiter", ",") \
            .option("inferSchema", "true") \
            .csv(path)

    def libsvm(self, spark ):

        return spark \
            .read \
            .format("libsvm") \
            .load(self.electionsTrainPathLibSvm)
