# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging


class LoadParties(object):

    logger = logging.getLogger()

    partiesTrainPath = "data/partiesTrain.csv"
    partiesTestPath = "data/partiesTest.csv"

    def train(self, sql_context):
        return self.run(sql_context, self.partiesTrainPath)

    def test(self, sql_context):
        return self.run(sql_context, self.partiesTestPath)

    def run(self, sql_context, path):

        self.logger.info(u"Process Load Parties")

        return sql_context \
            .read \
            .format("com.databricks.spark.csv") \
            .option("header", "true") \
            .option("delimiter", ",") \
            .option("inferSchema", "true") \
            .load(path)

