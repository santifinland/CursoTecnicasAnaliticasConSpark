# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging


class LoadCatastro(object):

    logger = logging.getLogger()

    catastroTrainPath = "../../data/CatastroTrainMadrid2014.csv"
    catastroTestPath = "../../data/CatastroTestMadrid2014.csv"

    def train(self, sql_context):
        return self.run(sql_context, self.catastroTrainPath)

    def test(self, sql_context):
        return self.run(sql_context, self.catastroTestPath)

    def run(self, sql_context, path):

        self.logger.info(u"Process Load Catastro")

        return sql_context \
            .read \
            .format("com.databricks.spark.csv") \
            .option("header", "true") \
            .option("delimiter", ",") \
            .option("inferSchema", "true") \
            .load(path)
