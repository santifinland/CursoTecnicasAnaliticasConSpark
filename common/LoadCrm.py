# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging


class LoadCrm(object):

    logger = logging.getLogger()

    path = "data/HOUSEHOLD_MASTER.csv"

    def run(self, sql_context):

        self.logger.info(u"Process Load Parties")

        return sql_context \
            .read \
            .format("com.databricks.spark.csv") \
            .option("header", "true") \
            .option("delimiter", ",") \
            .option("inferSchema", "true") \
            .load(self.path)

