# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging


class LoadRatings(object):

    logger = logging.getLogger()

    path = "data/ratings.csv"

    def run(self, sql_context):

        self.logger.info(u"Process Load Ratings")

        return sql_context \
            .read \
            .format("com.databricks.spark.csv") \
            .option("header", "true") \
            .option("delimiter", ",") \
            .option("inferSchema", "true") \
            .load(self.path)
