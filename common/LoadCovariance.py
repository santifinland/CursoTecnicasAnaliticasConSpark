# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

import logging


class LoadCovariance(object):

    logger = logging.getLogger()

    covarianceMatrixPath = "/tmp/cov/"

    def run(self, sql_context):

        self.logger.info(u"Process Load Covariance Matrix")

        return sql_context \
            .read \
            .format("com.databricks.spark.csv") \
            .option("header", "true") \
            .option("delimiter", "|") \
            .option("inferSchema", "true") \
            .load(self.covarianceMatrixPath)
