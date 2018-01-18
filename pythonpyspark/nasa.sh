#!/usr/bin/env bash

spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 2 \
    --executor-cores 4 \
    --driver-memory 4g \
    --executor-memory 5g \
    --conf spark.sql.parquet.compression.codec=snappy \
    --py-files zipfile.zip \
    pyspark/nasa.py $@