#!/usr/bin/env bash

: '
Técnicas analíticas con Spark y modelado predictivo
'

# Default values for parameters
SPARK_HOME_DEFAULT="/usr/local/spark/"

# Parse parameters
for i in "$@"
do
case $i in
    -s=*|--spark=*)
    SPARK_HOME="${i#*=}"
    shift # past argument=value
    ;;
esac
done

# Check if SPARK_HOME is set and set to default if not
if [ -z ${SPARK_HOME+x} ]
    then SPARK_HOME=${SPARK_HOME_DEFAULT}
fi

# Launch Exploratory Spark application
${SPARK_HOME}/bin/spark-submit \
    --master local[1] \
    --num-executors 1 \
    --packages com.databricks:spark-csv_2.10:1.4.0,graphframes:graphframes:0.1.0-spark1.5 \
    --conf spark.sql.parquet.compression.codec=snappy \
    --conf spark.default.parallelism=1 \
    pca.py $@
