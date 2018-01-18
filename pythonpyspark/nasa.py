# -*- coding: utf-8 -*-
# REGEX patterns for web logs

import re
import datetime
import os
import logging

from pyspark.sql import Row, SparkSession


from common.logger_configuration import LoggerManager

# Global vars
month_map = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,  'Sep': 9, 'Oct': 10,
             'Nov': 11, 'Dec': 12}
APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*).*?" (\d+) (\S+)'

# Get application logger
LoggerManager()
logger = logging.getLogger()
logger_spark = logging.getLogger('py4j')
logger_spark.setLevel(logging.INFO)


def main():
    logger.info(u"REGEX patterns for web logs")

    # Create Spark Session
    spark = SparkSession.builder.appName("Edu").getOrCreate()

    base_dir = os.path.join('../data')
    input_path = os.path.join('nasa.txt')
    log_file = os.path.join(base_dir, input_path)

    logs = load_logs(spark.sparkContext, log_file=log_file)
    print 'Logs example %s' % logs.take(5)

    # Numero de host distintos:
    hosts = logs.map(lambda l: l.host)
    unique_hosts = hosts.distinct()
    print 'Example Unique Host: %s' % unique_hosts.take(10)
    print 'Number Unique Hosts: %d' % unique_hosts.count()

    # Cuantos logs hay con error 404
    bad_records = (logs
                   .filter(lambda log: log.response_code == 404)
                   .cache())
    print 'Found %d 404 URLs' % bad_records.count()

    # Mostrar 10 endpoints no encontrados
    bad_endpoints = bad_records.map(lambda log: log.endpoint)
    bad_unique_endpoints = bad_endpoints.distinct()
    bad_unique_endpoints_pick10 = bad_unique_endpoints.take(10)
    print '404 URLS: %s' % bad_unique_endpoints_pick10

    # Top 10 endpoints
    endpoint_counts = (logs
                       .map(lambda log: (log.endpoint, 1))
                       .reduceByKey(lambda a, b: a + b))
    top_endpoints = endpoint_counts.takeOrdered(10, lambda s: -1 * s[1])
    print 'Top Ten Endpoints: %s' % top_endpoints

    # Nimero de Hosts unicos por dia
    day_to_host_pair_tuple = logs.map(lambda log: (log.date_time.day, log.host))
    day_grouped_hosts = day_to_host_pair_tuple.groupByKey().mapValues(lambda x: list(x))
    day_host_count = day_grouped_hosts.map(lambda (x, y): (x, len(set(y))))
    daily_hosts = (day_host_count
                   .sortByKey()).cache()
    daily_hosts_list = daily_hosts.take(30)
    print 'Daily Hosts List: %s' % daily_hosts_list

    # Grabar los datos en un fichero
    logs.map(lambda log: log.host+";"+str(log.date_time.year)+";"+str(log.date_time.month)+";"+str(log.date_time.day) +
             ";"+log.endpoint+";"+str(log.response_code)).saveAsTextFile('parsedLogs')

    # Ejecución con spark-submit
    # Primero Borramos el fichero de salida porque si no dara error cuando se ejecuta mas de una vez
    # hdfs dfs -rm -r parsedLogs
    # Ejecutamos el programa con spark-submit
    # spark-submit --master local[*] LogsPython.py


def parse_apache_time(s):
    """ Convert Apache time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format
    Returns:
        datetime: datetime object (ignore timezone for now)
    """
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))


def parse_apache_log_line(logline):
    """ Parse a line in the Apache Common Log format
    Args:
        logline (str): a line of text in the Apache Common Log format
    Returns:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0
    """
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return logline, 0
    size_field = match.group(9)
    if size_field == '-':
        size = long(0)
    else:
        size = long(match.group(9))
    return (Row(
        host=match.group(1),
        client_identd=match.group(2),
        user_id=match.group(3),
        date_time=parse_apache_time(match.group(4)),
        method=match.group(5),
        endpoint=match.group(6),
        protocol=match.group(7),
        response_code=int(match.group(8)),
        content_size=size
    ), 1)


def load_logs(sc, log_file):
    """ Load logs
        Cargamos el fichero de Logs, lo parseamos y filtramos los Logs que han mapeado con nuestro patrón.
    """
    access_logs = (sc
                   .textFile(log_file)
                   .map(parse_apache_log_line)
                   .cache())

    logs = (access_logs
            .filter(lambda s: s[1] == 1)
            .map(lambda s: s[0])
            .cache())
    print 'Read %d lines' % logs.count()
    return logs


if __name__ == "__main__":
    try:
        main()
    except Exception, e:
        logger.error('Failed to execute process: {}'.format(e.message), exc_info=True)
