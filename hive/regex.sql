CREATE EXTERNAL TABLE test.logs (
  host STRING,
  identity STRING,
  user_id STRING,
  time STRING,
  request STRING,
  status STRING,
  size STRING
  )
ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.RegexSerDe"
WITH SERDEPROPERTIES (
  "input.regex" = "([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*)",
  "output.format.string" = "%1$s %2$s %3$s %4$s %5$s %6$s %7$s"
)
STORED AS TEXTFILE
LOCATION "gs://dataproc-99ba7284-0055-48b5-ba71-79fc56a0ba27-europe-west1/CursoTecnicasAnaliticasConSpark/data/logsnasa";
