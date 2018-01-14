CREATE EXTERNAL TABLE catalogo (
  content_title STRING,
  content_sub_title STRING,
  external_asse_id STRING,
  content_kind STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 'gs://dataproc-99ba7284-0055-48b5-ba71-79fc56a0ba27-europe-west1/CursoTecnicasAnaliticasConSpark/data/peliculas/';
