CREATE TABLE IF NOT EXISTS test.test_table_ctas AS
SELECT content_title, external_asse_id
FROM test.catalogo WHERE content_kind = 'Cine';