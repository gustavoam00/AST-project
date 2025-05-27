CREATE TABLE table_2 (table_2_c0, table_2_c1, table_2_c2 );
CREATE TABLE IF NOT EXISTS table_3 (table_3_c0, table_3_c1 );
INSERT INTO table_3 (table_3_c0, table_3_c1) VALUES (-2, NULL);
SELECT DISTINCT * FROM table_3, table_2 WHERE EXISTS ( SELECT table_3_c1 FROM table_3 LIMIT NULL ) LIMIT 3;
