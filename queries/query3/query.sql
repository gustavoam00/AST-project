CREATE TABLE table_3 ( table_3_c0 , table_3_c1 ) ;
INSERT INTO table_3 ( table_3_c1 ) VALUES ( NULL ) ;
SELECT * FROM table_3 WHERE ( SELECT table_3_c1 LIMIT NULL ) ;
