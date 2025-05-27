CREATE TABLE IF NOT EXISTS table_3 ( table_3_c0 , table_3_c1 ) ;
INSERT INTO table_3 ( table_3_c0 , table_3_c1 ) VALUES ( -2 , NULL ) ;
SELECT * FROM table_3 WHERE ( SELECT table_3_c1 LIMIT NULL ) ;
