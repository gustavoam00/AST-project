CREATE TABLE t0 ( c0 UNIQUE , c1 , c2 UNIQUE , c3 UNIQUE ) ;
CREATE INDEX i8 ON t0 ( c1 ) WHERE t0.c3 < 'default' ;
INSERT INTO t0 ( c1 ) VALUES ( 89 ) , ( 49 ) , ( 74 ) , ( 88 ) , ( NULL ) , ( 13 ) , ( 58 ) , ( NULL ) , ( 87 ) , ( 69 ) ;
SELECT t0.c1 , t0.c0 , COALESCE ( AVG ( t0.c0 ) OVER ( PARTITION BY t0.c3 ORDER BY t0.c0 DESC ) , 9 ) FROM t0 WHERE t0.c1 IN ( SELECT t0.c1 FROM t0 )
