CREATE TABLE t0 ( c0 , c1 , c3 ) ;
CREATE INDEX i8 ON t0 ( c1 ) WHERE 'default' ;
INSERT INTO t0 ( c1 ) VALUES ( 69 ) ;
SELECT t0.c1 , COALESCE ( t0.c0 , 9 ) FROM t0 WHERE t0.c1 IN ( SELECT t0.c1 FROM t0 )
