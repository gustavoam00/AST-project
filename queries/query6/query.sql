CREATE TABLE t0 ( c1 , c2 , c5 ) ;
SELECT CASE WHEN subq1.c13 THEN subq1.c13 END c16 , CAST ( subq1.c13 AS ) FROM ( SELECT FALSE c13 FROM ( SELECT TRUE c9 , t1.c2 c11 , t1.c1 FROM t0 t1 WHERE 89 > t1.c1 ORDER BY c9 , c11 COLLATE NOCASE ) subq0 WHERE true ORDER BY c13 , c13 ) subq1 WHERE subq1.c13 = CASE subq1.c13 WHEN subq1.c13 = subq1.c13 THEN subq1.c13 ELSE subq1.c13 END OR subq1.c13 ;
