CREATE TABLE t0 ( c1 , c2 , c3 ) ;
SELECT subq1.c5 FROM ( SELECT FALSE c5 FROM ( SELECT t1.c3 , t1.c2 , t1.c2 c7 FROM t0 t1 ORDER BY c7 ) ORDER BY c5 ) subq1 WHERE subq1.c5 = CASE WHEN subq1.c5 <> CASE subq1.c5 WHEN subq1.c5 = subq1.c5 OR subq1.c5 = subq1.c5 THEN subq1.c5 WHEN true THEN subq1.c5 ELSE subq1.c5 END THEN subq1.c5 ELSE subq1.c5 END ;
