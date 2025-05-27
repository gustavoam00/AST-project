CREATE TABLE t0 ( c1 , c2 , c3 ) ;
SELECT subq1.c5 c6 FROM ( SELECT FALSE c5 FROM ( SELECT t1.c3 c6 FROM t0 t1 ORDER BY c6 ) subq0 JOIN t0 t2 ON ( subq0.c6 ) ) subq1 WHERE subq1.c5 = CASE WHEN NULLIF ( subq1.c5 , subq1.c5 ) = CASE subq1.c5 WHEN subq1.c5 OR subq1.c5 = subq1.c5 THEN subq1.c5 WHEN true THEN subq1.c5 ELSE subq1.c5 END THEN subq1.c5 ELSE subq1.c5 END ORDER BY c6 ;
