CREATE TABLE t0 ( c0 , c1 ) ;
SELECT subq1.c8 FROM ( SELECT TRUE c8 FROM ( SELECT t1.c1 c5 FROM t0 t1 ORDER BY c5 ) WHERE trim ( 67 ) NOT NULL ORDER BY c8 ) subq1 WHERE subq1.c8 <> CASE subq1.c8 WHEN subq1.c8 THEN subq1.c8 WHEN CAST ( subq1.c8 AS ) THEN CAST ( subq1.c8 AS ) ELSE ( subq1.c8 , subq1.c8 ) END = CASE subq1.c8 WHEN subq1.c8 = subq1.c8 THEN subq1.c8 ELSE subq1.c8 END ;
