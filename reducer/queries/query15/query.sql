CREATE TABLE T1 ( c3 ) ;
CREATE TABLE T2 ( c1 ) ;
CREATE TABLE T3 ( c1 , c2 ) ;
INSERT INTO T3 ( c1 ) VALUES ( 564 ) ;
INSERT INTO T1 VALUES ( -131 ) ;
SELECT 491 FROM T1 a LEFT JOIN T2 b ON a.c3 = b.c1 LEFT JOIN T3 c ON b.c1 WHERE c.c1 ;
