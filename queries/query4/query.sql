CREATE TABLE t1 ( col0 , col1 int , col2 , col3 int , col4 ) ;
INSERT INTO t1 ( col1 , col2 , col3 , col4 ) VALUES ( 448715285 , 'M' , -1832017664 , 'TOODVU' ) ;
INSERT INTO t1 ( col0 , col1 , col2 , col4 ) VALUES ( NULL , 2049300292 , 'sYCWGtg8J' , 'RxYwTBHZ3' ) ;
SELECT LAG ( -403961669 ) OVER ( ) AS win0 , AVG ( UPPER ( t1.col2 || 'HAFG' ) ) OVER ( PARTITION BY col4 ORDER BY col3 ) AS win1 , ROW_NUMBER ( ) OVER ( RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING ) AS win2 FROM t1 ;
