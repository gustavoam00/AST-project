CREATE TABLE F ( p NOT NULL NULL NOT NULL , i ) ;
INSERT INTO F SELECT * FROM ( VALUES ( true , false ) , ( NULL , true ) ) AS L WHERE ( true ) ;
