CREATE TABLE F ( p NOT NULL , i ) ;
INSERT INTO F SELECT * FROM ( VALUES ( true , false ) , ( NULL , true ) ) WHERE ( true ) ;
