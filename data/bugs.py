# BUGS from https://www3.sqlite.org/changes.html version 3.27.0 - 3.39.4

BUGS27 = [
    [
        "CREATE TABLE t1(a,b);",
        "INSERT INTO t1 VALUES(1,1);",
        "INSERT INTO t1 VALUES(2,2);",
        "CREATE TABLE t2(x);",
        "INSERT INTO t2 VALUES(1);",
        "INSERT INTO t2 VALUES(2);",
        "SELECT 'one', * FROM t2 WHERE x NOT IN (SELECT a FROM t1);",
        "CREATE INDEX t1a ON t1(a) WHERE b=1;",
        "SELECT 'two', * FROM t2 WHERE x NOT IN (SELECT a FROM t1);"
    ],[
        "SELECT * FROM(SELECT * FROM (SELECT 1 AS c) WHERE c IN (SELECT (row_number() OVER()) FROM (VALUES (0))));"
    ],[
        "CREATE TABLE t1(a, b);",
        "INSERT INTO t1 VALUES('abc', 'ABC');",
        "INSERT INTO t1 VALUES('def', 'DEF');",
        "SELECT * FROM t1 WHERE upper(a) = 'ABC' OR lower(b) = 'def';",
        "CREATE INDEX i1 ON t1( upper(a) );",
        "CREATE INDEX i2 ON t1( lower(b) );",
        "SELECT * FROM t1 WHERE upper(a) = 'ABC' OR lower(b) = 'def';"
    ],[
        "CREATE TABLE t1(a INT);",
        "INSERT INTO t1(a) VALUES(1);",
        "CREATE TABLE t2(b INT);",
        "SELECT a, b FROM t1 LEFT JOIN t2 ON true WHERE (b IS NOT NULL) IS false;"
    ],[
        "CREATE TABLE t1(x NOT NULL DEFAULT NULL);",
        "REPLACE INTO t1 DEFAULT VALUES;",
        "SELECT quote(x) FROM t1;"
    ],[
        # no problems here?
        "CREATE TABLE t1(x);",
        "INSERT INTO t1 VALUES('a'), ('b'), ('c');",

        "CREATE TABLE t2(a, b);",
        "INSERT INTO t2 VALUES('X', 1), ('X', 2), ('Y', 2), ('Y', 3);",

        "SELECT x, (SELECT sum(b) OVER (PARTITION BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t2 WHERE b<x) FROM t1;"
    ],[
        "CREATE TABLE t1(aaa, UNIQUE(aaA), UNIQUE(aAa), UNIQUE(aAA), CHECK(Aaa>0));",
        "ALTER TABLE t1 RENAME aaa TO bbb;"
    ],[
        "CREATE TABLE t1(a,b,c);",
        "CREATE INDEX t1bbc ON t1(b, b+c);",
        "INSERT INTO t1(a,b,c) VALUES(1,zeroblob(8),3);",
        "SELECT a, b, length(b), c FROM t1;"
    ],[
        "CREATE TABLE t1(aa, bb);",
        "CREATE INDEX t1x1 on t1(abs(aa), abs(bb));",
        "INSERT INTO t1 VALUES(-2,-3),(+2,-3),(-2,+3),(+2,+3);",
        "SELECT * FROM (t1) WHERE ((abs(aa)=1 AND 1=2) OR abs(aa)=2) AND abs(bb)=3;"
    ],[
        # problem only for 3.27?
        "CREATE TABLE IF NOT EXISTS t1(id INTEGER PRIMARY KEY);",
        "INSERT INTO t1 VALUES(1);",
        "SELECT a.id FROM t1 AS a JOIN t1 AS b ON a.id=b.id WHERE a.id IN (1,2,3);"
    ],[
        # problem only for 3.27?
        "CREATE TABLE t1(a INTEGER PRIMARY KEY);",
        "INSERT INTO t1(a) VALUES(1),(2),(3);",
        "CREATE TABLE t2(x INTEGER PRIMARY KEY, y INT);",
        "INSERT INTO t2(y) VALUES(2),(3);",
        "SELECT * FROM t1, t2 WHERE a=y AND y=3;"
    ],[
        "SELECT +sum(0)OVER() ORDER BY +sum(0)OVER();"
    ]
]

BUGS30 = [
    [
        "CREATE TABLE item (id int, price int);",
        "INSERT INTO item (id, price) VALUES (1, 1);",
        "SELECT COUNT(id) FILTER (WHERE double_price > 42) FROM (SELECT id, (price * 2) as double_price FROM item);"
    ],[
        "CREATE TABLE t1(a, b);",
        "CREATE TABLE t2(c, d);",
        "CREATE TABLE t3(e, f);",

        "INSERT INTO t1 VALUES(1, 1);",
        "INSERT INTO t2 VALUES(1, 1);",
        "INSERT INTO t3 VALUES(1, 1);",

        "SELECT d IN (SELECT sum(c) OVER (ORDER BY e+c) FROM t3) FROM (SELECT * FROM t2);"
    ]
]

BUGS31 = [
    [
        "CREATE TABLE t0(c0 AS(1), c1);",
        "PRAGMA legacy_file_format = true;",
        "CREATE INDEX i0 ON t0(0 DESC);",
        "VACUUM;"
    ]
]

BUGS32 = [
    [
        "CREATE TABLE t1(x INTEGER CHECK(typeof(x)=='text'));",
        "INSERT INTO t1 VALUES('123');",
        "PRAGMA integrity_check;"
    ],[
        "CREATE TABLE t2(x INT CHECK(typeof(x)=='integer'));",
        "INSERT INTO t2(x) VALUES('123');"
    ],[
        "CREATE TABLE t1(c1);     INSERT INTO t1 VALUES(12),(123),(1234),(NULL),('abc');",
        "CREATE TABLE t2(c2);     INSERT INTO t2 VALUES(44),(55),(123);",
        "CREATE TABLE t3(c3,c4);  INSERT INTO t3 VALUES(66,1),(123,2),(77,3);",
        "CREATE VIEW t5 AS SELECT c3 FROM t3 ORDER BY c4;",
        "SELECT * FROM t1, t2 WHERE c1=(SELECT 123 INTERSECT SELECT c2 FROM t5) AND c1=123;"
    ],[
        # no bug?
        "CREATE TABLE a(b TEXT);  INSERT INTO a VALUES(0),(4),(9);",
        "CREATE TABLE c(d NUM);",
        "CREATE VIEW f(g, h) AS SELECT b, 0 FROM a UNION SELECT d, d FROM c;",
        "SELECT g = g FROM f GROUP BY h;"
    ],[
        "CREATE TABLE v0 ( v1 );",
        "CREATE INDEX v2 ON v0 ( v1 , v1 );",
        "ALTER TABLE zipfile RENAME TO x ; DROP TRIGGER IF EXISTS x;",
        "WITH v2 AS ( SELECT v1 FROM v0 WHERE 0 ) SELECT ( SELECT v1 FROM v2 UNION ALL SELECT rank () OVER( PARTITION BY ( SELECT max ( ( SELECT count () FROM v0 JOIN v2 AS b ON v2 . v1 = v0 . v1 ) ) FROM v2 GROUP BY v1 HAVING v1 < 'MED PKG' ) ORDER BY ( SELECT v1 FROM v2 NATURAL JOIN v2 WHERE v1 = v1 AND v1 > 3 GROUP BY v1 ) ) FROM v2 ORDER BY v1 ASC , v1 ASC LIMIT 2 ) FROM v2 WHERE v1 IN ( 10 , NULL ) ;"
    ],[
        "CREATE TABLE a(b);",
        "SELECT SUM(0) OVER(ORDER BY(SELECT max(b) OVER(PARTITION BY SUM((SELECT b FROM a UNION SELECT b ORDER BY b))) INTERSECT SELECT b FROM a ORDER BY b)) FROM a;"
    ],[
        # no bug?
        "CREATE TABLE a(b);",
        "WITH c AS(SELECT a) SELECT(SELECT(SELECT zipfile(0, b, b) LIMIT(SELECT 0.100000 * AVG(DISTINCT(SELECT 0 FROM a ORDER BY b, b, b)))) FROM a GROUP BY b, b, b) FROM a EXCEPT SELECT b FROM a ORDER BY b, b, b;"
    ],[
        # no bug?
        "CREATE TABLE a(b);",
        "SELECT(SELECT b FROM a GROUP BY b HAVING(NULL AND b IN((SELECT COUNT() OVER(ORDER BY b) = lead(b) OVER(ORDER BY 3.100000 * SUM(DISTINCT CASE WHEN b LIKE 'SM PACK' THEN b * b ELSE 0 END) / b))))) FROM a EXCEPT SELECT b FROM a ORDER BY b, b, b;"
    ]
]

BUGS35 = [
    [
        "CREATE TABLE t1(a, b AS (datetime()));",
        "CREATE TABLE t2(x CHECK( x<julianday() ));"
    ],[
        "WITH xyz(a) AS (WITH abc AS ( SELECT 1234 ) SELECT * FROM abc) SELECT * FROM xyz AS one, xyz AS two, (SELECT * FROM xyz UNION ALL SELECT * FROM xyz);"
    ],[
        "CREATE TABLE t3(a TEXT PRIMARY KEY, b TEXT, x INT) WITHOUT ROWID;",
        "CREATE TABLE t4(c TEXT COLLATE nocase, y INT);",
        "INSERT INTO t3 VALUES('one', 'i', 1);",
        "INSERT INTO t3 VALUES('two', 'ii', 2);",
        "INSERT INTO t3 VALUES('three', 'iii', 3);",
        "INSERT INTO t3 VALUES('four', 'iv', 4);",
        "INSERT INTO t3 VALUES('five', 'v', 5);",
        "INSERT INTO t4 VALUES('FIVE',5), ('four',4), ('TWO',2), ('one',1);",
        "SELECT a FROM t3 WHERE EXISTS (SELECT 1 FROM t4 WHERE (a,x)=(c,y) LIMIT 1);"
    ],[
        "CREATE TABLE t1(x);",
        "INSERT INTO t1 VALUE(1) RETURNING t1.*;"
    ]
]

BUGS38 = [
    [
        "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT);",
        "CREATE TABLE t2(c INTEGER PRIMARY KEY, d INT);",
        "WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<100)",
        "INSERT INTO t1(a,b) SELECT x, 1000*x FROM c;",
        "INSERT INTO t2(c,d) SELECT b*2, 1000000*a FROM t1;",
        "ANALYZE;",
        "DELETE FROM sqlite_stat1;",
        "INSERT INTO sqlite_stat1(tbl,idx,stat) VALUES('t1',NULL,150105),('t2',NULL,98747);",
        "ANALYZE sqlite_schema;",
        #".eqp on",
        "SELECT count(*) FROM t1 LEFT JOIN t2 ON c=b WHERE c IS NULL;",
        "SELECT count(*) FROM t1 LEFT JOIN t2 ON +c=+b WHERE +c IS NULL;",
        #".testctrl optimizations 0x80000",
        "SELECT count(*) FROM t1 LEFT JOIN t2 ON c=b WHERE c IS NULL;"
    ],[
        "CREATE TABLE t1(a INT, b INT);",
        "CREATE TABLE t2(c INT, d INT);",
        "CREATE TABLE t3(e TEXT, f TEXT);",
        "INSERT INTO t1 VALUES(1, 1);",
        "INSERT INTO t2 VALUES(1, 2);",
        "SELECT * FROM t1 JOIN t2 ON (t2.c=t1.a) LEFT JOIN t3 ON (t2.d=1)"
    ],[
        "CREATE TABLE t1(a INT, b INT, c INT);",
        "WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<100) INSERT INTO t1(a,b,c) SELECT x, x*1000, x*1000000 FROM c;",
        "CREATE TABLE t2(b INT, x INT);",
        "INSERT INTO t2(b,x) SELECT b, a FROM t1 WHERE a%3==0;",
        "CREATE INDEX t2b ON t2(b);",
        "CREATE TABLE t3(c INT, y INT);",
        "INSERT INTO t3(c,y) SELECT c, a FROM t1 WHERE a%4==0;",
        "CREATE INDEX t3c ON t3(c);",
        "INSERT INTO t1(a,b,c) VALUES(200, 200000, NULL);",
        "ANALYZE;",
        "SELECT * FROM t1 NATURAL JOIN t2 NATURAL JOIN t3 WHERE x>0 AND y>0;"
    ]
]

BUGS39 = [
    [
        # RIGHT and FULL OUTER JOINs not supported by 3.26.0
        "CREATE TABLE t1(a INT);",
        "CREATE TABLE t2(b INT, c INT);",
        "CREATE VIEW t3(d) AS SELECT NULL FROM t2 FULL OUTER JOIN t1 ON c=a UNION ALL SELECT b FROM t2;",
        "INSERT INTO t1(a) VALUES (NULL);",
        "INSERT INTO t2(b, c) VALUES (99, NULL);",
        "SELECT DISTINCT * FROM t2, t3 WHERE b<>0 UNION SELECT DISTINCT * FROM t2, t3 WHERE b ISNULL;"
        # correct: two rows of results => correct for 3.39.4
    ],[
        # only comes up if you compile with -DSQLITE_ENABLE_STAT4
        "CREATE TABLE t1(a INT, b INT PRIMARY KEY) WITHOUT ROWID;",
        "INSERT INTO t1(a, b) VALUES (0, 1),(15,-7),(3,100);",
        "ANALYZE;",
        "SELECT * FROM t1 WHERE (b,a) BETWEEN (0,5) AND (99,-2);"
    ],[
        "CREATE TABLE v0 (c1 INT);",
        "BEGIN;",
        "INSERT INTO v0 (c1) VALUES (0), (nth_value (0, -1) OVER());",
        "SELECT * FROM v0;"
    ]
]

BUGS = BUGS27 + BUGS30 + BUGS31 + BUGS32 + BUGS35 + BUGS38 + BUGS39