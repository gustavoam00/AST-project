import docker
import logging
from bugs import BUGS
from tqdm import tqdm
import generator as gen
from metric import get_coverage, metric, coverage_score, save_error

logging.disable(logging.ERROR) 
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

LOCAL = False
SQLITE_VERSIONS = ["sqlite3-3.26.0", "sqlite3-3.39.4"]
DOCKER_IMAGE = "theosotr/sqlite3-test"

def coverage_test(sql_query, db="test.db"):
    """
    Test coverage for sqlite3-3.26.0
    """
    commands = [f"./sqlite3 {db} \"{query}\"" for query in sql_query]
    commands.append("gcov -b -o . sqlite3-sqlite3.c") # gcov to get coverage
    command_str = " ; ".join(commands)
    client = docker.from_env()
    try:
        result = client.containers.run(
            DOCKER_IMAGE,
            command=["bash", "-c", command_str],
            working_dir="/home/test/sqlite",
            remove=True,
            tty=True
        )
        return get_coverage(result.decode())
    except Exception as e:
        logging.error(f"Error running query: {e}")
        return (0, str(e))

def reset():
    # does nothing, just here because fuzzing calls reset() to reset the docker if local.py is used
    return 0 

def run_query(sql_query, sqlite_version):
    """
    Executes an SQL query using the specified SQLite version inside a Docker container.
    """
    sql_script = " ; ".join(sql_query)
    client = docker.from_env()
    try:
        result = client.containers.run(
            DOCKER_IMAGE,
            command=f'/bin/bash -c "echo \\"{sql_script}\\" | /usr/bin/{sqlite_version}"',
            working_dir="/home/test/sqlite",
            remove=True
        )
        return "  " + result.decode().strip().replace("\n", "\n  ")
    except Exception as e:
        logging.error(f"{sqlite_version}: {e}")
        return str(e)

def test(query):
    """
    Compares query results between two SQLite versions.
    """
    res1 = run_query(query, SQLITE_VERSIONS[0])
    res2 = run_query(query, SQLITE_VERSIONS[1])

    logging.info(f"Version ({SQLITE_VERSIONS[0]}) - Table Result:\n{res1}")
    logging.info(f"Version ({SQLITE_VERSIONS[1]}) - Table Result:\n{res2}")

    if res1 != res2:
        logging.warning("Bug found!")
        logging.info("Query:\n  " + "\n  ".join(query))
    else:
        logging.info("No bug detected.")

import random
import re

SQL_KEYWORDS = ["SELECT", "FROM", "WHERE", "ORDER BY", "GROUP BY", "JOIN", 
                "LEFT JOIN", "INNER JOIN", "LIMIT", "OFFSET", "CREATE", "TABLE", 
                "VIRTUAL", "VIEW", "BETWEEN", "AS", "IN", "LIKE", "AND", "OR",
                "MATCH", "EXISTS", "EXPLAIN", "BEGIN", "END", "COMMIT", "ROLLBACK",
                "IS", "NOT", "NULL", "CASE", "WHEN", "THEN", "ELSE", "RENAME", "COLUMN",
                "VALUES", "TO", "INSERT", "INTO", "UPDATE", "DELETE", "DEFAULT", "SET"
                "REPLACE", "WITH", "USING", "INDEX", "ON", "UNIQUE", "TRIGGER",
                "BEFORE", "AFTER", "TEMP", "IF", "FOR", "EACH", "ROW", "PRAGMA"]
OPERATORS = ["=", "!=", "<", ">", "<=", ">=", "LIKE"]

def mutate_sql(sql: str) -> str:
    mutations = [mutate_keyword, mutate_constant, mutate_operator, mutate_structure]
    mutation = random.choice(mutations)
    return mutation(sql)

def mutate_keyword(sql: str) -> str:
    if any(kw in sql.upper() for kw in SQL_KEYWORDS):
        old_kw = random.choice([kw for kw in SQL_KEYWORDS if kw in sql.upper()])
        new_kw = random.choice(SQL_KEYWORDS)
        return re.sub(old_kw, new_kw, sql, flags=re.IGNORECASE)
    return sql

def mutate_constant(sql: str) -> str:
    return re.sub(r"(?<!\w)(\d+)(?!\w)", lambda m: str(int(m.group(0)) + random.randint(-5, 5)), sql)

def mutate_operator(sql: str) -> str:
    for op in OPERATORS:
        if op in sql:
            new_op = random.choice(OPERATORS)
            return sql.replace(op, new_op, 1)
    return sql

def mutate_structure(sql: str) -> str:
    # Randomly insert a LIMIT clause
    if "LIMIT" not in sql.upper():
        return sql.strip().rstrip(";") + f" LIMIT {random.randint(1, 100)};"
    return sql

if __name__ == "__main__":
    SQL_TEST_QUERY = [
        "CREATE TABLE t0(c0 INT);",
        "CREATE INDEX i0 ON t0(1) WHERE c0 NOT NULL;",
        "INSERT INTO t0 (c0) VALUES (0), (1), (2), (NULL), (3);",
        "SELECT c0 FROM t0 WHERE t0.c0 IS NOT 1;",
    ]
    d = "test/best/_query_52.895.sql"
    d = "test/query_test.sql"
    with open(d, "r") as f:
        sql = f.read()
        FULL = [stmt.strip() + ";" for stmt in sql.split(";") if stmt.strip()]

    #q = random.choice(FULL)
    #print(q)
    #print(mutate_sql(q))

    lines_c, branch_c, taken_c, calls_c, msg = coverage_test(FULL)
    combined_cov = coverage_score(lines_c, branch_c, taken_c, calls_c)
    
    save_error(msg, "test/error/error_test.txt")

    #test(SQL_TEST_QUERY)
    a, b, c, d, _ = get_coverage(msg)
    print(a, b, c, d)
    #print(metric(SQL_TEST_QUERY))
    assert False
    BUGS[0:34]
    #test(BUGS[0])

    error = "constraint"
    runs = 0
    pbar = tqdm(total = runs+1)
    while "constraint" in error or not error.strip() or not "Error" in error or "no such column" in error:
        query = gen.randomQueryGen(prob, cycle=1)
        error = run_query([query], SQLITE_VERSIONS[0])
        if not "Error" in error:
            print(coverage_test([query]))
        pbar.update(1)
    print(error)
    pbar.close()

    assert False
    error = "constraint"
    runs = 0
    pbar = tqdm(total = runs+1)
    while "constraint" in error or not error.strip() or not "Error" in error:
        test_query = ""
        table = gen.Table.random()
        test_query += table.sql() + " "
        insert = gen.Insert.random(table)
        test_query += insert.sql() + " "
        update = gen.Update.random(table)
        test_query += update.sql() + " "
        delete = gen.Delete.random(table)
        test_query += delete.sql() + " "
        table = gen.AlterTable.random_tbl_rename(table)
        test_query += table.sql() + " "
        table = gen.AlterTable.random_add(table)
        test_query += table.sql() + " "
        table = gen.AlterTable.random_col_rename(table)
        test_query += table.sql() + " "
        test_query += gen.Select.random(table).sql() + "; "
        test_query += gen.With.random(table).sql() + " "
        table = gen.View.random(table)
        test_query += table.sql() + " "
        table2 = gen.Table.random()
        test_query += table2.sql() + " "
        test_query += gen.Select.random(table, other_tables=[table2]).sql() + "; "
        test_query += gen.Trigger.random(table2).sql() + " "
        test_query += gen.Index.random(table2).sql() + " "
        test_query += gen.Replace.random(table2).sql() + " "
        test_query += gen.Pragma.random().sql() + " "
        error = run_query([test_query], SQLITE_VERSIONS[0])
        pbar.update(1)
    print(error)
    pbar.close()

        





