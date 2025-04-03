import docker
import logging
from bugs import BUGS
from tqdm import tqdm
from query_generator import SQLiteTable, SQLiteQuery

logging.disable(logging.INFO) 
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

SQLITE_VERSIONS = ["sqlite3-3.26.0", "sqlite3-3.39.4"]
DOCKER_IMAGE = "sqlite3-test"

def run_query(sql_query, sqlite_version):
    """
    Executes an SQL query using the specified SQLite version inside a Docker container.
    """
    sql_script = " ".join(sql_query)
    client = docker.from_env()
    try:
        result = client.containers.run(
            DOCKER_IMAGE,
            command=f'/bin/bash -c "echo \\"{sql_script}\\" | /usr/bin/{sqlite_version}"',
            remove=True
        )
        return "  " + result.decode().strip().replace("\n", "\n  ")
    except Exception as e:
        logging.error(f"{sqlite_version}: {e}")
        return None

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
        logging.info("Query:\n  " + "\n  ".join(SQL_TEST_QUERY))
    else:
        logging.info("No bug detected.")


if __name__ == "__main__":
    SQL_TEST_QUERY = [
        "CREATE TABLE t0(c0 INT);",
        "CREATE INDEX i0 ON t0(1) WHERE c0 NOT NULL;",
        "INSERT INTO t0 (c0) VALUES (0), (1), (2), (NULL), (3);",
        "SELECT c0 FROM t0 WHERE t0.c0 IS NOT 1;"
    ]
    # test(SQL_TEST_QUERY)

    # BUGS[0:34]
    # test(BUGS[0])

    table_name = "test1"
    table = SQLiteTable(table_name)
    table.create(rows=100, max_cols=1)
    script = table.get_script()
    script_len = len(script)

    tables_cols = {}
    tables_cols[table_name] = table.get_cols()
    query_gen = SQLiteQuery([table_name], tables_cols)
    
    for i in tqdm(range(100000)):
        query = query_gen.select(table_name, size=2)
        script.append(query)
        if i % 500 == 0:
            test(script) #no bugs found
            script = script[:script_len]
        





