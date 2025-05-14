import docker
import logging
from tqdm import tqdm
from helper.helper import get_coverage, coverage_score, save_error
from helper.metric import extract_metric

#logging.disable(logging.INFO) 
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

LOCAL = False
SQLITE_VERSIONS = ["sqlite3-3.26.0", "sqlite3-3.39.4"]
DOCKER_IMAGE = "sqlite3-fuzzing" #"theosotr/sqlite3-test"

def run_coverage(sql_query, db="test.db", timeout=1):
    """
    Test coverage for sqlite3-3.26.0
    """
    commands = [f'./sqlite3 {db} "{query}"' for query in sql_query]
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

def run_query(sql_query, sqlite_version, db="test.db"):
    """
    Executes an SQL query using the specified SQLite version inside a Docker container.
    """
    commands = [f'./{sqlite_version} {db} "{query}"' for query in sql_query]
    command_str = " ; ".join(commands)
    client = docker.from_env()
    try:
        result = client.containers.run(
            DOCKER_IMAGE,
            command=["bash", "-c", command_str], #f'/bin/bash -c "echo \\"{sql_script}\\" | /usr/bin/{sqlite_version}"',
            working_dir="/usr/bin",
            stdout = True,
            stderr = True,
            remove=True,
            tty=True
        )
        return result.decode()
    except Exception as e:
        #logging.error(f"{sqlite_version}: {e}")
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
        #logging.info(res1)
    else:
        logging.info("No bug detected.")

if __name__ == "__main__":
    SQL_TEST_QUERY = [
        "CREATE TABLE t0(c0 INT);",
        "CREATE INDEX i0 ON t0(1) WHERE c0 NOT NULL;",
        "INSERT INTO t0 (c0) VALUES (0), (1), (2), (NULL), (3);",
        "SELECT c0 FROM t0 WHERE t0.c0 IS NOT 1;",
    ]
    d = "test/best/_query_52.895.sql"
    d = "test/query_test.sql"
    d = "test/results/_query_52.9500.sql"
    with open(d, "r") as f:
        sql = f.read()
        FULL = [stmt.strip() + ";" for stmt in sql.split(";") if stmt.strip()]

    lines_c, branch_c, taken_c, calls_c, msg = run_coverage(FULL)
    combined_cov = coverage_score(lines_c, branch_c, taken_c, calls_c)
    
    save_error(msg, "test/error/error_test.txt")

    test(FULL[:3])

    a, b, c, d, _ = get_coverage(msg)
    print(a, b, c, d)


        





