import subprocess
import logging
from metric import get_coverage, coverage_score, save_error, get_error, sql_cleaner, remove_lines, remove_common_lines

LOCAL = True
SQLITE_VERSIONS = ["sqlite3-3.26.0", "sqlite3-3.39.4"]

def coverage_test(sql_query, db="test.db", timeout=1):
    """
    Local version of coverage_test
    """
    commands = [f'./sqlite3 {db} "{query}"' for query in sql_query]
    commands.append("gcov -b -o . sqlite3-sqlite3.c")
    command_str = " ; ".join(commands)

    try:
        result = subprocess.run(
            ["bash", "-c", command_str],
            cwd="/home/test/sqlite",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
            timeout=timeout
        )
        return get_coverage(result.stderr + "\n" + result.stdout)
    except subprocess.TimeoutExpired:
        return 0, 0, 0, 0, "Error: Timeout"
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running query: {e.stderr}")
        return 0, 0, 0, 0, str(e.stderr)

def reset():
    reset_cmds = [
        "rm -f test.db", 
        "find . -name '*.gcda' -delete",  
        "find . -name '*.gcov' -delete" 
    ]

    command_str = " ; ".join(reset_cmds)

    try:
        result = subprocess.run(
            ["bash", "-c", command_str],
            cwd="/home/test/sqlite",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        return get_coverage(result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running query: {e.stderr}")
        return 0, 0, 0, 0, str(e.stderr)
    
def run_query(sql_query, sqlite_version, db="test.db"):
    """
    Executes an SQL query using the specified SQLite version inside a Docker container.
    """
    commands = [f'/usr/bin/{sqlite_version} {db} "{query}"' for query in sql_query]
    command_str = " ; ".join(commands)
    try:
        result = subprocess.run(
            ["bash", "-c", command_str],
            cwd="/usr/bin/test-db",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        return result.stdout + "\n" + get_error(result.stderr)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running query: {e.stderr}")
        return str(e.stderr)

def test(query):
    """
    Compares query results between two SQLite versions.
    """
    results = []

    reset_database()
    results.append(run_query(query, SQLITE_VERSIONS[0]))

    reset_database()
    results.append(run_query(query, SQLITE_VERSIONS[1]))

    for i, ver in enumerate(SQLITE_VERSIONS):
        with open(f"test/bug/{ver}.txt", "w") as f:
            f.write(results[i])

    if results[0] != results[1]:
        logging.warning("Bug found!")
        #logging.info(res1)
    else:
        logging.info("No bug detected.")

def reset_database(db_path="test.db"):
    with open(db_path, "w") as f:
        f.write("")
    
if __name__ == "__main__":
    reset()
    d = "test/results/_query_52.9500.sql"
    with open(d, "r") as f:
        sql = f.read()
        FULL = sql_cleaner(sql)
        
    '''
    [stmt.strip() + ";" for stmt in sql.split(";") if stmt.strip() 
    and "EXPLAIN" not in stmt and "dbstat" not in stmt
    and "date" not in stmt and "time" not in stmt 
    and "PRAGMA" not in stmt]

    out = "test/bug/bug_query_test.sql"
    with open(out, "w") as f:
        for query in FULL:
            if "ANALYZE" not in query and "VACUUM" not in query and "REINDEX" not in query:
                f.write(query + "\n")
    
    lines_c, branch_c, taken_c, calls_c, msg = coverage_test(FULL, timeout=None)
    combined_cov = coverage_score(lines_c, branch_c, taken_c, calls_c)

    save_error(msg, "test/error/error_local.txt")

    a, b, c, d, _ = get_coverage(msg)
    print(a, b, c, d)
    '''
    test(FULL)
    file1 = "test/bug/sqlite3-3.26.0.txt"
    file2 = "test/bug/sqlite3-3.39.4.txt"
    out = "test/bug/result.txt"
    remove_lines(file1, file2, out)
    out1 = "test/bug/result1.txt"
    out2 = "test/bug/result2.txt"
    remove_common_lines(file1, file2, out1, out2)
    
    
