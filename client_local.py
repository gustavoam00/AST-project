import subprocess
import logging
from metric import get_coverage, coverage_score, save_error, get_error, sql_cleaner, remove_lines, remove_common_lines
from pathlib import Path

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
    except subprocess.TimeoutExpired as e:
        return 0, 0, 0, 0, str(e.stderr)
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
        return result.stdout, get_error(result.stderr)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running query: {e.stderr}")
        return str(e.stderr)

def test(query, it: int = 0):
    """
    Compares query results between two SQLite versions.
    """
    results = []

    reset_database()
    out1, err1 = run_query(query, SQLITE_VERSIONS[0])
    results.append(out1)

    reset_database()
    out2, err2 = run_query(query, SQLITE_VERSIONS[1])
    results.append(out2)

    for i, ver in enumerate(SQLITE_VERSIONS):
        with open(f"test/{ver}.txt", "w") as f:
            f.write(results[i])

    file1 = "test/bug/sqlite3-3.26.0.txt"
    file2 = "test/bug/sqlite3-3.39.4.txt"
    out = "test/bug/result.txt"
    result = remove_lines(file1, file2, out)
    out1 = "test/bug/result1.txt"
    out2 = "test/bug/result2.txt"
    result1, result2 = remove_common_lines(file1, file2, out1, out2)

    if result1 != result2:
        logging.warning("Bug found!")
        with open(f"test/bug/{it}_1.txt", "w") as f:
            f.writelines(result1)
        with open(f"test/bug/{it}_2.txt", "w") as f:
            f.writelines(result2)
    if result:
        logging.warning("Maybe Bug found!")
        with open(f"test/bug/{it}_r.txt", "w") as f:
            f.writelines(result)

def reset_database(db_path="test.db"):
    with open(db_path, "w") as f:
        f.write("")
    
if __name__ == "__main__":
    reset()

    sql_folder = Path('test')
    for i, sql_file in enumerate(sql_folder.glob('*.sql')):
        with sql_file.open('r', encoding='utf-8') as f:
            query = sql_cleaner(f.read())
            test(query, i)
    
    
