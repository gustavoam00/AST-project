import subprocess
import logging
from metric import get_coverage, coverage_score, save_error, get_error, sql_cleaner, remove_lines, remove_common_lines
from pathlib import Path
from tqdm import tqdm

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
        #logging.error(f"Error running query: {e.stderr}")
        return "", str(e.stderr)

def test(query, name):
    """
    Compares query results between two SQLite versions.
    """
    results = []
    ERRORS = False

    reset_database()
    out1, err1 = run_query(query, SQLITE_VERSIONS[0])
    results.append(out1)

    reset_database()
    out2, err2 = run_query(query, SQLITE_VERSIONS[1])
    results.append(out2)

    for i, ver in enumerate(SQLITE_VERSIONS):
        with open(f"test/{ver}.txt", "w") as f:
            f.write(results[i])

    out1 = out1.split("\n")
    out2 = out2.split("\n")

    result = remove_lines(out1, out2)
    errors = remove_lines(err1, err2)
    result1, result2 = remove_common_lines(out1, out2)
    errors1, errors2 = remove_common_lines(err1, err2)

    if result1 != result2:
        logging.warning("Bug found!")
        with open(f"test/bug/{name}_1.txt", "w") as f:
            f.writelines(result1)
        with open(f"test/bug/{name}_2.txt", "w") as f:
            f.writelines(result2)
        with open(f"test/bug/{name}_clean.sql", "w") as f:
            f.writelines("\n".join(query))
    if result:
        logging.warning("Maybe Bug found!")
        with open(f"test/bug/{name}_r.txt", "w") as f:
            f.writelines(result)
        with open(f"test/bug/{name}_clean.sql", "w") as f:
            f.writelines("\n".join(query))
    if errors1 != errors2 and ERRORS:
        logging.warning("Error Bug found!")
        with open(f"test/bug/{name}_err1.txt", "w") as f:
            f.writelines(errors1)
        with open(f"test/bug/{name}_err2.txt", "w") as f:
            f.writelines(errors2)
    if errors and ERRORS:
        logging.warning("Maybe Error Bug found!")
        with open(f"test/bug/{name}_errr.txt", "w") as f:
            f.writelines(errors)

def reset_database(db_path="test.db"):
    with open(db_path, "w") as f:
        f.write("")
    
if __name__ == "__main__":
    reset()

    sql_folder = Path('test')
    
    for i, sql_file in tqdm(enumerate(sql_folder.glob('*.sql')), desc="Testing Queries for Bugs"):
        with sql_file.open('r', encoding='utf-8') as f:
            query = sql_cleaner(f.read())
            test(query, sql_file.stem)
    
    
