import subprocess
import logging
from metric import get_coverage, sql_cleaner
from pathlib import Path
from config import TEST_FOLDER, RESULT_FOLDER, SQLITE_VERSIONS
import os

LOCAL = True

def run_coverage(sql_query, db="test.db", timeout=1):
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
    reset_cmd = [
        "rm -f test.db", 
        "find . -name '*.gcda' -delete",  
        "find . -name '*.gcov' -delete" 
    ]
    command_str = " ; ".join(reset_cmd)

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
    
def run_query(cmd):
    try:
        result = subprocess.run(
            ["bash", "-c", cmd],
            cwd="/usr/bin/test-db",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        return result.stdout.strip(), ""
    except subprocess.CalledProcessError as e:
        return "", e.stderr.strip()

def log_output(filepath, query, output):
    with open(filepath, "a") as f:
        f.write(query + "\n")
        if output:
            for line in output.splitlines():
                f.write(f"-- {line}\n")
    
def run_test(queries, name):
    reset_db() # resets the database

    db1 = "test1.db"
    db2 = "test2.db"

    file1 = os.path.join(RESULT_FOLDER, f"{name}_{SQLITE_VERSIONS[0]}.sql")
    file2 = os.path.join(RESULT_FOLDER, f"{name}_{SQLITE_VERSIONS[1]}.sql")
    file_diff = os.path.join(RESULT_FOLDER, f"{name}_diff.txt")

    bugs = 0

    for query in queries:
        cmd1 = f"/usr/bin/{SQLITE_VERSIONS[0]} {db1} \"{query}\""
        cmd2 = f"/usr/bin/{SQLITE_VERSIONS[1]} {db2} \"{query}\""

        out1, err1 = run_query(cmd1)
        out2, err2 = run_query(cmd2)

        log_output(file1, query, err1 or out1)
        log_output(file2, query, err2 or out2)

        # log different outputs
        if not err1 and not err2 and out1 != out2:
            with open(file_diff, "a") as f:
                f.write(query + "\n")
                f.write(f"{SQLITE_VERSIONS[0]} Output:\n")
                for line in out1.splitlines():
                    f.write(f"-- {line}\n")
                f.write("\n")
                f.write(f"{SQLITE_VERSIONS[1]} Output:\n")
                for line in out2.splitlines():
                    f.write(f"-- {line}\n")
            bugs += 1

    if bugs == 0:
        for f in [file1, file2]:
            if os.path.exists(f):
                os.remove(f)
    else:
        print(f"Bug found in {name}.sql")

def reset_db():
    for db in ["test.db", "test1.db", "test2.db"]:
        if os.path.exists(db):
            os.remove(db)
    
if __name__ == "__main__":
    reset()

    sql_folder = Path(TEST_FOLDER)
    for i, sql_file in enumerate(sql_folder.glob('*.sql')):
        with sql_file.open('r', encoding='utf-8') as f:
            query = sql_cleaner(f.read())
            run_test(query, sql_file.stem)
    
    
