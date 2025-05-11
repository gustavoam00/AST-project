import subprocess
import logging
from metric import get_coverage, coverage_score, save_error

LOCAL = True

def coverage_test(sql_query, db="test.db"):
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
            check=True
        )
        return get_coverage(result.stderr + "\n" + result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running query: {e.stderr}")
        return (0, str(e.stderr))

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
        return (0, str(e.stderr))
    
if __name__ == "__main__":
    reset()
    d = "test/query_test.sql"
    with open(d, "r") as f:
        sql = f.read()
        FULL = [stmt.strip() + ";" for stmt in sql.split(";") if stmt.strip()]

    lines_c, branch_c, taken_c, calls_c, msg = coverage_test(FULL)
    combined_cov = coverage_score(lines_c, branch_c, taken_c, calls_c)

    print(msg)

    save_error(msg, "test/error/error_local.txt")

    a, b, c, d, _ = get_coverage(msg)
    print(a, b, c, d)
