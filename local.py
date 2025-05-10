import subprocess
import logging
from metric import get_coverage

LOCAL = True

def coverage_test(sql_query):
    """
    Local version of coverage_test (same output format)
    Reset:
        rm -f test.db
        find . -name '*.gcda' -delete
        find . -name '*.gcov' -delete
    """
    commands = [f'./sqlite3 test.db "{query}"' for query in sql_query]
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
        return get_coverage(result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running query: {e.stderr}")
        return (0, str(e.stderr))

def reset():
    work_dir = "/home/test/sqlite"

    reset_cmds = [
        "rm -f test.db", 
        "find . -name '*.gcda' -delete",  
        "find . -name '*.gcov' -delete" 
    ]

    command_str = " ; ".join(reset_cmds)

    try:
        result = subprocess.run(
            ["bash", "-c", command_str],
            cwd=work_dir,
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