import subprocess, os
from src.config import SQLITE_VERSIONS, DB1, DB2, TEMP_OUTPUT

def run_query(cmd: str) -> tuple[str, str]:
    try:
        result = subprocess.run(
            ["bash", "-c", cmd],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5
        )
        if result.returncode < 0:
            if result.returncode == -11:
                return "", "Error: segmentation fault (core dumped)"
            return "", f"Terminated by signal {result.returncode}"
        return result.stdout.strip(), result.stderr.strip()
    except subprocess.CalledProcessError as e:
        return "", e.stderr.strip()

def log_output(filepath: str, query: str, output: str):
    with open(filepath, "a") as f:
        f.write(query + "\n")
        if output:
            for line in output.splitlines():
                f.write(f"-- {line}\n")

def initial_run_test(queries: list[str], path: str, full: bool = False) -> tuple[int, list[str], tuple[str, str]]:
    """
    INPUT:
        queries: list of queries
        path: path to save
        oracle: CRASH(3.26.0), CRASH(3.39.4), DIFF
        full: minimizes call to log_output (subprocess.run) by joining the query strings into one big string

    OUTPUT:
        i: index number of bug in query list
        errlist: tracks error list that are not the bug (same error for both veresions) to remove them later
        msg: output of each version that triggered the bug    
    """
    reset_db()  # resets the database

    file1 = os.path.join(TEMP_OUTPUT, f"{path}_{SQLITE_VERSIONS[0]}.sql")
    file2 = os.path.join(TEMP_OUTPUT, f"{path}_{SQLITE_VERSIONS[1]}.sql")
    file_diff = os.path.join(TEMP_OUTPUT, f"{path}_diff.txt")

    for f in [file1, file2, file_diff]:
        if os.path.exists(f):
            os.remove(f)

    bugs: int = 0
    errlist: list[str] = []
    msg: tuple[str, str] = ("", "")
    v1 = SQLITE_VERSIONS[0]
    v2 = SQLITE_VERSIONS[1]

    if full:
        queries = [" ".join(queries)]

    out1 = out2 = err1 = err2 = None

    for i, query in enumerate(queries):       
        cmd1 = f"/usr/bin/{v1} {DB1} \"{query}\""
        cmd2 = f"/usr/bin/{v2} {DB2} \"{query}\""
        out1, err1 = run_query(cmd1)
        out2, err2 = run_query(cmd2)

        log_output(file1, query, err1 or out1)
        log_output(file2, query, err2 or out2)

        if not err1 and not err2 and out1 != out2:
            with open(file_diff, "a") as f:
                f.write(query + "\n")
                f.write(f"{v1} Output:\n")
                for line in out1.splitlines():
                    f.write(f"-- {line}\n")
                f.write("\n")
                f.write(f"{v2} Output:\n")
                for line in out2.splitlines():
                    f.write(f"-- {line}\n")
            bugs += 1
            msg = (out1, out2)
            return i, errlist, msg
        elif (err1 or err2) and "NOT NULL constraint failed" not in err1 and "NOT NULL constraint failed" not in err2:
            errlist.append(query)

        if err2 and not err1:
            with open(file_diff, "a") as f:
                f.write(query + "\n")
                f.write(f"{SQLITE_VERSIONS[0]} Output:\n")
                for line in out1.splitlines():
                    f.write(f"-- {line}\n")
                f.write("\n")
                f.write(f"{SQLITE_VERSIONS[1]} Output:\n")
                for line in err2.splitlines():
                    f.write(f"-- {line}\n")
            bugs += 1
            msg = (out1, err2)
            return i, errlist, msg

        if err1 and not err2:
            with open(file_diff, "a") as f:
                f.write(query + "\n")
                f.write(f"{SQLITE_VERSIONS[0]} Output:\n")
                for line in err1.splitlines():
                    f.write(f"-- {line}\n")
                f.write("\n")
                f.write(f"{SQLITE_VERSIONS[1]} Output:\n")
                for line in out2.splitlines():
                    f.write(f"-- {line}\n")
            bugs += 1
            msg = (err1, out2)
            return i, errlist, msg

    return len(queries) - 1, errlist, msg

def reset_db():
    for db in [DB1, DB2]:
        if os.path.exists(db):
            os.remove(db)

    
    
