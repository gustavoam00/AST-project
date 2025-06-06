import subprocess
from .helper import get_coverage, sql_cleaner
from pathlib import Path
from .config import QUERY_TEST_FOLDER, SQLITE_VERSIONS, DB1, DB2, BUGS_FOLDER
from tqdm import tqdm
import os, argparse


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
                return "", "Segmentation fault (core dumped)"
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

def run_test2(queries: list[str], path: str, oracle: str, full: bool = False) -> tuple[int, list[str], tuple[str, str]]:
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

    file1 = os.path.join(BUGS_FOLDER, f"{path}_{SQLITE_VERSIONS[0]}.sql")
    file2 = os.path.join(BUGS_FOLDER, f"{path}_{SQLITE_VERSIONS[1]}.sql")
    file_diff = os.path.join(BUGS_FOLDER, f"{path}_diff.txt")

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

    if oracle == "DIFF":
        out1 = out2 = err1 = err2 = None

        # TODO: group queries with no output?
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

    # TODO: group queries?
    elif oracle == "CRASH(3.26.0)":
        for i, query in enumerate(queries):     
            cmd1 = f"/usr/bin/{v1} {DB1} \"{query}\""
            out1, err1 = run_query(cmd1)
            log_output(file1, query, err1 or out1)

            if err1:
                bugs += 1
                msg = (err1, "")
                return i, errlist, msg

    elif oracle == "CRASH(3.39.4)":
        for i, query in enumerate(queries):  
            cmd2 = f"/usr/bin/{v2} {DB2} \"{query}\""
            out2, err2 = run_query(cmd2)
            log_output(file2, query, err2 or out2)

            if err2:
                bugs += 1
                msg = ("", err2)
                return i, errlist, msg

        else:
            raise ValueError(f"Unknown oracle: {oracle}")

    return len(queries) - 1, errlist, msg

    
def run_test(queries, name):
    reset_db() # resets the database

    file1 = os.path.join(BUGS_FOLDER, f"{name}_{SQLITE_VERSIONS[0]}.sql")
    file2 = os.path.join(BUGS_FOLDER, f"{name}_{SQLITE_VERSIONS[1]}.sql")
    file_diff = os.path.join(BUGS_FOLDER, f"{name}_diff.txt")

    for f in [file1, file2, file_diff]:
        if os.path.exists(f):
            os.remove(f)

    bugs = 0

    r1 = 0
    r2 = 0

    errlist = []
    msg = ("", "")

    for i, query in enumerate(queries):
        cmd1 = f"/usr/bin/{SQLITE_VERSIONS[0]} {DB1} \"{query}\""
        cmd2 = f"/usr/bin/{SQLITE_VERSIONS[1]} {DB2} \"{query}\""

        out1, err1 = run_query(cmd1)
        out2, err2 = run_query(cmd2)

        #print(SQLITE_VERSIONS[0], out1, err1)
        #print(SQLITE_VERSIONS[1], out2, err2)

        if err1:
            r1 += 1
        if err2:
            r2 += 1

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
            msg = (out1, out2)

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

        if bugs > 0 or r1 != r2:
            return i, errlist, msg
        elif err1 or err2:
            errlist.append(query)
        
    return len(queries)-1, errlist, msg

def reset_db():
    for db in [DB1, DB2]:
        if os.path.exists(db):
            os.remove(db)

def debugging_f(queries: list[str], oracle: str, msg: tuple[str, str], name: str="test2.sql") -> bool:
    if oracle == "DIFF":
        new_msg = run_test2(queries, name, oracle, full=True)[2]
        return new_msg[0] != new_msg[1]
    elif oracle == "CRASH(3.26.0)":
        return msg[0] == run_test2(queries, name, oracle, full=True)[2][0]
    else:
        return msg[1] == run_test2(queries, name, oracle, full=True)[2][1]

def main(args=None):
    from .vertical_reduction import trace_context, delta_debug
    for q in range(20):
        if q == 19:
            path = QUERY_TEST_FOLDER + f"query{q+1}/"
            sql_folder = Path(path)

            # query1: 2 -> 2 -> 2
            # query2: 18 -> 7 -> 3
            # query3: 97 -> 36 -> 36
            # query4: 42 -> 9 -> 4
            # query5: 4 -> 3 -> 3
            # query6: 16 -> 2 -> 2
            # query7: 32 -> 2 -> 2
            # query8: 41 -> 6 -> 4
            # query9: 30 -> 1 -> 1
            # query10: 8 -> 5 -> 5
            # query11: 4 -> 4 -> 3
            # query12: 190 -> 22 -> 4
            # query13: 13 -> 4 -> 4
            # query14: 534 -> 161 -> 50
            # query15: 13 -> 13 -> 6
            # query16: 2 -> 2 -> 2
            # query17: 64 -> 1 -> 1
            # query18: 2, 2, 2
            # query19: 4, 4, 4
            # query20: 16, 2, 2

            with open(path + "oracle.txt") as f:
                oracle = str(f.read().strip())

            sql_files = list(sql_folder.glob('*.sql'))  
            for i, sql_file in enumerate(tqdm(sql_files, desc="Testing for bugs")):
                with sql_file.open('r', encoding='utf-8') as f:
                    query  = f.read().rstrip()
                    raw_queries = query.split(';')

                    queries: list[str] = []
                    for query in raw_queries:
                        cleaned = ' '.join(query.strip().split())
                        if cleaned: 
                            queries.append(cleaned + ';')  

                    # original query
                    index, errlist, msg = run_test2(queries, sql_file.stem, oracle)
                    test1 = len(queries)

                    # remove queries via context
                    setup_queries, context_queries, error_query = trace_context(queries, index, errlist, msg)
                    run_test2(setup_queries + context_queries + error_query, "test_" + sql_file.stem, oracle)
                    test2 = len(setup_queries + context_queries + error_query)

                    # delta debug
                    if len(queries) > 50:
                        delta_queries = setup_queries + delta_debug(setup_queries, context_queries, error_query, lambda x, y=oracle, z=msg, name="test2_" + sql_file.stem: debugging_f(x, y, z, name), n=2) + error_query
                    else:
                        delta_queries = delta_debug([], setup_queries + context_queries + error_query, [], lambda x, y=oracle, z=msg, name="test2_" + sql_file.stem: debugging_f(x, y, z, name), n=2)
                    index, errlist, msg = run_test2(delta_queries, "test2_" + sql_file.stem, oracle)
                    test3 = len(delta_queries)

                    with open(QUERY_TEST_FOLDER + "test/result.txt", "a") as f:
                        f.write(f"query{q+1}: {test1}, {test2}, {test3}\n")


if __name__ == "__main__":
    main()

    

    
    
