import subprocess
from src.helper import sql_cleaner
from pathlib import Path
from src.config import BUGS_FOLDER, SQLITE_VERSIONS, DB1, DB2, QUERY_TEST_FOLDER
from tqdm import tqdm
import os, argparse

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
            text=False,
            check=True,
            timeout=timeout
        )
        return get_coverage(result.stderr.decode('utf-8', errors='ignore') + "\n" + result.stdout.decode('utf-8', errors='ignore'))
    except subprocess.TimeoutExpired as e:
        return 0, 0, 0, 0, str(e.stderr)
    except subprocess.CalledProcessError as e:
        return 0, 0, 0, 0, str(e.stderr)

def reset():
    reset_cmd = [
        f"rm -f test.db", 
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
        return 0, 0, 0, 0, str(e.stderr)
    
def run_query(cmd):
    try:
        result = subprocess.run(
            ["bash", "-c", cmd],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        return result.stdout.strip(), ""
    except subprocess.CalledProcessError as e:
        return "", e.stderr.strip()

def run_query2(query, version):
    reset_db()
    cmd = f"/usr/bin/{SQLITE_VERSIONS[version]} {DB1} \"{query}\""
    try:
        result = subprocess.run(
            ["bash", "-c", cmd],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.returncode < 0:
            if result.returncode == -11:
                return "", "Segmentation fault (core dumped)"
            return "", f"Terminated by signal {result.returncode}"

        return result.stdout.strip(), result.stderr.strip()
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

    file1 = os.path.join(BUGS_FOLDER, f"{name}_{SQLITE_VERSIONS[0]}.sql")
    file2 = os.path.join(BUGS_FOLDER, f"{name}_{SQLITE_VERSIONS[1]}.sql")
    file_diff = os.path.join(BUGS_FOLDER, f"{name}_diff.txt")

    bugs = 0

    r1 = 0
    r2 = 0

    for query in queries:
        cmd1 = f"/usr/bin/{SQLITE_VERSIONS[0]} {DB1} \"{query}\""
        cmd2 = f"/usr/bin/{SQLITE_VERSIONS[1]} {DB2} \"{query}\""

        out1, err1 = run_query(cmd1)
        out2, err2 = run_query(cmd2)

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

        if err2 and out1:
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

        if err1 and out2:
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

    if bugs == 0:
        for f in [file1, file2]:
            if os.path.exists(f):
                os.remove(f)
    else:
        print(f"Bug found in {name}.sql")
        file = os.path.join(BUGS_FOLDER, f"{name}_clean.sql")
        with open(file, "w") as f:
            for q in queries:
                f.write(q + "\n")

def reset_db():
    for db in [DB1, DB2]:
        if os.path.exists(db):
            os.remove(db)

def load_query_and_oracle(n):
    base_path = os.path.join(".","queries", f"query{n}")
    query_file = os.path.join(base_path, "original_test.sql")
    oracle_file = os.path.join(base_path, "oracle.txt")

    with open(query_file, "r") as qf:
        query = qf.read().strip().replace('\n', ' ')
    with open(oracle_file, "r") as of:
        oracle = of.read().strip()
        
    return query, oracle

def check(original_query, candidate_query, oracle):
    def run_and_capture(query1, query2, version):
        cmd1 = f"/usr/bin/{SQLITE_VERSIONS[version]} {DB1} \"{query1}\""
        cmd2 = f"/usr/bin/{SQLITE_VERSIONS[version]} {DB2} \"{query2}\""
        out1, err1 = run_query(cmd1)
        out2, err2 = run_query(cmd2)
        return (out1, err1), (out2, err2)

    if oracle == "DIFF":
        reset_db()
        (orig_out1, orig_err1), (cand_out1, cand_err1) = run_and_capture(original_query, candidate_query, 0)
        reset_db()
        (orig_out2, orig_err2), (cand_out2, cand_err2) = run_and_capture(original_query, candidate_query, 1)
        reset_db()
        # print(str(1)+orig_out1)
        # print(str(2)+orig_err1)
        # print(str(3)+orig_out2)
        # print(str(4)+orig_err2)
        # print(str(5)+cand_out1)
        # print(str(6)+cand_err1)
        # print(str(7)+cand_out2)
        # print(str(8)+cand_err2)
        if (orig_err1 == cand_err1 and orig_err2 == cand_err2)\
        and (orig_out1 != orig_out2 and cand_out1 != cand_out2):
            return True #Errors are the same with both queries, outputs are different across versions
        if (orig_err1 == cand_err1 and orig_err2 == cand_err2)\
        and (orig_err1 == "" or orig_err2 == ""):
            return True #Errors are the same wiht both queries, only one version has error
        
        return False
    elif oracle == "CRASH(3.26.0)":
        reset_db()
        (_, orig_err1), (_, cand_err1) = run_and_capture(original_query, candidate_query, 0)
        reset_db()
        if (orig_err1 == cand_err1):
            return True
        
        return False
    elif oracle == "CRASH(3.39.4)":
        reset_db()
        (_, orig_err2), (_, cand_err2) = run_and_capture(original_query, candidate_query, 1)
        reset_db()
        if (orig_err2 == cand_err2):
            return True
        
        return False
    
    else:
        raise Exception(f"wrong oracle: {oracle}")
        

def main(args=None):
    #parser = argparse.ArgumentParser(description="Testing")
    #parser.add_argument("type", help="Select testing: BUGS, DATA", nargs="?", default="BUGS")
    
    #args = parser.parse_args(args)

    #if args.type == "BUGS":
    reset()
    sql_folder = Path(QUERY_TEST_FOLDER)
    sql_files = list(sql_folder.glob('*.sql'))  
    for i, sql_file in enumerate(tqdm(sql_files, desc="Testing for bugs")):
        with sql_file.open('r', encoding='utf-8') as f:
            query = sql_cleaner(f.read())
            run_test(query, sql_file.stem)

    '''
    elif args.type == "DATA":
        metrics_folder = Path(STATS_FOLDER)
        counters = []
        for metric in metrics_folder.glob('*.txt'):
            counters.append(parse_metric(metric))
        avg_c = avg_counter(counters)
        with open(f"{TEST_FOLDER}average_count.txt", "w") as f:
            for k, v in avg_c.most_common():
                f.write(f"{k}: {v}\n")
    '''
                
if __name__ == "__main__":
    main()

    

    
    
