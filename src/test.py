import subprocess
from .helper import get_coverage, sql_cleaner
from pathlib import Path
from .config import QUERY_TEST_FOLDER, SQLITE_VERSIONS, DB1, DB2, BUGS_FOLDER
from tqdm import tqdm
import os, argparse

LOCAL = True

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

        print(SQLITE_VERSIONS[0], out1, err1)
        print(SQLITE_VERSIONS[1], out2, err2)

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

        if err2 and not out2:
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

        if err1 and not out1:
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

def main(args=None):
    #parser = argparse.ArgumentParser(description="Testing")
    #parser.add_argument("type", help="Select testing: BUGS, DATA", nargs="?", default="BUGS")
    
    #args = parser.parse_args(args)

    #if args.type == "BUGS":
    reset()
    # SELECT folder inside queries to test
    sql_folder = Path(QUERY_TEST_FOLDER + "query1/")
    sql_files = list(sql_folder.glob('*.sql'))  
    for i, sql_file in enumerate(tqdm(sql_files, desc="Testing for bugs")):
        with sql_file.open('r', encoding='utf-8') as f:
            query = sql_cleaner(f.read())
            run_test(query, sql_file.stem)

if __name__ == "__main__":
    main()

    

    
    
