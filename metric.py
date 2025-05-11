import re
from collections import Counter
from typing import List, Union
from config import ERROR 

def metric(query: Union[List, str]) -> Counter:
    if isinstance(query, List):
        query = " ".join(query)
    words = re.sub(r'[^a-zA-Z ]', ' ', query).split()
    upper = [w for w in words if w.isupper()]
    return Counter(upper)

def get_coverage(result: str) -> tuple[float, str]:
    lines = re.search(r"Lines executed:([\d.]+)% of (\d+)", result)
    branches = re.search(r"Branches executed:([\d.]+)% of (\d+)", result)
    taken = re.search(r"Taken at least once:([\d.]+)% of (\d+)", result)
    calls = re.search(r"Calls executed:([\d.]+)% of (\d+)", result)
    if lines and branches and taken and calls:
        lines_p = float(lines.group(1))
        branches_p = float(branches.group(1))
        taken_p = float(taken.group(1))
        calls_p = float(calls.group(1))
        return lines_p, branches_p, taken_p, calls_p, result
    else:
        return 0, 0, 0, 0, "Error: Could not extract coverage info."
    
def coverage_score(lines, branches, taken, calls, weights=(1.0, 1.0, 1.0, 1.0)):
    return (
        weights[0] * lines +
        weights[1] * branches +
        weights[2] * taken +
        weights[3] * calls
    ) / float(sum(weights))
    
def save_error(msg: str, save: str) -> str:
    if "Error" in msg and ERROR:
        with open(save, "w") as f:
            errors = re.findall(r"(Error:.*)\n", msg)
            f.write(f"Total Errors: {len(errors)}\n")
            for err in errors:
                f.write(f"{err}\n")

def get_error(msg: str) -> str:
    msg = re.sub(r'\b(in prepare|stepping),\s*', '', msg)
    msg = re.findall(r"Error:(.*)\n", msg)
    msg = [re.sub(r'\s\(\d+\)\s*$', '', line) for line in msg]
    msg = [line.replace('table', 'view', 1) if 'view' in line else line for line in msg]
    msg = [line.replace('database table is locked', 'SQL logic error', 1) if "is locked" in line else line for line in msg]
    msg = [line.replace('unknown or unsupported join type', 'unknown join type', 1) if "unsupported join type" in line else line for line in msg]
    return "\n".join(msg)

def sql_cleaner(queries: str) -> list[str]:
    return [stmt.strip() + ";" for stmt in queries.split(";") if stmt.strip() 
            and "EXPLAIN" not in stmt 
            and "dbstat" not in stmt
            and "date" not in stmt
            and "time" not in stmt 
            and "PRAGMA" not in stmt]

def remove_lines(forwards_file, reference_file, output_file):
    with open(forwards_file, 'r') as f1, open(reference_file, 'r') as f2:
        forwards_lines = f1.readlines()
        reference_lines = f2.readlines()

    length = min(len(forwards_lines), len(reference_lines))
    keep = [True] * len(forwards_lines)

    # Forward comparison
    for i in range(length):
        if forwards_lines[i] == reference_lines[i]:
            keep[i] = False

    # Backward comparison
    for i in range(1, length + 1):
        if forwards_lines[-i] == reference_lines[-i]:
            keep[-i] = False

    result_lines = [line for line, k in zip(forwards_lines, keep) if k]

    with open(output_file, 'w') as out:
        out.writelines(result_lines)

def remove_common_lines(file1_path, file2_path, out1_path, out2_path):
    with open(file1_path, 'r') as f1, open(file2_path, 'r') as f2:
        lines1 = f1.readlines()
        lines2 = f2.readlines()

    set1 = set(lines1)
    set2 = set(lines2)
    common = set1 & set2

    unique1 = [line for line in lines1 if line not in common]
    unique2 = [line for line in lines2 if line not in common]

    with open(out1_path, 'w') as out1, open(out2_path, 'w') as out2:
        out1.writelines(unique1)
        out2.writelines(unique2)
    
