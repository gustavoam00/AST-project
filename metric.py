import re
from collections import Counter
from typing import List, Union
from config import ERROR 

def metric(query: Union[List, str]) -> Counter:
    '''
    Extracts all words that is in uppercase inside a list or string
    '''
    if isinstance(query, List):
        query = " ".join(query)
    query = " ".join(query.split(";"))
    words = re.sub(r'[^a-zA-Z ]', ' ', query).split()
    upper = [w for w in words if w.isupper()]
    return Counter(upper)

def parse_metric(filepath: str) -> Counter:
    '''
    Extracts metrics information of the file at filepath:
    Metrics:
        SELECT: 3
        CASE: 1
        FROM: 3
        ...
    Returns: Counter({"SELECT": 3, "CASE": 1, "FROM": 3, ...})
    '''
    metrics = Counter()
    in_metrics = False

    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()

            if line == "Metrics:":
                in_metrics = True
                continue

            if in_metrics:
                if not line or ":" not in line:
                    continue
                try:
                    key, value = line.split(":")
                    metrics[key.strip()] = int(value.strip())
                except ValueError:
                    continue 

    return metrics

def avg_counter(counters: List[Counter]) -> Counter:
    all_keys = set().union(*counters)

    n = len(counters)
    average = Counter({
        key: sum(c.get(key, 0) for c in counters) / n
        for key in all_keys
    })

    return average

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
    
def save_error(msg: str, save: str) -> int:
    errors = []
    if "Error" in msg and ERROR:
        with open(save, "w") as f:
            errors = re.findall(r"(Error:.*)\n", msg)
            f.write(f"Total Errors: {len(errors)}\n")
            for err in errors:
                f.write(f"{err}\n")
    return len(errors)

def get_error(msg: str) -> str:
    msg = re.findall(r"Error:(.*)\n", msg)
    msg = [re.sub(r'\b(in prepare|stepping),\s*', '', line) for line in msg]
    msg = [re.sub(r'\s\(\d+\)\s*$', '', line) for line in msg]
    msg = [line.replace('table', 'view', 1) if 'view' in line else line for line in msg]
    msg = [line.replace('database table is locked', 'SQL logic error', 1) if "is locked" in line else line for line in msg]
    msg = [line.replace('unknown or unsupported join type', 'unknown join type', 1) if "unsupported join type" in line else line for line in msg]
    return "\n".join(msg)

def sql_cleaner(queries: str) -> list[str]:
    return [
        stmt.strip() + ";" for stmt in queries.split(";") if stmt.strip() 
            and "EXPLAIN" not in stmt 
            and "dbstat" not in stmt
            and "date" not in stmt
            and "time" not in stmt 
            and "collation_list" not in stmt
            and "function_list" not in stmt
            and "analysis_limit" not in stmt
            and "database_list" not in stmt
            and "ANALYZE" not in stmt
            and "REINDEX" not in stmt
            and "VACUUM" not in stmt
            and "julianday" not in stmt
    ]

def remove_lines(result1: list[str], result2: list[str]):
    length = min(len(result1), len(result2))
    keep = [True] * len(result1)

    # forward comparison
    for i in range(length):
        if result1[i] == result2[i]:
            keep[i] = False

    # backward comparison
    for i in range(1, length + 1):
        if result1[-i] == result2[-i]:
            keep[-i] = False

    result_lines = [line for line, k in zip(result1, keep) if k]

    return result_lines

def remove_common_lines(result1: list[str], result2: list[str]):
    set1 = set(result1)
    set2 = set(result2)
    common = set1 & set2

    unique1 = [line for line in result1 if line not in common]
    unique2 = [line for line in result2 if line not in common]

    return unique1, unique2
    
