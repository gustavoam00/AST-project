import re
from collections import Counter
from typing import List, Union
from config import ERROR 
from pathlib import Path
import ast

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
    Parses a file containing metric data, either as:
    - A single line with Counter({...})
    - A block format:
        Metrics:
            KEY: VALUE
            ...

    Returns:
        Counter with metric keys and their counts.
    '''
    metrics = Counter()
    with open(filepath, 'r') as f:
        lines = f.readlines()

    for line in lines:
        line = line.strip()
        if line.startswith("Metrics:") and "Counter({" in line:
            try:
                counter_str = line.split("Counter(")[1]
                counter_str = counter_str.rstrip(")")
                metrics = Counter(ast.literal_eval(counter_str))
                return metrics
            except Exception:
                pass

    in_metrics = False
    for line in lines:
        line = line.strip()
        if line == "Metrics:":
            in_metrics = True
            continue

        if in_metrics:
            if not line or ":" not in line:
                continue
            try:
                key, value = line.split(":", 1)
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

def avg_metric(filepath):
    total_files = 0
    total_runtime = 0.0
    total_average_coverage = 0.0
    total_lines_coverage = 0.0
    total_branch_coverage = 0.0
    total_taken_coverage = 0.0
    total_calls_coverage = 0.0
    total_valid = 0
    total_invalid = 0
    total_errors = 0

    patterns = {
        "Average Coverage": re.compile(r"Average Coverage:\s*([\d.]+)"),
        "Lines Coverage": re.compile(r"Lines Coverage:\s*([\d.]+)"),
        "Branch Coverage": re.compile(r"Branch Coverage:\s*([\d.]+)"),
        "Taken Coverage": re.compile(r"Taken Coverage:\s*([\d.]+)"),
        "Calls Coverage": re.compile(r"Calls Coverage:\s*([\d.]+)"),
        "Valid/Invalid": re.compile(r"Valid/Invalid:\s*(\d+)\s*/\s*(\d+)"),
        "Errors": re.compile(r"Errors:\s*(\d+)"),
        "Runtime": re.compile(r"Runtime:\s*([\d.]+)")
    }

    # Loop through .txt files
    for file in Path(filepath).rglob("*.txt"):
        with open(file, "r") as f:
            content = f.read()

        try:
            total_average_coverage += float(patterns["Average Coverage"].search(content).group(1))
            total_lines_coverage += float(patterns["Lines Coverage"].search(content).group(1))
            total_branch_coverage += float(patterns["Branch Coverage"].search(content).group(1))
            total_taken_coverage += float(patterns["Taken Coverage"].search(content).group(1))
            total_calls_coverage += float(patterns["Calls Coverage"].search(content).group(1))
            valid_invalid_match = patterns["Valid/Invalid"].search(content)
            total_valid += int(valid_invalid_match.group(1))
            total_invalid += int(valid_invalid_match.group(2))
            total_errors += int(patterns["Errors"].search(content).group(1))
            total_runtime += float(patterns["Runtime"].search(content).group(1))
            total_files += 1
        except AttributeError:
            print(f"Warning: Some metrics missing in file {file.name}. Skipping.")

    if total_files > 0:
        print("=== Averages Over Files ===")
        print(f"Average Coverage: {total_average_coverage / total_files:.2f}")
        print(f"Lines Coverage: {total_lines_coverage / total_files:.2f}")
        print(f"Branch Coverage: {total_branch_coverage / total_files:.2f}")
        print(f"Taken Coverage: {total_taken_coverage / total_files:.2f}")
        print(f"Calls Coverage: {total_calls_coverage / total_files:.2f}")
        print(f"Valid/Invalid: {total_valid}/{total_invalid}")
        print(f"Errors: {total_errors}")
        print(f"Avg Runtime: {total_runtime / total_files:.4f}")
        print(f"Total Runtime: {total_runtime:.4f}")
    else:
        print("No valid .txt files found with the required metrics.")

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
            and "RANDOM" not in stmt
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
    
