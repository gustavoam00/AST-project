import re
from collections import Counter
from generator import randomQueryGen
from typing import List, Union
from config import PROB_TABLE

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
    
def coverage_score(lines, branches, taken, calls, weights=(1.0, 1.0, 1.5, 1.0)):
    return (
        weights[0] * lines +
        weights[1] * branches
    ) / 2.0
    
def get_error(result: str) -> str:
    return re.findall(r"(Error:.*)", result)

if __name__ == "__main__":
    query = randomQueryGen(PROB_TABLE, debug=False, cycle=1000)
    print(metric(query))