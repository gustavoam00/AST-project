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
        return 0, "Error: Could not extract coverage info."
    
def get_error(result: str) -> str:
    # Error: Constraint Failed.
    # Error: No such table
    # Error: No such Column
    # Error: Syntax error near unexpected token
    # Error: Attempt to Write a Read-Only Database
    # Error: Foreign Key Constraint Failed.
    # Error: Out of Memory
    # Error: Column Index Out of Range
    # Error: Abort due to constraint violation
    # Error: Database Schema has Changed
    # Error: misuse of aggregate function
    # Error: Type Mismatch in Expression
    # Error: Cannot start a transaction within a transaction
    # Error: Recursive CTE Query Limit Exceeded
    # Error: Virtual Table Mismatch
    # Error: Attempt to Bind Null Value
    # Error: NULL value in NOT NULL column
    # Error: Division by Zero
    # Error: PRIMARY KEY must be unique
    # Error: Unindexed Query Detected
    # Error: Triggers are Disabled
    # Error: Parameter Count Mismatch
    # Error: Invalid Function in Query
    # Error: Cannot Add Column in Virtual Table 
    # Error: Duplicate Column Name in Table 
    # Error: Cannot Drop a Referenced Table
    # Error: Infinite Loop Detected in Trigger Execution 
    # Error: Subquery Returns Multiple Rows
    # Error: ROWID Value is NULL
    # Error: Invalid Use of PRAGMA Statement 
    # Error: Failed to Commit Transaction
    # Error: Cannot Use JSON Functions Without Data
    # Error: Table Name Already Exists 
    # Error: CTE Expression Exceeds Allowed Recursion Depth 
    # Error: Invalid Value for PRAGMA Configuration 
    return 0

if __name__ == "__main__":
    query = randomQueryGen(PROB_TABLE, debug=False, cycle=1000)
    print(metric(query))