import re

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
    
def save_error(msg: str, savepath: str, save: bool = False) -> int:
    errors = re.findall(r"(Error:.*)\n", msg)
    if "Error" in msg and save:
        with open(savepath, "w") as f:
            f.write(f"Total Errors: {len(errors)}\n")
            for err in errors:
                f.write(f"{err}\n")
    return len(errors)

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