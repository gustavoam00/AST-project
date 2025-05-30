import re
    
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

def get_queries(path: str) -> list[str]:
    with open(path, 'r') as f:
        lines = f.readlines()
    lines = [line for line in lines if not line.strip().startswith('.')]
    lines = ''.join(lines)

    queries: list[str] = []
    current_query: list[str] = []
    in_single_quote = False
    in_double_quote = False
    i = 0
    length = len(lines)

    while i < length:
        char = lines[i]
        next_char = lines[i+1] if i + 1 < length else ''
        current_query.append(char)

        if char == "'" and not in_double_quote:
            if next_char == "'":  
                current_query.append(next_char)
                i += 1 
            else:
                in_single_quote = not in_single_quote

        elif char == '"' and not in_single_quote:
            if next_char == '"':
                current_query.append(next_char)
                i += 1
            else:
                in_double_quote = not in_double_quote

        elif char == ';' and not in_single_quote and not in_double_quote:
            query = ''.join(current_query).strip()
            if query:
                queries.append(query)
            current_query = []

        i += 1

    if current_query:
        query = ''.join(current_query).strip()
        if query:
            queries.append(query)

    return queries


def read_info(path: str) -> tuple[int, list[str], tuple[str, str]]:
    with open(path, "r") as f:
        index = int(f.readline().strip()) 
        msg0 = f.readline().strip()
        msg1 = f.readline().strip()
        msg = (msg0, msg1)
        errlist = f.readlines()
    
    return index, errlist, msg

def write_queries(path: str, queries: list[str]):
     with open(path, "w") as f:
        f.writelines(line + "\n" for line in queries)

def group_queries(queries: list[str], group_size: int = 200) -> list[str]:
    grouped: list[str] = []
    for i in range(0, len(queries), group_size):
        group = " ".join(queries[i:i + group_size])
        grouped.append(group)
    return grouped

def flatten(tokens_list: list[list[str]]) -> list[str]:
    return [token for tokens in tokens_list for token in tokens]