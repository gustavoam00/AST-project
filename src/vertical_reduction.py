import re
from typing import Callable
from collections import defaultdict

def remove_false_where_exists(sql: str) -> str:
    """
    Replace WHERE EXISTS (...) blocks that contain always-false conditions like `x <> x`
    with WHERE FALSE or remove them if safe.
    """
    exists_pattern = re.compile(r"WHERE\s+EXISTS\s*\((.*?)\)")

    def is_always_false(subquery: str) -> bool:
        return bool(re.search(r"(\w+)\s*<>\s*\1", subquery))

    def replacer(match: re.Match[str]) -> str:
        subquery = match.group(1)
        if is_always_false(subquery):
            return "WHERE FALSE"
        return match.group(0)

    return exists_pattern.sub(replacer, sql)

def extract_temp(query: str) -> tuple[list[str], list[str], str]:
    query = query.strip()
    temp_names: list[str] = []
    temp_subqueries: list[str] = []

    i = query.upper().find("WITH") + 4
    length = len(query)

    while i < length:
        while i < length and query[i].isspace():
            i += 1

        start = i
        while i < length and (query[i].isalnum() or query[i] == "_"):
            i += 1
        name = query[start:i]
        temp_names.append(name)

        while i < length and query[i] != '(':
            i += 1
        if i == length:
            break
        i += 1

        depth = 1
        start_subq = i
        while i < length and depth > 0:
            if query[i] == '(':
                depth += 1
            elif query[i] == ')':
                depth -= 1
            i += 1
        subquery = query[start_subq:i - 1].strip()
        temp_subqueries.append(subquery)

        while i < length and query[i].isspace():
            i += 1
        if i < length and query[i] == ',':
            i += 1
        else:
            break

    return temp_names, temp_subqueries, query[i:]

def extract_dependencies(query: str, temp_names: list[str]) -> set[str]:
    matches = re.findall(r'\bFROM\s+([A-Za-z0-9_]+)|\bJOIN\s+([A-Za-z0-9_]+)', query, flags=re.IGNORECASE)
    tables = {name for pair in matches for name in pair if name}
    return {t for t in tables if t in temp_names}

def find_used_with(final_query: str, temp_names: list[str], cte_subqueries: list[str]) -> set[str]:
    used: set[str] = set()
    queue = list(extract_dependencies(final_query, temp_names))

    while queue:
        cte = queue.pop()
        if cte not in used:
            used.add(cte)
            index = temp_names.index(cte)
            deps = extract_dependencies(cte_subqueries[index], temp_names)
            queue.extend(deps - used)

    return used

def reduce_temp_tables(query: str) -> str:
    temp_names, temp_subqueries, final_query = extract_temp(query)
    with_queries = find_used_with(final_query, temp_names, temp_subqueries)

    kept_temp: list[str] = []
    for name, subquery in zip(temp_names, temp_subqueries):
        if name in with_queries:
            kept_temp.append(f"{name} AS ({subquery})")

    if kept_temp:
        return "WITH " + ", ".join(kept_temp) + "\n" + final_query
    else:
        return final_query

def vertical_delta_debug(setup_queries: list[str], queries: list[str], error_query: list[str], test: Callable[[list[str]], bool], n: int=2) -> list[str]:
    if len(queries) == 0:
        return []

    while True:
        length = len(queries)
        if n > length:
            break

        chunk_size: int = length // n
        subsets: list[list[str]] = []
        complements: list[list[str]] = []

        for i in range(n):
            start = i * chunk_size
            end = (i + 1) * chunk_size if i < n - 1 else length
            delta_i = queries[start:end]
            nabla_i = queries[:start] + queries[end:]
            subsets.append(delta_i)
            complements.append(nabla_i)

        for delta_i in subsets:
            if test(setup_queries + delta_i + error_query):
                return vertical_delta_debug(setup_queries, delta_i, error_query, test, n=2)

        for nabla_i in complements:
            if test(setup_queries + nabla_i + error_query):
                return vertical_delta_debug(setup_queries, nabla_i, error_query, test, n=max(n - 1, 2))

        if n >= length:
            break

        n = min(n * 2, length)

    return queries 

def extract_tables(query: str) -> tuple[list[str], list[str]]:
    tables: list[str] = []
    
    query_clean = re.sub(r'\bFROM\s+\(\s*([A-Z_][A-Z0-9_]*)\s*\)', r'FROM \1', query.strip())
    
    # extract tables
    q0 = re.findall(r'VIEW\s+([A-Za-z_][A-Za-z0-9_]*)', query_clean)
    q1 = re.findall(r'TABLE\s+([A-Za-z_][A-Za-z0-9_]*)', query_clean)
    q2 = re.findall(r'EXISTS\s+([A-Za-z_][A-Za-z0-9_]*)', query_clean) 
    q3 = re.findall(r'INTO\s+([A-Za-z_][A-Za-z0-9_]*)', query_clean) 
    q4 = re.findall(r'FROM\s+([A-Za-z_][A-Za-z0-9_]*)', query_clean)
    q5 = re.findall(r'JOIN\s+([A-Za-z_][A-Za-z0-9_]*)', query_clean)
    q6 = re.findall(r'ON\s+([A-Za-z_][A-Za-z0-9_]*)', query_clean)
    q7 =re.findall(r'UPDATE\s+([A-Za-z_][A-Za-z0-9_]*)', query_clean)
    q8 = re.findall(r'DELETE FROM\s+([A-Za-z_][A-Za-z0-9_]*)', query_clean)
    q9 = re.findall(r'ALTER TABLE\s+([A-Za-z_][A-Za-z0-9_]*)', query_clean)
    q10 = re.findall(r'REPLACE INTO\s+([A-Za-z_][A-Za-z0-9_]*)', query_clean)

    # SELECT ... FROM ( test1 ) -> test1
    from_matches = re.findall(r'\b(FROM|JOIN)\s+([^;]+?)(?:\bWHERE|\bON|\bGROUP|\bORDER|\bHAVING|$)', query_clean)
    # SELECT ... FROM test1, test2 -> test1 and test2
    from_matches2 = re.findall(r'\b(FROM|JOIN)\s+(\([^\)]+\)|\w+)(?:\s+AS\s+\w+)?', query_clean)

    for _, t in from_matches + from_matches2:
        parts = [part.strip("() ").strip() for part in t.split(',')]
        for part in parts:
            table_name = part.split()[0]
            if re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', table_name):
                if table_name not in tables:
                    tables.append(table_name)

    for t in q0 + q1 + q2 + q3 + q4 + q5 + q6 + q7 + q8 + q9 + q10:
        if t not in tables:
            tables.append(t)

    return tables, q0

def trace_context(queries: list[str], index: int, errlist: list[str], msg: tuple[str, str]):
    """
    keeps dependency list for the query that causes the bug and traces its context backwards
    """
    dependency_tables, _ = extract_tables(queries[index]) 
    original_dependency = dependency_tables
    tables: list[str] = []
    result: list[str] = []
    
    for i in range(index-1, -1, -1):
        query = queries[i]
        if query + "\n" in errlist:
            continue

        tables_in_query, q0 = extract_tables(query)

        # create view that is not used in the query that caused the bug should not influence the bug causing query
        if query.startswith("CREATE VIEW") and q0[0] not in original_dependency:
            continue
        
        if not set(dependency_tables).isdisjoint(tables_in_query):
            # segmentation fault is probably because of semantic/syntax/parsing errors therefore 
            # updating, adding, deleting, etc. should not influence the bug causing query
            if msg[0] == "Error: segmentation fault (core dumped)" or msg[1] == "Error: segmentation fault (core dumped)":
                if query.startswith("CREATE TABLE") or query.startswith("ALTER TABLE") or query.startswith("CREATE VIEW"):
                    if query.startswith("CREATE TABLE") or query.startswith("CREATE VIEW"):
                        tables.insert(0, query)
                    else:
                        result.insert(0, query)
                    for t in tables_in_query:
                        if t not in dependency_tables:
                            dependency_tables.append(t)
            # queries with select and with does not modify the table therefore
            # it should not inluence the bug causing query
            elif not query.startswith("SELECT") and not query.startswith("WITH"): 
                if query.startswith("CREATE TABLE") or query.startswith("CREATE VIEW"):
                    tables.insert(0, query)
                else:
                    result.insert(0, query)
                for t in tables_in_query:
                    if t not in dependency_tables:
                        dependency_tables.append(t)
    
    return tables, result, [queries[index]]

def compress_insert(queries: list[str]) -> list[str]:
    inserts_by_table: dict[str, list[str]] = defaultdict(list)
    output_lines: list[str] = []
    table_first_line_index: dict[str, int] = {}
    insert_pattern = re.compile(r"INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.*)\)\s*;")

    for i, line in enumerate(queries):
        stripped = line.strip()
        match = insert_pattern.match(stripped)
        if match:
            table, values = match.groups()
            inserts_by_table[table].append(values.strip())
            if table not in table_first_line_index:
                table_first_line_index[table] = len(output_lines)
        else:
            output_lines.append(stripped)

    for table, values in inserts_by_table.items():
        compressed_insert = f"INSERT INTO {table} VALUES " + ", ".join(f"({v})" for v in values) + ";"
        insert_pos = table_first_line_index[table]
        output_lines.insert(insert_pos, compressed_insert)

    return output_lines
