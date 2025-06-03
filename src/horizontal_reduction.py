import re
import math
import sys
import difflib
from tqdm import tqdm

KEYWORDS = [
    'values', 'in', #'set', 'update', 'insert', 'into', 'select', 'where',

    'lower', 'upper', 'lag', 'lead', 'abs', 'sum', 'avg', 'min', 'max',
    'count', 'length', 'trim', 'substr', 'round', 'cast', 'coalesce',
    'nullif', 'ifnull', 'date', 'datetime', 'quote', 'zeroblob', 'typeof',
    ]
            
def simplify_sql(query):
    """
    Evaluates all logic and math expressions to simplify them as much as possible
    """
    def try_eval_math(expr):
        try:
            expr = expr.replace(' ', '')
            expr = expr.replace('++', '+').replace('--', '+').replace('+-', '-').replace('-+', '-')
            expr = re.sub(r'(-?\d+)/(-?\d+)', r'int(\1/\2)', expr)
            result = eval(expr, {"__builtins__": None, "int": int}, {})
            return str(result)
        except:
            return None

    def try_eval_bool(expr):
        expr = expr.strip().lower()
        normalized = " ".join(expr.split())

        if normalized == "not true":
            return "false"
        if normalized == "not false":
            return "true"

        match = re.fullmatch(
            r'(true|false|[-+]?\d+(?:\.\d+)?(?:e[-+]?\d+)?)[ ]*'
            r'(=|<>|!=|<=|>=|<|>)[ ]*'
            r'(true|false|[-+]?\d+(?:\.\d+)?(?:e[-+]?\d+)?)',
            expr
        )
        if match:
            lhs, op, rhs = match.groups()
            lhs = lhs.replace('true', 'True').replace('false', 'False')
            rhs = rhs.replace('true', 'True').replace('false', 'False')
            py_op = op.replace('=', '==').replace('<>', '!=').replace('!==', '!=')

            try:
                result = eval(f"{lhs} {py_op} {rhs}", {"__builtins__": None}, {})
                return 'true' if result else 'false'
            except:
                return None
        return None

    paren_expr = re.compile(r'\(([^()]+)\)')
    
    while True:
        changed = False
        for match in paren_expr.finditer(query):
            inner = match.group(1).strip()

            if re.fullmatch(r'[ \d\+\-\*/.eE]+', inner):
                result = try_eval_math(inner)
            else:
                result = try_eval_bool(inner)

            if result is not None:
                context_before = query[:match.start()].rstrip().lower()
                prev_word = context_before.split()[-1] if context_before.split() else ""

                if prev_word in KEYWORDS:
                    result = f'__open__ {result} __close__'
                elif prev_word == ',' and context_before.split()[-2] == '__close__':
                    result = f'__open__ {result} __close__'
                    
                query = query[:match.start()] + result + query[match.end():]
                changed = True
                break

        if not changed:
            break

    query = query.replace('__open__', '(').replace('__close__', ')')
    
    query = query.replace('(', ' ( ').replace(')', ' ) ').replace(',', ' , ')
    query = re.sub(r'\+\s+\+', '+', query)
    query = re.sub(r'-\s+-', '+', query)
    query = re.sub(r'\+\s+-', '-', query)
    query = re.sub(r'-\s+\+', '-', query)
    return query


def minimize_columns(query_list, test):
    """
    Tries to eliminate cols ffrom inside insert statements
    """
    def transform_insert_statements(query_list):
        """
        Helper transforms Insert without cols to ahve cols
        """
        def get_all_cols(query_list):
            def get_table_cols(create_stmt):
                create_stmt = create_stmt.strip().strip(';')
                match = re.match(r"CREATE TABLE\s+(\w+)\s*\((.*?)\)", create_stmt, re.IGNORECASE | re.DOTALL)
                if not match:
                    return {}
                table_name = match.group(1)
                columns_str = match.group(2)
                columns = [line.strip().split()[0] for line in columns_str.split(',') if line.strip()]
                return {table_name: columns}
            cols = {}
            for stmt in query_list:
                if stmt.strip().upper().startswith("CREATE TABLE"):
                    cols.update(get_table_cols(stmt))
            return cols
        
        table_columns = get_all_cols(query_list)
        updated_sql = []

        insert_regex = re.compile(r"INSERT INTO\s+(\w+)\s+VALUES", re.IGNORECASE)

        for stmt in query_list:
            match = insert_regex.match(stmt.strip())
            if match:
                table_name = match.group(1)
                if table_name in table_columns:
                    columns_str = ' , '.join(table_columns[table_name])
                    stmt = re.sub(
                        r"(INSERT INTO\s+" + re.escape(table_name) + r")\s+VALUES",
                        r"\1 ( " + columns_str + r" ) VALUES",
                        stmt,
                        flags=re.IGNORECASE
                    )
            updated_sql.append(stmt)
        return updated_sql
    modified_sql = transform_insert_statements(query_list)
    if not test(modified_sql):
        return query_list

    for i, stmt in enumerate(modified_sql):
        insert_match = re.match(r"INSERT INTO\s+(\w+)\s+\((.*?)\)\s+VALUES\s+(.*)", stmt, re.IGNORECASE | re.DOTALL)
        if not insert_match:
            continue
        
        table_name, col_list_str, values_section = insert_match.groups()
        columns = [col.strip() for col in col_list_str.split(',')]
        values = re.findall(r'\((.*?)\)', values_section, re.DOTALL)
        values_lists = [list(map(str.strip, val.split(','))) for val in values]
        changed = False
        col_idx = 0

        while col_idx < len(columns):
            temp_columns = columns[:col_idx] + columns[col_idx + 1:]
            temp_values_lists = [vals[:col_idx] + vals[col_idx + 1:] for vals in values_lists]

            new_values_str = ' , '.join(f"({' , '.join(vals)})" for vals in temp_values_lists)
            new_stmt = f"INSERT INTO {table_name} ( {' , '.join(temp_columns)} ) VALUES {new_values_str} ;"

            trial_sql = modified_sql[:]
            trial_sql[i] = new_stmt
            if test(trial_sql):
                columns = temp_columns
                values_lists = temp_values_lists
                modified_sql[i] = new_stmt
                changed = True
                col_idx = 0
            else:
                col_idx += 1

        if not changed:
            modified_sql[i] = query_list[i]
    modified_sql = [space_it_out(q) for q in modified_sql]
    return modified_sql


def strip_column_types(sql):
    """
    Strip all col type denitions in create tables
    """
    def remove_types(match):
        header = match.group(1)
        body = match.group(2)

        columns = []
        current = ''
        depth = 0
        for c in body:
            if c == ',' and depth == 0:
                columns.append(current.strip())
                current = ''
            else:
                current += c
                if c == '(':
                    depth += 1
                elif c == ')':
                    depth -= 1
        if current:
            columns.append(current.strip())

        TYPE_PATTERN = re.compile(r"^(INTEGER|TEXT|REAL|BOOLEAN|BLOB|NUMERIC|CHARACTER|BIGINT|INT|MEDIUMINT|NCHAR|DOUBLE|CLOB|DATE|UNSIGNED|BIG|PRECISION|JSON)$", re.IGNORECASE)

        stripped_columns = []
        for col in columns:
            parts = col.strip().split()
            if not parts:
                continue

            if re.match(r'(?i)(PRIMARY|FOREIGN|UNIQUE|CHECK|CONSTRAINT)', parts[0]):
                stripped_columns.append(col)
                continue

            col_name = parts[0]
            rest = parts[1:]
            new_parts = [p for p in rest if not TYPE_PATTERN.fullmatch(p)]
            stripped_line = f"{col_name} {' '.join(new_parts)}".strip()
            stripped_columns.append(stripped_line)

        formatted = ", ".join(stripped_columns)
        return f"{header}( {formatted} ) ;"

    pattern = re.compile(
        r"(CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+[^\(]+?)\s*\((.*?)\)\s*;",
        re.IGNORECASE | re.DOTALL
    )

    return pattern.sub(remove_types, sql)

def strip_select(sql): #group and order by maybe possible, more difficult
    """
    Simplify select a bit with parts that are not error prone
    """
    sql = re.sub(r'LIMIT\s+\d+', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'OFFSET\s+\d+', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bDISTINCT\b', '', sql, flags=re.IGNORECASE)
    return sql

def simplify_nullif(sql):
    """
    Evaluates simple NULLIF functions where both arguments are literals or simple values.
    """
    pattern = re.compile(r'NULLIF\s*\(\s*([^\(\),]+?)\s*,\s*([^\(\),]+?)\s*\)', re.IGNORECASE)

    def replacer(match):
        val1 = match.group(1).strip()
        val2 = match.group(2).strip()
        return 'NULL' if val1 == val2 else val1

    return pattern.sub(replacer, sql)

def clean_semicolons(sql):
    """
    Remove doubled semicolons
    """
    sql = re.sub(r'(;\s*){2,}', '; ', sql)
    return sql.strip()

def remove_nested_parentheses(sql):
    """
    Remove doubled parenthesis
    """
    pattern = re.compile(r'\(\s*\(([^()]*?)\)\s*\)')
    prev = None
    while prev != sql:
        prev = sql
        sql= pattern.sub(r'(\1)', sql)
    return sql.strip()

def remove_functions(sql):
    """
    Simplify values that some functions represent
    """
    sql = re.sub(r'CAST\s*\(\s*(.*?)\s+AS\s+[^)]+\)', r'\1', sql, flags=re.IGNORECASE)
    sql = re.sub(r'TYPEOF\s*\(\s*(.*?)\s*\)', r'\1', sql, flags=re.IGNORECASE)
    sql = re.sub(r'QUOTE\s*\(\s*(.*?)\s*\)', r'\1', sql, flags=re.IGNORECASE)
    sql = re.sub(r'UNLIKELY\s*\(\s*(.*?)\s*\)', r'\1', sql, flags=re.IGNORECASE)
    sql = re.sub(r'LIKELY\s*\(\s*(.*?)\s*\)', r'\1', sql, flags=re.IGNORECASE)
    return sql.strip()

def sanitize_string_literals(sql):
    """
    Remove spaces inside strings ''
    """
    def clean_match(match):
        inner = match.group(1)
        cleaned = inner.replace('(', '').replace(')', '').replace(',', '').replace(' ', '')
        return f"'{cleaned}'"
    return re.sub(r"'(.*?)'", clean_match, sql)

def space_it_out(query):
    """
    Separates all tokens in a query
    """
    query = query.replace('(', ' ( ').replace(')', ' ) ')
    query = query.replace(';', ' ; ').replace(',', ' , ')
    query = re.sub(r'\s+', ' ', query).strip()
    def remove_spaces_in_quotes(match):
        inner = match.group(1)
        cleaned = inner.replace(' ', '')
        return f"'{cleaned}'"
    query = re.sub(r"'(.*?)'", remove_spaces_in_quotes, query)
    return query

def cleaning_pipeline(query):
    """
    Applies all cleaning fucnitons to query
    """
    new_query = space_it_out(query)
    new_query = clean_semicolons(new_query)
    new_query = simplify_sql(new_query)
    new_query = remove_nested_parentheses(new_query)
    new_query = strip_column_types(new_query)
    new_query = strip_select(new_query)
    new_query = remove_functions(new_query)
    new_query = simplify_nullif(new_query)
    new_query = sanitize_string_literals(new_query)
    new_query = space_it_out(new_query)
    return new_query

def cleaning_by_query(query_list, test):
    """
    Used if cleaning all doesn't work, clean only the queries that work
    """
    result = []
    for i in range(len(query_list)):
        query = query_list[i]
        cleaned = cleaning_pipeline(query)
        if test(result + [cleaned] + query_list[i+1:]):
            result.append(cleaned)
        else:
            result.append(query)
    return result

def try_simplify_cases(queries, test):
    """
    Tries to simplify all case statements to be a single value, wherever possible
    """
    def simplify_case_expr(case_expr):
        then_values = re.findall(r'\bTHEN\s+(.*?)(?=\s+WHEN|\s+ELSE|\s+END)', case_expr, re.IGNORECASE | re.DOTALL)
        else_match = re.search(r'\bELSE\s+(.*?)(?=\s+END)', case_expr, re.IGNORECASE | re.DOTALL)

        values = then_values[:]
        if else_match:
            values.append(else_match.group(1).strip())

        for val in values:
            if val.strip().upper() == 'NULL':
                return 'NULL'

        return values[0].strip() if values else 'NULL'
    
    minimized_queries = []

    for i, q in enumerate(queries):
        tokens = q.split()
        simplified_tokens = tokens[:]
        positions = []

        stack = []
        for idx, token in enumerate(tokens):
            if token.upper() == 'CASE':
                stack.append(idx)
            elif token.upper() == 'END' and stack:
                start = stack.pop()
                positions.append((start, idx))

        positions = sorted(positions, key=lambda x: x[0], reverse=True)

        for start, end in positions:
            case_tokens = simplified_tokens[start:end + 1]
            case_str = ' '.join(case_tokens)
            simplified_val = simplify_case_expr(case_str)

            new_tokens = simplified_tokens[:start] + [simplified_val] + simplified_tokens[end + 1:]
            if check_query_tokens(new_tokens, i, minimized_queries, queries, test):
                simplified_tokens = new_tokens

        minimized_queries.append(' '.join(simplified_tokens))

    return minimized_queries

def try_remove_parens(queries, test):
    """
    Tries to remove parenthesis that are not needed 
    """
    minimized_queries = []

    for i, q in enumerate(queries):
        tokens = q.split()
        stack = []
        pairs = []

        for idx, token in enumerate(tokens):
            if token == '(':
                stack.append(idx)
            elif token == ')':
                if stack:
                    start = stack.pop()
                    pairs.append((start, idx))

        pairs = sorted(pairs, reverse=True)

        for start, end in pairs:
            if start > 0 and tokens[start - 1].lower() in KEYWORDS:
                continue
            new_tokens = tokens[:start] + tokens[start+1:end] + tokens[end+1:]

            if check_query_tokens(new_tokens, i, minimized_queries, queries, test):
                tokens = new_tokens

        minimized_queries.append(' '.join(tokens))

    return minimized_queries


### DELTA DEBUGGING
def check_query_tokens(new_tokens, idx, minimized_so_far, queries, test):
    rebuilt = []
    rebuilt.extend(minimized_so_far)
    rebuilt.append(' '.join(new_tokens))
    rebuilt.extend(q.strip() for q in queries[idx + 1:])

    return test(rebuilt)

def horizontal_delta_debug(queries, test, id = 1):
    minimized_queries = []

    for i, q in enumerate(queries):
        tokens = q.split()
        tokens = tokens[:-1]
        if len(tokens) > 180:
            minimized_queries.append(q)
            continue
        
        if id == 0:
            reduced_tokens = delta_debug_extra(
                tokens,
                lambda new_tokens: check_query_tokens(new_tokens+[';'], i, minimized_queries, queries, test)
            )
        elif id == 1:
            reduced_tokens = delta_debug(
                tokens,
                lambda new_tokens: check_query_tokens(new_tokens+[';'], i, minimized_queries, queries, test)
            )
        else:
            reduced_tokens = sliding_window(
                tokens,
                lambda new_tokens: check_query_tokens(new_tokens+[';'], i, minimized_queries, queries, test)
            )
        if reduced_tokens:
            minimized_queries.append(' '.join(reduced_tokens+[';']))

    return minimized_queries

def delta_debug(tokens, test):
    """
    Classic Delta Debugging
    """
    length = len(tokens)
    chunk_size = max(length // 2, 1)

    while chunk_size >= 1:
        i = 0
        while i < len(tokens):
            start = i
            end = min(i + chunk_size, len(tokens))
            reduced = tokens[:start] + tokens[end:]

            if test(reduced):
                tokens = reduced
            else:
                i += chunk_size

        chunk_size = math.floor(chunk_size / 2)

    return tokens

def delta_debug_extra(tokens, test):
    """
    Delta Debugging, but,
    Restarts if tokens removed to try to simplify again
    """
    n = 2
    while True:
        chunk_size = math.floor(len(tokens) / n)
        if chunk_size == 0:
            break
        subset_found = False
        for i in range(n):
            start = i * chunk_size
            end = start + chunk_size if i < n - 1 else len(tokens)
            reduced = tokens[:start] + tokens[end:]
            if test(reduced):
                tokens = reduced
                n = max(n - 1, 2)
                subset_found = True

        if not subset_found:
            if n >= len(tokens):
                break
            n = min(n * 2, len(tokens))

    return tokens

def sliding_window(tokens, test):
    """
    Sets a chunk size as window ans slides through token, attempting to remove them.
    """
    chunk_size = 4 if len(tokens) > 30 else 3

    while chunk_size >= 1:
        i = 0
        while i < len(tokens):
            start = i
            end = min(i + chunk_size, len(tokens))
            reduced = tokens[:start] + tokens[end:]

            if test(reduced):
                tokens = reduced
                i = max(0, start - chunk_size)
            else:
                i += 1

        chunk_size -= 1

    return tokens


def main():
    sql = "INSERT OR ABORT INTO tbl_wqiwo (rcol_eitnk, tcol_wskpp, tcol_yqthy) VALUES (1.0, 11960.180152676927, CAST('v_citjr' GLOB 'rAag' AS TEXT)), (CAST(UNLIKELY(- (NULL)) / NULLIF(0,0) AS REAL), 3802.849483805112, 1), (CAST(PRINTF('%.6e', -60733.41165606832 != -54381.10564306702) AS REAL), 8446, 'v_bwaja'), (24227.630527005007, 999999999999999999999999999999999999999999999999999999999999999999999999, 'v_fgzuu');"
    print(cleaning_pipeline(sql))
    return
    
if __name__ == "__main__":
    main()