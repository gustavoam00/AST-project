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


def get_table_cols(create_stmt):
    create_stmt = create_stmt.strip().strip(';')
    match = re.match(r"CREATE TABLE\s+(\w+)\s*\((.*?)\)", create_stmt, re.IGNORECASE | re.DOTALL)
    if not match:
        return {}
    table_name = match.group(1)
    columns_str = match.group(2)
    columns = [line.strip().split()[0] for line in columns_str.split(',') if line.strip()]
    return {table_name: columns}

def get_all_cols(query_list):
    cols = {}
    for stmt in query_list:
        if stmt.strip().upper().startswith("CREATE TABLE"):
            cols.update(get_table_cols(stmt))
    return cols

def transform_insert_statements(query_list):
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

def minimize_columns(query_list, test):
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


def strip_column_types(sql_text):
    def process_create_table_block(match):
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

        TYPE_PATTERN = re.compile(r"^(INTEGER|TEXT|REAL|BOOLEAN|BLOB|NUMERIC|CHARACTER|BIGINT|INT|MEDIUMINT|NCHAR|DOUBLE|CLOB|DATE|UNSIGNED|BIG|PRECISION)$", re.IGNORECASE)

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

    return pattern.sub(process_create_table_block, sql_text)

def strip_select(query): #group and order by maybe possible, more difficult
    query = re.sub(r'LIMIT\s+\d+', '', query, flags=re.IGNORECASE)
    query = re.sub(r'OFFSET\s+\d+', '', query, flags=re.IGNORECASE)
    query = re.sub(r'\bDISTINCT\b', '', query, flags=re.IGNORECASE)
    return query

def remove_case(query):
    def simplify_case(case_expr):
        then_values = re.findall(r'THEN\s+(.*?)(?=\s+WHEN|\s+ELSE|\s+END)', case_expr, re.IGNORECASE | re.DOTALL)
        else_match = re.search(r'ELSE\s+(.*?)(?=\s+END)', case_expr, re.IGNORECASE | re.DOTALL)

        if 'NULL' in case_expr.upper():
            return 'NULL'
        elif else_match:
            return else_match.group(1).strip()
        elif then_values:
            return then_values[0].strip()
        else:
            return 'NULL'

    case_pattern = re.compile(r'CASE\s+.*?\s+END', re.IGNORECASE | re.DOTALL)

    def replacer(match):
        case_expr = match.group()
        return simplify_case(case_expr)

    return re.sub(case_pattern, replacer, query)

def clean_semicolons(sql):
    sql = re.sub(r'(;\s*){2,}', '; ', sql)
    return sql.strip()

def remove_nested_parentheses(s):
    pattern = re.compile(r'\(\s*\(([^()]*?)\)\s*\)')
    prev = None
    while prev != s:
        prev = s
        s = pattern.sub(r'(\1)', s)
    return s

def remove_functions(s):
    s = re.sub(r'CAST\s*\(\s*(.*?)\s+AS\s+[^)]+\)', r'\1', s, flags=re.IGNORECASE)
    s = re.sub(r'TYPEOF\s*\(\s*(.*?)\s*\)', r'\1', s, flags=re.IGNORECASE)
    s = re.sub(r'QUOTE\s*\(\s*(.*?)\s*\)', r'\1', s, flags=re.IGNORECASE)
    return s.strip()

def sanitize_string_literals(s):
    def clean_match(match):
        inner = match.group(1)
        cleaned = inner.replace('(', '').replace(')', '').replace(',', '').replace(' ', '')
        return f"'{cleaned}'"
    return re.sub(r"'(.*?)'", clean_match, s)

def remove_useless_parentheses(s):
    return

def space_it_out(query):
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
    new_query = space_it_out(query)
    new_query = clean_semicolons(new_query)
    new_query = simplify_sql(new_query)
    new_query = remove_nested_parentheses(new_query)
    new_query = strip_column_types(new_query)
    new_query = strip_select(new_query)
    new_query = remove_functions(new_query)
    # new_query = remove_case(new_query)
    new_query = sanitize_string_literals(new_query)
    new_query = space_it_out(new_query)
    return new_query

def aggressive_cleaning_pipeline(query):
    new_query = cleaning_pipeline(query)
    new_query = remove_useless_parentheses(new_query)
    new_query = space_it_out(new_query)
    return new_query

def cleaning_by_query(query_list, test):
    result = []
    for i in range(len(query_list)):
        query = query_list[i]
        cleaned = cleaning_pipeline(query)
        if test(result + [cleaned] + query_list[i+1:]):
            result.append(cleaned)
        else:
            result.append(query)
    return result

def try_remove_parens(queries, test):
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


def check_query_tokens(new_tokens, idx, minimized_so_far, queries, test):
    rebuilt = []
    rebuilt.extend(minimized_so_far)
    rebuilt.append(' '.join(new_tokens))
    rebuilt.extend(q.strip() for q in queries[idx + 1:])

    return test(rebuilt)

def delta_debug(queries, test, id = 1):
    minimized_queries = []

    for i, q in enumerate(queries):
        tokens = q.split()
        if id == 0:
            reduced_tokens = ddmin(
                tokens,
                lambda new_tokens: check_query_tokens(new_tokens, i, minimized_queries, queries, test)
            )
        elif id == 1:
            reduced_tokens = ddmin2(
                tokens,
                lambda new_tokens: check_query_tokens(new_tokens, i, minimized_queries, queries, test)
            )
        else:
            reduced_tokens = ddmin3(
                tokens,
                lambda new_tokens: check_query_tokens(new_tokens, i, minimized_queries, queries, test)
            )
        if reduced_tokens:
            minimized_queries.append(' '.join(reduced_tokens))

    return minimized_queries

def selective_delta_debug(queries, test, id = 1, skip_prefixes=("INSERT", "REPLACE")):
    minimized_queries = []

    for i, q in enumerate(queries):

        if any(q.startswith(prefix.upper()) for prefix in skip_prefixes):
            minimized_queries.append(q)
            continue

        tokens = q.split()
        if len(tokens) > 200:
            minimized_queries.append(q)
            continue
        
        if id == 0:
            reduced_tokens = ddmin(
                tokens,
                lambda new_tokens: check_query_tokens(new_tokens, i, minimized_queries, queries, test)
            )
        elif id == 1:
            reduced_tokens = ddmin2(
                tokens,
                lambda new_tokens: check_query_tokens(new_tokens, i, minimized_queries, queries, test)
            )
        else:
            reduced_tokens = ddmin3(
                tokens,
                lambda new_tokens: check_query_tokens(new_tokens, i, minimized_queries, queries, test)
            )
        if reduced_tokens:
            minimized_queries.append(' '.join(reduced_tokens))

    return minimized_queries

def ddmin(tokens, test):
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

def ddmin2(tokens, test):
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

def ddmin3(tokens, test):
    chunk_size = 3

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
    query = "INSERT INTO t5 ( c0 , c1 , c2 ) VALUES ( '-u!.j!p2P rZ' , -7608565 , 'etWPioGcJ-r9' ) , ( 'aFGxdW5PRFhKFE' , -2444631 , 'H4 , C0XvPP8W8Yq' ) , ( 'aZbvf3I7q , AKk' , -1947983 , '?l , nL9H' ) , ( 'N1L0J1RZsIbuJ0!g' , 923750 , 'zo2oR' ) , ( '_JB5-Z!' , -2877915 , 'dD0J0N-77?0Uo1e , ' ) , ( 'Q_YOxU o' , 4636258 , 'Z_SRJld5gVFP' ) , ( 'mNlEgKF2!' , -584367 , 'wx_TvWXV' ) , ( 'CuA1emC-R' , 1509128 , 'MDhbR RClrvKHOzN' ) , ( 'mFCgpHaC-_' , 3353807 , 'usA8w_mSpY?_ TL' ) , ( 'v , v77liG' , 4349642 , 'DXPmTZ.P2!Kx' ) , ( 'crx8flX , g02DPmA' , -1990450 , '8GrbC9U_7Xhk' ) , ( 'G-.Xe7' , 684380 , 'UrnRAA2g3VUV3 ' ) , ( 'Q9AVRo28eIMSq' , 3937253 , '9l4xWb.nvV?BgZ4c' ) , ( '9pYLxycVQp6B' , 2005450 , ' , ?!lycuysWJ' ) , ( 'tHF9J0B9' , -5586608 , 'z.jePZ7T-' ) , ( 'FawzS_iRS82M' , -5313409 , 'JqWLoScb' ) , ( '20EMpwWKwVfC8' , 7771574 , 'e65r1Y' ) , ( 'Tz_CJnPo7 , BwVipSd' , 2940263 , 'QtbpQ?E1w4Z.HCxWFM!' ) , ( 'kya9nkIDJqikr9' , 6088621 , 'NuK?YToc7mCYQ6FBZo' ) , ( 'j_jxija' , -2685025 , ' QedKsQ_yLuMR.Y' ) , ( '05ytv3iYVR4iSWbaJ3da' , -6561348 , 'sJG9T0M' ) , ( 'z2p_pAo_DI8FfutF0' , -1470225 , 'ZhJicQ' ) , ( 'de GCJAmNWk1do7XAN?J' , 6645446 , 'Z_WaSBSPcXmzWvDYb!k' ) , ( 'LSYOD5bxRq3.' , 2778922 , 'vbB6VDNCblDJ5D!fLvf' ) , ( 'KUJR-z6b72tA , 5j' , 3359107 , '?1mic' ) , ( 'bT-Nxqsh' , -1028521 , 'g2SsLta , KC fnlM!EITo' ) , ( '?m9n_o' , -5416584 , 'o2QDi5yH!b' ) , ( 'Dr2T2W2' , -8072188 , 'Vut!Zi4?' ) , ( 'tfU3oMigZU1ZvvCdAmU6' , 5315105 , 't-Vazhk' ) , ( 'xXF-q2Re' , -8369543 , 'fJ8_HW7Lf' ) , ( 'tD5dr NbhE' , 83100 , '2!mmVhgBRvs' ) , ( '5QC6bWkkO?i' , 1579836 , 'LH-Jz G' ) , ( '!A1rZ5g!-HGDXSvct' , 930168 , 'yIioLV' ) , ( 'mKUBXhc6m3D- cn' , -901863 , 'pnscE' ) , ( 'Z43 , OzKdAwr' , 8124048 , '-p0kCOmOi4iLdqHsqKS' ) , ( 'dthFdZTo' , 5302984 , 'M!x_f1QuMqSo' ) , ( '3E75kVioit , ' , 3912383 , 'UiE5FLuqN6' ) , ( 'V?KxKy2FWQJ7deO!sb?f' , 5112056 , 'LRsV3glE4Y?' ) , ( 'MV6H7EW , 4-Q' , 500598 , 'iKbYn4mYe.mz6H!OfRi' ) , ( 'wZfDq2b1t8' , -5392615 , '?XZp.GNkqxYR4' ) , ( 'gcam8ECh9GLNzzO?' , 7774759 , 'MU3s2fK' ) , ( 'dG8_2O__cSqB.3' , -5546456 , 'z2 , o9Fp.q6fRfJ' ) , ( '7tiG-YFjdHP9D9' , -7571907 , 'Zi9ZSyH5cf' ) , ( 'mgpktsi4IpsLnYx.E' , -4702394 , 'b?S1ByB' ) ;"
    
    queries = [q + ";" for q  in query.split(";")]
    new = transform_insert_statements(queries)
    for q in new:
        print(q)
    return
    
if __name__ == "__main__":
    main()