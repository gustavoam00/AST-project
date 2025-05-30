import re
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

        TYPE_PATTERN = re.compile(r"^(INTEGER|TEXT|REAL|BOOLEAN|BLOB|NUMERIC)$", re.IGNORECASE)

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

def remove_useless_parentheses(s):
    keyword_pattern = '|'.join(KEYWORDS)
    pattern = re.compile(r'\(\s*([^\s()]+)\s*\)')

    def is_useless(match):
        start = match.start()
        before = s[:start].rstrip()

        # Look behind for keyword + optional space
        for kw in KEYWORDS:
            if re.search(rf'\b{kw}\s*$', before, re.IGNORECASE):
                return False

        # Look for ")," before the match
        if re.search(r'\)\s*,\s*$', before):
            return False

        return True

    matches = list(pattern.finditer(s))
    new_s = s
    offset = 0

    for match in matches:
        if is_useless(match):
            span = match.span()
            # Replace the whole "(token)" with just "token"
            token = match.group(1)
            start, end = span[0] + offset, span[1] + offset
            new_s = new_s[:start] + token + new_s[end:]
            offset += len(token) - (end - start)

    return new_s

def space_it_out(query):
    query = query.replace('(', ' ( ').replace(')', ' ) ')
    query = query.replace(';', ' ; ').replace(',', ' , ')
    query = re.sub(r'\s+', ' ', query)
    query = query.strip()
    return query

def cleaning_pipeline(query):
    new_query = space_it_out(query)
    new_query = clean_semicolons(new_query)
    new_query = simplify_sql(new_query)
    new_query = remove_nested_parentheses(new_query)
    new_query = strip_column_types(new_query)
    new_query = strip_select(new_query)
    new_query = remove_functions(new_query)
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

def delta_debug(queries, test):
    minimized_queries = []

    for i, q in enumerate(queries):
        tokens = q.split()
        reduced_tokens = ddmin(
            tokens,
            lambda new_tokens: check_query_tokens(new_tokens, i, queries, test)
        )
        if reduced_tokens:
            minimized_queries.append(' '.join(reduced_tokens))

    return minimized_queries

def check_query_tokens(new_tokens, idx, queries, test):
    rebuilt = []
    for j, q in enumerate(queries):
        if j == idx:
            rebuilt.append(' '.join(new_tokens))
        else:
            rebuilt.append(q.strip())
    return test(rebuilt)

def ddmin(tokens, test):
    n = 2
    length = len(tokens)

    while length >= 1:
        start = 0
        subset_found = False
        chunk_size = max(1, len(tokens) // n)

        while start < len(tokens):
            end = min(start + chunk_size, len(tokens))
            reduced = tokens[:start] + tokens[end:]

            if test(reduced):
                tokens = reduced
                length = len(tokens)
                n = max(n - 1, 2)
                subset_found = True
                break
            start = end

        if not subset_found:
            if n >= len(tokens):
                break
            n = min(n * 2, len(tokens))

    return tokens

    

def main():
    query = "SELECT * FROM tbl0 WHERE ( NOT ( ( ( CAST ( tbl0.c1 AS BLOB ) ) - ( tbl0.c0 NOTNULL ) ) % ( tbl0.c2 ) ) ) NOT IN ( ( ( ( ( tbl0.c0 ) <> ( tbl0.c0 ) ) NOT BETWEEN ( TYPEOF ( tbl0.c2 ) ) AND ( CAST ( tbl0.c2 AS INT ) ) ) LIKE ( ( x'7B85F7B0' ) IS NOT ( ( tbl0.c1 ) AND ( tbl0.c0 ) AND ( tbl0.c2 ) OR ( tbl0.c0 ) OR ( tbl0.c0 ) ) ) ) NOT BETWEEN ( ( ( TYPEOF ( tbl0.c1 ) ) % ( CAST ( tbl0.c1 AS INT ) ) ) / ( ( CAST ( tbl0.c2 AS BLOB ) ) OR ( TYPEOF ( tbl0.c2 ) ) OR ( TYPEOF ( tbl0.c0 ) ) OR ( ( tbl0.c2 ) = ( tbl0.c2 ) ) ) ) AND ( ( 'y5db:DC4[J/t|D\z[w ; Len 6k.Hwp' ) BETWEEN ( TYPEOF ( tbl0.c1 ) ) AND ( ( tbl0.c0 ) != ( tbl0.c1 ) ) IS FALSE ) ) ;"
    new_query = cleaning_pipeline(query)
    new_query = aggressive_cleaning_pipeline(query)
    print(new_query)
    return
    
if __name__ == "__main__":
    main()