import re
import sys
from src.reduce import check_query_similarity, load_query_and_oracle, run_query2

def simplify_sql(query):
    def try_eval_math(expr):
        try:
            expr = expr.replace('++', '+').replace('--', '+').replace('+-', '-').replace('-+', '-')
            return str(eval(expr, {"__builtins__": None}, {}))
        except:
            return None
        
    def try_eval_bool(expr):
        expr = expr.strip()
        lowered = expr.lower()
        normalized = " ".join(lowered.split())

        if normalized == "not true":
            return "false"
        if normalized == "not false":
            return "true"

        match = re.fullmatch(
            r'(true|false|[-+]?\d+(?:\.\d+)?(?:e[-+]?\d+)?)[ ]*'
            r'(=|<>|!=|<=|>=|<|>)[ ]*'
            r'(true|false|[-+]?\d+(?:\.\d+)?(?:e[-+]?\d+)?)',
            lowered
        )
        if match:
            lhs, op, rhs = match.groups()
            lhs = lhs.replace('true', 'True').replace('false', 'False')
            rhs = rhs.replace('true', 'True').replace('false', 'False')
            py_op = op.replace('=', '==').replace('<>', '!=')

            try:
                result = eval(f"{lhs} {py_op} {rhs}", {"__builtins__": None}, {})
                return 'true' if result else 'false'
            except:
                return None

        return None
    
    #
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
                query = query[:match.start()] + result + query[match.end():]
                changed = True
                break

        if not changed:
            break

    return query


def strip_column_types(query):
    match = re.search(r"^(CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+\w+\s*)\((.*)\)", query, re.DOTALL)
    if not match:
        return query  # Not a valid CREATE TABLE

    header = match.group(1)
    body = match.group(2)
    print(body)
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

    TYPE_PATTERN = re.compile(r"^(INTEGER|TEXT|REAL|BOOLEAN|BLOB)$")

    stripped_columns = []

    for col in columns:
        parts = col.strip().split()
        if not parts:
            continue

        col_name = parts[0]
        rest = parts[1:]

        constraints = []

        for token in rest:
            if TYPE_PATTERN.match(token):
                continue
            else:
                constraints.append(token)

        constraint_str = ' '.join(constraints).strip()
        stripped_line = f"{col_name} {constraint_str}".strip()
        stripped_columns.append(stripped_line)

    return f"{header}(" + ", ".join(stripped_columns) + ");"

def strip_select(query):
    def remove_clause(query, clause_keyword):
        clause_boundaries = [
            r"\bWHERE\b",
            r"\bGROUP\s+BY\b",
            r"\bHAVING\b",
            r"\bORDER\s+BY\b",
            r"\bPARTITION\s+BY\b"
            r"\bLIMIT\b",
            r"\bOFFSET\b",
            r"\bFETCH\b",
            r"\bFOR\b",
            r"\bUNION\b",
            r"\bINTERSECT\b",
            r"\bEXCEPT\b",
            r"\)",
            r";",
        ]
        boundary_pattern = '|'.join(clause_boundaries)
        pattern = rf"(?i)\b{clause_keyword}\b\s+.*?(?=({boundary_pattern})|$)"
        return re.sub(pattern, "", query, flags=re.DOTALL)
    
    if not re.match(r"(?i)^\s*SELECT\b", query.strip()):
        return query

    query = remove_clause(query, "ORDER BY")
    query = remove_clause(query, "GROUP BY")
    # query = remove_clause(query, "PARTITION BY")
    # query = remove_clause(query, "HAVING")
    query = remove_clause(query, "LIMIT")
    query = remove_clause(query, "OFFSET")

    query = re.sub(r"\s+", " ", query)
    query = re.sub(r"\s*;\s*", ";", query).strip()

    return query




def main():
    query, type = load_query_and_oracle(sys.argv[1])
    print(type)
    print('====')
    out0, err0 = run_query2(query, 0)
    out1, err1 = run_query2(query, 1)
    print(out0)
    print('----')
    print(err0)
    print('====')
    print(out1)
    print('----')
    print(err1)
    
if __name__ == "__main__":
    main()