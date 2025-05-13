import re
import docker
import logging
from tqdm import tqdm
from collections import Counter
import sqlglot
from sqlglot import parse_one, parse, errors, Expression
from sqlglot.expressions import Expression

DOCKER_IMAGE = "sqlite3-test"
SQLITE_VERSION = "sqlite3-3.39.4" 
INPUT_FILE = "evaluation/sampled_queries.txt"

SQL_CLAUSES = [
    "CREATE TABLE", "CREATE VIEW", "CREATE INDEX", "CREATE TRIGGER", 
    "ALTER", "DROP",
    
    "INSERT", "UPDATE", "DELETE", "REPLACE", 
    
    "SELECT", "FROM", "WITH", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "OFFSET",
    "LEFT JOIN", "INNER JOIN", "CROSS JOIN", 
    
    "PRAGMA",
    
]

def run_query(sql_query, sqlite_version):
    """
    Executes an SQL query using the specified SQLite version inside a Docker container.
    """
    client = docker.from_env()
    try:
        result = client.containers.run(
            DOCKER_IMAGE,
            command=f'/bin/bash -c "echo \\"{sql_query}\\" | /usr/bin/{sqlite_version}"',
            remove=True
        )
        return "  " + result.decode().strip().replace("\n", "\n  ")
    except Exception as e:
        logging.error(f"{sqlite_version}: {e}")
        return "ERROR" + str(e)

def validity_evaluation():
    """
    Tests the validity of all queries .
    """
    total_queries = 0
    error_count = 0
    log_file = "evaluation/validity_log.txt"

    with open(INPUT_FILE, "r", encoding="utf-8") as f, open(log_file, "w", encoding="utf-8") as log:
        queries = f.readlines()

        for i, query in enumerate(tqdm(queries, desc="Running queries"), start=1):
            query = query.strip()
            if not query:
                continue

            total_queries += 1
            result = run_query(query, SQLITE_VERSION)

            if "Error" in result or "ERROR" in result:
                error_count += 1
                log.write(f"\nError in query #{i}:\n")
                log.write(f"  Error: {result.strip()}\n")
            else:
                continue

        
        log.write("\n-------- Summary ----------\n")
        log.write(f"Total queries executed: {total_queries}\n")
        log.write(f"Total errors found: {error_count}\n")

def clause_evaluation():
    """
    Finds the frequency of clauses in all queries.
    """
    log_file = "evaluation/clauses_log.txt"
    clause_counter = Counter()
    clause_patterns = []

    for clause in SQL_CLAUSES:
        parts = clause.split()
        if len(parts) == 1:
            pattern = re.compile(r'\b' + re.escape(clause) + r'\b')
        else:
            pattern = re.compile(r'\b' + r'(?:\s+\w+|\s+){0,3}'.join(map(re.escape, parts)) + r'\b')
        clause_patterns.append((clause, pattern))

    total_queries = 0
    total_clause_matches = 0

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            query = line.strip()
            if not query:
                continue
            total_queries += 1
            clauses_in_this_query = 0
            for clause, pattern in clause_patterns:
                matches = pattern.findall(query)
                if matches:
                    count = len(matches)
                    clause_counter[clause] += count
                    clauses_in_this_query += count
            total_clause_matches += clauses_in_this_query

    average_clauses_per_query = (
        total_clause_matches / total_queries if total_queries > 0 else 0
    )

    with open(log_file, "w", encoding="utf-8") as log:
        log.write("SQL Clause Frequency Evaluation\n")
        log.write(f"Total Queries: {total_queries}\n")
        log.write(f"Average Clauses per Query: {average_clauses_per_query:.2f}\n")
        log.write("-------------------------------\n")
        for clause in SQL_CLAUSES:
            count = clause_counter.get(clause, 0)
            log.write(f"{clause}: {count}\n")

    return clause_counter


def depth_evaluation():
    """
    Finds the average, min, and max expression depth of all queries.
    """
    def filter_out_triggers(sql: str) -> str:
        """
        Helper. Filters out TRIGGER-related statements, which sqlglot can't parse.
        """
        statements = sql.split(';')
        filtered = [stmt for stmt in statements if 'TRIGGER' not in stmt.upper()]
        return ';'.join(filtered)

    def find_expression_depth(sql_query):
        """
        Helper. Computes expression depth using sqlglot AST.
        """
        try:
            expression = parse_one(sql_query)
        except errors.ParseError:
            return None

        def _depth(expr: Expression, current_depth: int = 1) -> int:
            if not isinstance(expr, Expression):
                return current_depth
            return max(
                (_depth(arg, current_depth + 1) for arg in expr.args.values() if isinstance(arg, Expression)),
                default=current_depth
            )

        return _depth(expression)

    def paren_depth(query: str) -> int:
        """
        Heleper. Estimate depth using parentheses.
        """
        max_depth = depth = 0
        for char in query:
            if char == '(':
                depth += 1
                max_depth = max(max_depth, depth)
            elif char == ')':
                depth = max(depth - 1, 0)
        return max_depth + 1

    log_file = "evaluation/depth_log.txt"
    ast_depths = []
    paren_depths = []

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            query = filter_out_triggers(line.strip())
            if not query:
                continue

            ast_depth = find_expression_depth(query)
            if ast_depth is not None:
                ast_depths.append(ast_depth)

            paren_depths.append(paren_depth(query))

    
    ast_avg = sum(ast_depths) / len(ast_depths) if ast_depths else 0
    ast_min = min(ast_depths) if ast_depths else 0
    ast_max = max(ast_depths) if ast_depths else 0

    paren_avg = sum(paren_depths) / len(paren_depths) if paren_depths else 0
    paren_min = min(paren_depths) if paren_depths else 0
    paren_max = max(paren_depths) if paren_depths else 0

    with open(log_file, 'w', encoding='utf-8') as log:
        log.write("SQL Expression Depth Evaluation\n")

        log.write("----- SyntaxTree-Based Depth -----\n")
        log.write(f"Processed {len(ast_depths)} queries\n\n")
        log.write(f"Average: {ast_avg:.2f}\n")
        log.write(f"Minimum: {ast_min}\n")
        log.write(f"Maximum: {ast_max}\n\n")

        log.write("----- Parentheses-Based Depth -----\n")
        log.write(f"Processed {len(paren_depths)} queries\n\n")
        log.write(f"Average: {paren_avg:.2f}\n")
        log.write(f"Minimum: {paren_min}\n")
        log.write(f"Maximum: {paren_max}\n")

if __name__ == "__main__":
    depth_evaluation()
    # clause_evaluation()
    # validity_evaluation()
    pass