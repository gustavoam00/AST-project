#!/usr/bin/env python3
import argparse, subprocess, os, time
from src.vertical_reduction import trace_context, vertical_delta_debug, reduce_temp_tables, compress_insert
from src.horizontal_reduction import cleaning_pipeline, horizontal_delta_debug, space_it_out, cleaning_by_query, minimize_columns, try_remove_parens, try_simplify_cases
from src.initial_test import initial_run_test
from src.config import INFO_OUTPUT, TEMP_OUTPUT
from src.helper import get_queries, write_queries, flatten

def run_test_script(test: str, query_path: str, queries: list[str] = []):
    if queries:
        os.makedirs(os.path.dirname(query_path), exist_ok=True)
        with open(query_path, "w") as f:
            for query in queries:
                f.write(query + "\n")
    try:
        result = subprocess.run(
            [test],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print("Test script timed out.")
        return False

def main():
    parser = argparse.ArgumentParser(description="SQL Query Reducer")
    parser.add_argument("--query", required=True, help="Path to SQL query file")
    parser.add_argument("--test", required=True, help="Shell script to test for the bug")
    args = parser.parse_args()

    # PATH SETUP
    original_path = str(args.query)
    default_reduced_path = os.path.join(os.path.dirname(args.test), "query.sql")
    reduced_path  = os.environ.get("TEST_CASE_LOCATION", default_reduced_path)

    info_path = os.path.join(INFO_OUTPUT, "info.txt")
    os.makedirs(os.path.dirname(info_path), exist_ok=True)
    if os.path.exists(info_path):
        os.remove(info_path)

    os.makedirs(TEMP_OUTPUT, exist_ok=True)

    # REDUCTION
    start = time.time()
    first_time = start
    queries = get_queries(original_path)
    save_query = [space_it_out(q) for q in queries]

    write_queries(reduced_path, save_query)
    index, errlist, msg = initial_run_test(save_query, "reduce")
    with open(info_path, "w") as f:
        f.write(str(index) + "\n")
        f.write(" ".join(msg[0].split()).strip() + "\n")
        f.write(" ".join(msg[1].split()).strip() + "\n")
        f.writelines(line + "\n" for line in errlist)
    end = time.time()
    init_query = len(save_query)
    init_token = len(flatten([q.split(" ") for q in save_query]))
    print(f"INITIAL RUN ({end - start:.3f}s):", len(save_query), "(number of queries)", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    
    # TRACE CONTEXT REDUCTION
    start = time.time()
    setup_queries, context_queries, bug_query = trace_context(save_query, index, errlist, msg)
    write_queries(reduced_path, setup_queries + context_queries + bug_query)
    if not run_test_script(args.test, reduced_path):
        write_queries(reduced_path, save_query)
        end = time.time()
        print(f"FAILED: TRACE CONTEXT ({end - start:.3f}s):", len(save_query), "(number of queries)", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    else:
        save_query = setup_queries + context_queries + bug_query
        end = time.time()
        print(f"SUCCESS: TRACE CONTEXT ({end - start:.3f}s):", len(save_query), "(number of queries)", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")

    # FIRST DELTA DEBUG REDUCTION
    start = time.time()
    if len(queries) > 50:
        delta_queries = setup_queries + vertical_delta_debug(setup_queries, context_queries, bug_query, lambda q: run_test_script(args.test, reduced_path, q), n=2) + bug_query
    else:
        delta_queries = vertical_delta_debug([], save_query, [], lambda q: run_test_script(args.test, reduced_path, q), n=2)
    write_queries(reduced_path, delta_queries)
    if not run_test_script(args.test, reduced_path):
        write_queries(reduced_path, save_query)
        end = time.time()
        print(f"FAILED: FIRST VERTICAL DELTA DEBUG ({end - start:.3f}s):", len(save_query), "(number of queries)", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    else:
        save_query = delta_queries
        end = time.time()
        print(f"SUCCESS: FIRST VERTICAL DELTA DEBUG ({end - start:.3f}s):", len(save_query), "(number of queries)", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")

    # QUERY CLEAN UP REDUCTION
    start = time.time()
    cleaned_queries = [cleaning_pipeline(q) for q in save_query]
    write_queries(reduced_path, cleaned_queries)
    cleaned_fail = False
    if not run_test_script(args.test, reduced_path):
        write_queries(reduced_path, save_query)
        cleaned_fail = True
        end = time.time()
        print(f"FAILED: FIRST CLEANED ({end - start:.3f}s):", len(save_query), "(number of queries)", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    else:
        save_query = cleaned_queries
        end = time.time()
        print(f"SUCCESS: FIRST CLEANED ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
        
    if cleaned_fail and len(save_query) <= 10: #tries to clean line by line, 10 line check for now just in case
        start = time.time()
        cleaned_queries = cleaning_by_query(save_query,  lambda q: run_test_script(args.test, reduced_path, q))
        write_queries(reduced_path, cleaned_queries)
        if not run_test_script(args.test, reduced_path):
            write_queries(reduced_path, save_query)
            end = time.time()
            print(f"FAILED: FIRST CLEANED AGAIN ({end - start:.3f}s):", len(save_query), "(number of queries)", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
        else:
            save_query = cleaned_queries
            end = time.time()
            print(f"SUCCESS: FIRST CLEANED AGAIN ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    
    start = time.time()
    case_queries = try_simplify_cases(save_query, lambda q: run_test_script(args.test, reduced_path, q))
    write_queries(reduced_path, case_queries)
    if not run_test_script(args.test, reduced_path):
        write_queries(reduced_path, save_query)
        cleaned_fail = True
        end = time.time()
        print(f"FAILED: CASE REDUCTION ({end - start:.3f}s):", len(save_query), "(number of queries)", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    else:
        save_query = case_queries
        end = time.time()
        print(f"SUCCESS: CASE REDUCTION ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    
    # CLEANING
    start = time.time()
    cleaned_queries: list[str] = []
    for query in save_query:
        if query.startswith("WITH"):
            cleaned_queries.append(reduce_temp_tables(query))
        else:
            cleaned_queries.append(query)
    cleaned_queries = [space_it_out(q) for q in cleaned_queries]
    write_queries(reduced_path, cleaned_queries)
    if not run_test_script(args.test, reduced_path):
        write_queries(reduced_path, save_query)
        end = time.time()
        print(f"FAILED: REDUCTION ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    else:
        save_query = cleaned_queries
        end = time.time()
        print(f"SUCCESS: REDUCTION ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")

    # SECOND DELTA DEBUG REDUCTION
    if len(queries) > 50 and len(save_query) < 30:
        start = time.time()
        delta_queries = vertical_delta_debug([], save_query, [], lambda q: run_test_script(args.test, reduced_path, q), n=2)
        write_queries(reduced_path, delta_queries)
        if not run_test_script(args.test, reduced_path):
            write_queries(reduced_path, save_query)
            end = time.time()
            print(f"FAILED: SECOND VERTICAL DELTA DEBUG ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
        else:
            save_query = delta_queries
            end = time.time()
            print(f"SUCCESS: SECOND VERTICAL DELTA DEBUG ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")

    # INSERT COMPRESSION
    start = time.time()
    cleaned_queries = compress_insert(save_query)
    cleaned_queries = [space_it_out(q) for q in cleaned_queries]
    write_queries(reduced_path, cleaned_queries)
    if not run_test_script(args.test, reduced_path):
        write_queries(reduced_path, save_query)
        end = time.time()
        print(f"FAILED: FIRST INSERT COMPRESSION ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    else:
        save_query = cleaned_queries
        end = time.time()
        print(f"SUCCESS: FIRST INSERT COMPRESSION ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    
    #COLUMN REMOVER
    start = time.time()
    min_cols_queries = minimize_columns(save_query, lambda q: run_test_script(args.test, reduced_path, q))
    write_queries(reduced_path, min_cols_queries)
    if not run_test_script(args.test, reduced_path):
        write_queries(reduced_path, min_cols_queries)
        end = time.time()
        print(f"FAILED: FIRST COLUMN MINIMIZER ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    else:
        save_query = min_cols_queries
        end = time.time()
        print(f"SUCCESS: FIRST COLUMN MINIMIZER ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")

    #HORIZONTAL DELTA DEBUG
    start = time.time()
    delta_queries = horizontal_delta_debug(save_query, lambda q: run_test_script(args.test, reduced_path, q), id=1)
    write_queries(reduced_path, delta_queries)
    if not run_test_script(args.test, reduced_path):
        write_queries(reduced_path, save_query)
        end = time.time()
        print(f"FAILED: HORIZONTAL DELTA DEBUG ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    else:
        save_query = delta_queries
        end = time.time()
        print(f"SUCCESS: HORIZONTAL DELTA DEBUG ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
         
    #SLIDING WINDOW DEBUGGER
    start = time.time()
    delta_queries = horizontal_delta_debug(save_query, lambda q: run_test_script(args.test, reduced_path, q), id=2)
    write_queries(reduced_path, delta_queries)
    if not run_test_script(args.test, reduced_path):
        write_queries(reduced_path, save_query)
        end = time.time()
        print(f"FAILED: SLIDING WINDOW DEBUG ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    else:
        save_query = delta_queries
        end = time.time()
        print(f"SUCCESS: SLIDING WINDOW DEBUG ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    
    #PARENTHESIS REMOVAL
    start = time.time()
    paren_queries = try_remove_parens(save_query, lambda q: run_test_script(args.test, reduced_path, q))
    write_queries(reduced_path, paren_queries)
    if not run_test_script(args.test, reduced_path):
        write_queries(reduced_path, save_query)
        end = time.time()
        print(f"FAILED: PARENTHESIS REMOVAL ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    else:
        save_query = paren_queries
        end = time.time()
        print(f"SUCCESS: PARENTHESIS REMOVAL ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")

    start = time.time()
    cleaned_queries = compress_insert(save_query)
    cleaned_queries = [space_it_out(q) for q in cleaned_queries]
    write_queries(reduced_path, cleaned_queries)
    if not run_test_script(args.test, reduced_path):
        write_queries(reduced_path, save_query)
        end = time.time()
        print(f"FAILED: SECOND INSERT COMPRESSION ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    else:
        save_query = cleaned_queries
        end = time.time()
        print(f"SUCCESS: SECOND INSERT COMPRESSION ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    
    start = time.time()
    min_cols_queries = minimize_columns(save_query, lambda q: run_test_script(args.test, reduced_path, q))
    write_queries(reduced_path, min_cols_queries)
    if not run_test_script(args.test, reduced_path):
        write_queries(reduced_path, min_cols_queries)
        end = time.time()
        print(f"FAILED: SECOND COLUMN MINIMIZER ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    else:
        save_query = min_cols_queries
        end = time.time()
        print(f"SUCCESS: SECOND COLUMN MINIMIZER ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")

    # QUERY CLEAN UP REDUCTION 2nd
    start = time.time()
    cleaned_queries = [cleaning_pipeline(q) for q in save_query]
    write_queries(reduced_path, cleaned_queries)
    cleaned_fail = False
    if not run_test_script(args.test, reduced_path):
        write_queries(reduced_path, save_query)
        cleaned_fail = True
        end = time.time()
        print(f"FAILED: SECOND CLEANED ({end - start:.3f}s):", len(save_query), "(number of queries)", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    else:
        save_query = cleaned_queries
        end = time.time()
        print(f"SUCCESS: SECOND CLEANED ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
        
    if cleaned_fail and len(save_query) <= 10: #tries to clean line by line, 10 line check for now just in case
        start = time.time()
        cleaned_queries = cleaning_by_query(save_query,  lambda q: run_test_script(args.test, reduced_path, q))
        write_queries(reduced_path, cleaned_queries)
        if not run_test_script(args.test, reduced_path):
            write_queries(reduced_path, save_query)
            end = time.time()
            print(f"FAILED: SECOND CLEANED AGAIN ({end - start:.3f}s):", len(save_query), "(number of queries)", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
        else:
            save_query = cleaned_queries
            end = time.time()
            print(f"SUCCESS: SECOND CLEANED AGAIN ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
            
    # TRACE CONTEXT REDUCTION
    start = time.time()
    setup_queries, context_queries, bug_query = trace_context(save_query, len(save_query)-1, errlist, msg)
    write_queries(reduced_path, setup_queries + context_queries + bug_query)
    if not run_test_script(args.test, reduced_path):
        write_queries(reduced_path, save_query)
        end = time.time()
        print(f"FAILED: TRACE CONTEXT ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")
    else:
        save_query = setup_queries + context_queries + bug_query
        end = time.time()
        print(f"SUCCESS: TRACE CONTEXT ({end - start:.3f}s):", len(save_query), "(number of queries),", len(flatten([q.split(" ") for q in save_query])), "(number of tokens)")

    # FINAL RESULT FOR DEBUGGING
    run_test_script(args.test, reduced_path)
    end_time = time.time()
    print(f"TOTAL RUN TIME: {end_time - first_time:.3f}s")
    print(f"FINAL NUMBER OF QUERIES: {len(save_query)} from {init_query}")
    print(f"FINAL NUMBER OF TOKENS: {len(flatten([q.split(' ') for q in save_query]))} from {init_token}")

    
if __name__ == "__main__":
    main()
