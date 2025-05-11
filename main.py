import docker
import logging
from bugs import BUGS
from tqdm import tqdm
import generator as gen

logging.disable(logging.ERROR) 
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

SQLITE_VERSIONS = ["sqlite3-3.26.0", "sqlite3-3.39.4"]
DOCKER_IMAGE = "sqlite3-test"

def run_query(sql_query, sqlite_version):
    """
    Executes an SQL query using the specified SQLite version inside a Docker container.
    """
    sql_script = " ".join(sql_query)
    client = docker.from_env()
    try:
        result = client.containers.run(
            DOCKER_IMAGE,
            command=f'/bin/bash -c "echo \\"{sql_script}\\" | /usr/bin/{sqlite_version}"',
            remove=True
        )
        return "  " + result.decode().strip().replace("\n", "\n  ")
    except Exception as e:
        logging.error(f"{sqlite_version}: {e}")
        return str(e)

def test(query):
    """
    Compares query results between two SQLite versions.
    """
    res1 = run_query(query, SQLITE_VERSIONS[0])
    res2 = run_query(query, SQLITE_VERSIONS[1])

    logging.info(f"Version ({SQLITE_VERSIONS[0]}) - Table Result:\n{res1}")
    logging.info(f"Version ({SQLITE_VERSIONS[1]}) - Table Result:\n{res2}")

    if res1 != res2:
        logging.warning("Bug found!")
        logging.info("Query:\n  " + "\n  ".join(query))
    else:
        logging.info("No bug detected.")


if __name__ == "__main__":
    SQL_TEST_QUERY = [
        "CREATE TABLE t0(c0 INT);",
        "CREATE INDEX i0 ON t0(1) WHERE c0 NOT NULL;",
        "INSERT INTO t0 (c0) VALUES (0), (1), (2), (NULL), (3);",
        "SELECT c0 FROM t0 WHERE t0.c0 IS NOT 1;"
    ]
    #test(SQL_TEST_QUERY)
    # QUERY = "CREATE TABLE t0 (c0 INT); INSERT INTO t0 (c0) VALUES (0), (1), (2), (NULL), (3); CREATE VIEW v0 AS SELECT * FROM t0; ALTER TABLE t0 RENAME c0 TO c9;" #this is fine tho
    # print(run_query([QUERY], SQLITE_VERSIONS[0]))
    
    # BUGS[0:34]
    # #test(BUGS[0])

    prob = {   
        "table"  :  0.2,
        "alt_ren":  0.2, 
        "alt_add":  0.2,
        "alt_col":  0.2,
        "select1":  0.2,
        "select2":  0.2,
        "with":     0.2,
        "view":     0.2,
        "index":    1,
        "trigger":  0.2,
        "insert":   0.2,
        "update":   0.2,
        "replace":  0.2,
        "delete":   0.2,
        "pragma":   0.01,
        "control":  0.01,
        "optimize": 0.01,
        "drop_tbl": 0.05,
    }

    
    for _ in tqdm(range(100)):
        query = gen.randomQueryGen(param_prob=prob, debug=False, cycle=1)
        error = run_query([query], SQLITE_VERSIONS[0])
        
        if "Error" in error:
            print(error)
            # breakpoint()
            break
    
    
    # pbar.close()
    # pbar = tqdm(total = runs+1)
    # error = "constraint"
    # runs = 0    
    # while "constraint" in error or not error.strip() or not "Error" in error or "no such column" in error:
    #     query = gen.randomQueryGen(prob, debug=False, cycle=1)
    #     error = run_query([query], SQLITE_VERSIONS[0])
    #     pbar.update(1)
    # print(error)
    # pbar.close()

    # assert False
    # error = "constraint"
    # runs = 0
    # pbar = tqdm(total = runs+1)
    # while "constraint" in error or not error.strip() or not "Error" in error:
    #     test_query = ""
    #     table = gen.Table.random()
    #     test_query += table.sql() + " "
    #     insert = gen.Insert.random(table)
    #     test_query += insert.sql() + " "
    #     update = gen.Update.random(table)
    #     test_query += update.sql() + " "
    #     delete = gen.Delete.random(table)
    #     test_query += delete.sql() + " "
    #     table = gen.AlterTable.random_tbl_rename(table)
    #     test_query += table.sql() + " "
    #     table = gen.AlterTable.random_add(table)
    #     test_query += table.sql() + " "
    #     table = gen.AlterTable.random_col_rename(table)
    #     test_query += table.sql() + " "
    #     test_query += gen.Select.random(table).sql() + "; "
    #     test_query += gen.With.random(table).sql() + " "
    #     table = gen.View.random(table)
    #     test_query += table.sql() + " "
    #     table2 = gen.Table.random()
    #     test_query += table2.sql() + " "
    #     test_query += gen.Select.random(table, other_tables=[table2]).sql() + "; "
    #     test_query += gen.Trigger.random(table2).sql() + " "
    #     test_query += gen.Index.random(table2).sql() + " "
    #     test_query += gen.Replace.random(table2).sql() + " "
    #     test_query += gen.Pragma.random().sql() + " "
    #     error = run_query([test_query], SQLITE_VERSIONS[0])
    #     pbar.update(1)
    # print(error)
    # pbar.close()
    
    
    # # table_name = "test1"
    # # table = SQLiteTable(table_name)
    # # table.create(rows=100, max_cols=1)
    # # script = table.get_script()
    # # script_len = len(script)

    # # tables_cols = {}
    # # tables_cols[table_name] = table.get_cols()
    # # query_gen = SQLiteQuery([table_name], tables_cols)
    
    # # for i in tqdm(range(100000)):
    # #     query = query_gen.select(table_name, size=2)
    # #     script.append(query)
    # #     if i % 500 == 0:
    # #         test(script) #no bugs found
    # #         script = script[:script_len]
        





