from tqdm import tqdm
from test import coverage_test
import generator as gen
import random

random.seed(42)

COMPACT = 5000
def compact_queries(strings: list[str], max_length: int) -> list[str]:
    result = []
    current = ""

    for s in strings:
        if len(current) + len(s) <= max_length:
            current += s
        elif current:
            result.append(current)
            current = s
        else:
            current = s

    if current:
        result.append(current)

    return result

class Fuzzing:
    """
    Fuzzing pipeline 
    """
    def __init__(self, name, gen_fn, threshold=5, max=100, needs_table=True, other_tables=False, 
                 gen_table=False, rem_table=False):
        self.name = name
        self.gen_fn = gen_fn
        self.threshold = threshold
        self.max = max
        self.needs_table = needs_table 
        self.other_tables = other_tables
        self.gen_table = gen_table
        self.rem_table = rem_table

    def gen_valid_query(self, query: list, table: gen.Table, tables: list):
        for _ in range(self.max):
            if self.other_tables:
                node = self.gen_fn.random(table, other_tables=tables)
            elif self.needs_table:
                node = self.gen_fn.random(table)
            else:
                node = self.gen_fn.random()
            new_query = node.sql()
            cov_valid, msg = coverage_test(query + [new_query])
            if "Error" in msg:
                with open("error.txt", "a") as f:
                    f.write(f"Query: {query+[new_query]}\nMessage: {msg}\n\n")
            if cov_valid > 0:
                return cov_valid, [new_query], node
            
        return 0, [], None

    def generate(self, cov: int, init_query: list, tables: list, find_best: bool = False):
        pbar = tqdm(total=self.threshold, desc=f"{self.name} (cov={cov}) (query={len(init_query)})")

        tries = 0
        best_cov = cov
        new_query = init_query
        updated_tables = list(tables)

        while tries < self.threshold:
            if tables:
                table = random.choice(updated_tables)
                cov_valid, valid_query, node = self.gen_valid_query(init_query, table, updated_tables)
            else:
                cov_valid, valid_query, node = self.gen_valid_query(init_query, None, updated_tables)
            combined_query = (init_query if find_best else new_query) + valid_query
            combined_cov, _ = coverage_test(combined_query)

            if combined_cov > best_cov:
                best_cov = combined_cov
                new_query = combined_query

                if self.gen_table: # view, alter table
                    updated_tables.append(node)
                if self.rem_table: # alter table
                    updated_tables.remove(table)

                tries = 0
                pbar.set_description(f"{self.name} (cov={best_cov}) (query={len(combined_query)})")
            else:
                tries += 1

            pbar.update(1)

        pbar.close()
        return best_cov, new_query, updated_tables

def run_pipeline(init_cov: int, init_query: list, init_tables: list, fuzz_pipeline: list[Fuzzing]):
    cov = init_cov
    query = init_query
    tables = init_tables
    repeat = 5

    for i in range(repeat):
        for stage in fuzz_pipeline:
            cov, query, tables = stage.generate(cov, query, tables)

        with open(f"save_{i}.txt", "w") as f:
            f.write(f"Best Coverage: {cov}\n")
            f.write(f"Best Query: {query}\n")
            f.write(f"Tables: {tables}\n")

    return cov, query, tables


if __name__ == "__main__":
    table1 = gen.Table.random()
    table2 = gen.Table.random()
    fuzz_pipeline = [
        Fuzzing("Table", gen.Table, gen_table=True, needs_table=False),
        Fuzzing("Insert", gen.Insert), #, only_tables=True),
        Fuzzing("Update", gen.Update), #, only_tables=True),
        Fuzzing("Select", gen.Select, other_tables=True, threshold=10),
        Fuzzing("With", gen.With, threshold=10),
        Fuzzing("Trigger", gen.Trigger), #, only_tables=True),
        Fuzzing("Index", gen.Index), #, only_tables=True),
        Fuzzing("Pragma", gen.Pragma, needs_table=False),
        Fuzzing("AlterTable", gen.AlterTable), #, only_tables=True),
        Fuzzing("View", gen.View),
        Fuzzing("Select", gen.Select, other_tables=True, threshold=10),
        Fuzzing("With", gen.With, threshold=10),
        Fuzzing("Delete", gen.Delete), #, only_tables=True, max=4),
        Fuzzing("Replace", gen.Replace) #, max=4)
    ]
    cov, query, tables = run_pipeline(0, [], [], fuzz_pipeline)
    print(f"Final Coverage: {cov}")
