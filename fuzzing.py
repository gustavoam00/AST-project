from tqdm import tqdm
from test import coverage_test
import generator as gen
import random
from config import TEST_FOLDER, SEED, PROB_TABLE

random.seed(SEED)

FUZZING_PIPELINE = lambda x: [
    Fuzzing("Table", gen.Table, gen_table=True, needs_table=False, need_prob=False),
    Fuzzing("AlterTable", gen.AlterTable, gen_table=True, rem_table=True, need_prob=False), 
    #Fuzzing("View", gen.View, gen_table=True, prob=x),
    Fuzzing("Insert", gen.Insert, mod_table=True, prob=x),
    Fuzzing("Update", gen.Update, mod_table=True, prob=x, max=5),
    Fuzzing("Select", gen.Select, other_tables=True, threshold=10, prob=x),
    Fuzzing("With", gen.With, threshold=10, prob=x),
    Fuzzing("Trigger", gen.Trigger, prob=x),
    Fuzzing("Index", gen.Index, prob=x),
    Fuzzing("Pragma", gen.Pragma, needs_table=False, need_prob=False),
    #Fuzzing("AlterTable", gen.AlterTable, gen_table=True, rem_table=True, need_prob=False), 
    Fuzzing("View", gen.View, gen_table=True, prob=x),
    Fuzzing("Select", gen.Select, other_tables=True, threshold=10, prob=x),
    Fuzzing("With", gen.With, threshold=10, prob=x),
    Fuzzing("Delete", gen.Delete, mod_table=True, prob=x), 
    Fuzzing("Replace", gen.Replace, mod_table=True, prob=x) 
]

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
                 gen_table=False, rem_table=False, need_prob=True, mod_table=False, prob=None):
        self.name = name
        self.gen_fn = gen_fn
        self.threshold = threshold
        self.max = max
        self.needs_table = needs_table 
        self.other_tables = other_tables
        self.gen_table = gen_table
        self.rem_table = rem_table
        self.need_prob = need_prob
        self.mod_table = mod_table
        self.prob = prob

    def get_random(self, table: gen.Table, tables: list):
        if self.need_prob:
            if self.other_tables:
                node = self.gen_fn.random(table, other_tables=tables, param_prob=self.prob)
            elif self.needs_table:
                node = self.gen_fn.random(table, param_prob=self.prob)
            else:
                node = self.gen_fn.random(param_prob=self.prob)
        else:
            if self.other_tables:
                node = self.gen_fn.random(table, other_tables=tables)
            elif self.needs_table:
                node = self.gen_fn.random(table)
            else:
                node = self.gen_fn.random()
        return node

    def gen_valid_query(self, query: list, table: gen.Table, tables: list):
        for _ in range(self.max):
            node = self.get_random(table, tables)
            new_query = node.sql() + ";"
            cov_valid, msg = coverage_test(query + [new_query])
            if "Error" in msg:
                with open(TEST_FOLDER + "error.txt", "a") as f:
                    f.write(f"Query: {query + [new_query]}\nMessage: {msg}\n\n")
            if cov_valid > 0:
                return cov_valid, [new_query], node
            
        return 0, [], None

    def generate(self, cov: int, init_query: list, tables: list, nodes: list, find_best: bool = False):
        pbar = tqdm(total=self.threshold, desc=f"{self.name} (cov={cov}) (query={len(init_query)})")

        tries = 0
        best_cov = cov
        new_query = init_query
        updated_tables = list(tables)
        best_nodes = nodes

        while tries < self.threshold:
            if tables:
                table = random.choice(updated_tables)
                while self.mod_table and isinstance(table, gen.View):
                    table = random.choice(updated_tables)
                cov_valid, valid_query, node = self.gen_valid_query(init_query, table, updated_tables)
            else:
                cov_valid, valid_query, node = self.gen_valid_query(init_query, None, updated_tables)
            combined_query = (init_query if find_best else new_query) + valid_query
            combined_cov, _ = coverage_test(combined_query)

            if combined_cov > best_cov:
                best_cov = combined_cov
                new_query = combined_query
                best_nodes.append(node)

                if self.rem_table: # alter table
                    updated_tables = list(filter(lambda x: x.name != table.name, updated_tables))
                if self.gen_table: # view, alter table
                    updated_tables.append(node)
                    init_query += valid_query

                tries = 0
                pbar.set_description(f"{self.name} (cov={best_cov}) (query={len(combined_query)})")
            else:
                tries += 1

            pbar.update(1)

        pbar.close()
        return best_cov, new_query, updated_tables, best_nodes

def run_pipeline(init_cov: int, init_query: list, init_tables: list, init_nodes: list, fuzz_pipeline: list, repeat: int = 1):
    cov = init_cov
    query = init_query
    tables = init_tables
    nodes = init_nodes

    for i in range(repeat):
        for stage in fuzz_pipeline:
            cov, query, tables, nodes = stage.generate(cov, query, tables, nodes)

        with open(TEST_FOLDER + f"save_{i}.txt", "w") as f:
            f.write(f"Best Coverage: {cov}\n")
            f.write(f"Best Query: {query}\n")
            f.write(f"Tables: {tables}\n")
        with open(TEST_FOLDER + f"query_{i}.sql", "w") as f:
            f.write("\n".join(query))

    return cov, query, tables, nodes

if __name__ == "__main__":
    cov, query, tables, nodes = run_pipeline(0, [], [], [], FUZZING_PIPELINE(PROB_TABLE), repeat=5)
    print(f"Final Coverage: {cov}")
