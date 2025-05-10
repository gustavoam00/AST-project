from tqdm import tqdm
from local import coverage_test, reset
from metric import coverage_score, get_error
import generator as gen
import random
from config import TEST_FOLDER, SEED, PROB_TABLE

random.seed(SEED)

FUZZING_PIPELINE = lambda x: [
    Fuzzing("Table", gen.Table, gen_table=True, needs_table=False, need_prob=False),
    Fuzzing("View", gen.View, gen_table=True, other_tables=True, prob=x),
    Fuzzing("VirtualTable", gen.VirtualTable, gen_table=True, needs_table=False, need_prob=False),
    Fuzzing("AlterTable", gen.AlterTable, gen_table=True, commit=True, no_virt=True, mod_table=True, rem_table=True, need_prob=False), 
    Fuzzing("Insert", gen.Insert, mod_table=True, commit=True, prob=x),
    Fuzzing("Update", gen.Update, mod_table=True, commit=True, prob=x),
    Fuzzing("Select", gen.Select, other_tables=True, prob=x),
    Fuzzing("With", gen.With, prob=x),
    Fuzzing("Trigger", gen.Trigger, mod_table=True, commit=True, no_virt=True, prob=x),
    Fuzzing("Index", gen.Index, no_virt=True, commit=True, prob=x),
    Fuzzing("Pragma", gen.Pragma, needs_table=False, need_prob=False),
    Fuzzing("Delete", gen.Delete, mod_table=True, commit=True, prob=x), 
    Fuzzing("Replace", gen.Replace, mod_table=True, commit=True, prob=x) 
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
                 gen_table=False, rem_table=False, need_prob=True, no_virt=False, mod_table=False, 
                 commit=False, prob=None, corpus=[]):
        self.name = name
        self.gen_fn = gen_fn
        self.threshold = threshold
        self.max = max
        self.needs_table = needs_table 
        self.other_tables = other_tables
        self.gen_table = gen_table
        self.rem_table = rem_table
        self.need_prob = need_prob
        self.no_virt = no_virt
        self.mod_table = mod_table
        self.commit = commit
        self.prob = prob
        self.corpus = corpus

    def get_random(self, table: gen.Table, tables: list) -> "gen.SQLNode":
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

    def gen_valid_query(self, query: list, table: gen.Table, tables: list) -> tuple[int, list[str], gen.SQLNode]:
        for _ in range(self.max):
            mutate = False
            if self.corpus and random.random() < 0.3:
                node = self.mutate()
                mutate = True
            else:
                node = self.get_random(table, tables)
            if not node: continue
            if self.commit:
                new_query = random.choices(["BEGIN; " + node.sql() + ";" + random.choice([" COMMIT;", " ROLLBACK;"]), 
                                           "EXPLAIN " + node.sql() + ";", node.sql() + ";"], weights=[0.15, 0.15, 0.7], k=1)[0]
            else:
                new_query = random.choices(["EXPLAIN " + node.sql() + ";", node.sql() + ";"], weights=[0.2, 0.8], k=1)[0]
            lines_c, branch_c, taken_c, calls_c, msg = coverage_test(query + [new_query])
            cov_valid = coverage_score(lines_c, branch_c, taken_c, calls_c)
            if "Error" in msg and "constraint" not in msg:
                with open(TEST_FOLDER + f"error/error_valid_{cov_valid}_{random.randint(0, 10000)}.txt", "w") as f:
                    f.write(f"Query: {query + [new_query]}\n")
                    for err in get_error(msg):
                        f.write(f"Message: {err}\n")
            if cov_valid > 0:
                return cov_valid, [new_query], node, mutate
            
        return 0, [], None, False

    def generate(self, cov: float, c, init_query: list, tables: list, nodes: list, 
                 find_best: bool = False):
        pbar = tqdm(desc=f"{self.name:<15} (cov={cov:7.4f}) (query={len(init_query):03})")

        tries = 0
        best_cov = cov
        best_c = c
        new_query = init_query
        updated_tables = list(tables)
        self.corpus = nodes

        while tries < self.threshold:
            if tables:
                table = random.choice(updated_tables)
                while ((self.mod_table and (isinstance(table, gen.View) or (isinstance(table, gen.VirtualTable) and table.vtype == "dbstat"))) or 
                       (self.no_virt and isinstance(table, gen.VirtualTable))):
                    table = random.choice(updated_tables)
                cov_valid, valid_query, node, mut = self.gen_valid_query(init_query, table, updated_tables)
            else:
                cov_valid, valid_query, node, mut = self.gen_valid_query(init_query, None, updated_tables)
            if cov_valid == 0:
                continue
            reset()
            combined_query = (init_query if find_best else new_query) + valid_query + random.choices([["ANALYZE;"], []], weights=[0.05, 0.95], k=1)[0]
            lines_c, branch_c, taken_c, calls_c, msg = coverage_test(combined_query)
            combined_cov = coverage_score(lines_c, branch_c, taken_c, calls_c)

            if combined_cov > best_cov:
                best_cov = combined_cov
                best_c = (lines_c, branch_c, taken_c, calls_c)
                new_query = combined_query
                self.corpus.append(node)

                with open(TEST_FOLDER + f"query_test2.sql", "w") as f:
                    f.write("\n".join(new_query))

                if "Error" in msg and "constraint" not in msg:
                    with open(TEST_FOLDER + f"error/error_{cov}_{random.randint(0, 10000)}.txt", "w") as f:
                        f.write(f"Query: {new_query}\n")
                        for err in get_error(msg):
                            f.write(f"Message: {err}\n")
                
                if "ROLLBACK;" not in valid_query[0] and "EXPLAIN" not in valid_query[0] and not mut:
                    if self.rem_table: # alter table
                        updated_tables.remove(table)
                    if self.gen_table: # view, alter table
                        updated_tables.append(node)
                        init_query += valid_query

                tries = 0
                pbar.set_description(f"{self.name:<15} (cov={best_cov:7.4f}) (query={len(combined_query):03})")
            else:
                tries += 1

            pbar.update(1)

        pbar.close()
        return best_cov, best_c, new_query, updated_tables, self.corpus
    
    def mutate(self) -> gen.SQLNode:
        if not self.corpus:
            return None
        base_node = random.choice(self.corpus)
        if hasattr(base_node, "mutate"):
            try:
                return base_node.mutate()
            except Exception:
                return None
        return None

def run_pipeline(init_cov: int, init_query: list, init_tables: list, init_nodes: list, fuzz_pipeline: list, 
                 repeat: int = 1, save: bool = True):
    cov = init_cov
    query = init_query
    tables = init_tables
    nodes = init_nodes
    c = (0, 0, 0, 0)

    reset()
    for i in range(repeat):
        for stage in fuzz_pipeline:
            cov, c, query, tables, nodes = stage.generate(cov, c, query, tables, nodes)
        random.shuffle(fuzz_pipeline)
        
        if save:
            with open(TEST_FOLDER + f"save_{cov}.txt", "w") as f:
                f.write(f"Best Coverage: {cov}, {c}\n")
                f.write(f"Best Query: {query}\n")
                f.write(f"Tables: {tables}\n")
            with open(TEST_FOLDER + f"query_{cov}.sql", "w") as f:
                f.write("\n".join(query))
            with open(TEST_FOLDER + "error.txt", "w") as f:
                f.write("") # reset error.txt

    return cov, c, query, tables, nodes

if __name__ == "__main__":
    prob = {k: 0.5 for k in PROB_TABLE}
    cov, c, query, tables, nodes = run_pipeline(0, [], [], [], FUZZING_PIPELINE(prob), repeat=5)
    print(f"Final Coverage: {cov}")
