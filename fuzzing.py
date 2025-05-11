from tqdm import tqdm
from client_local import coverage_test, reset, LOCAL
from metric import coverage_score, save_error
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

def mutating_string(query: str) -> str:

    return query

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
    def __init__(self, name, gen_fn, threshold=10, max=100, needs_table=True, other_tables=False, 
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
        self.valid = 0
        self.invalid = 0

    def get_random(self, table: gen.Table, tables: list) -> "gen.SQLNode":
        '''
        selects the parameters depending if it has table, other_tables, param_prob  
        '''
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

    def gen_valid_query(self, query: list, table: gen.Table, tables: list, mut: bool, active: bool, db: str = "test.db") -> tuple[int, list[str], gen.SQLNode]:
        '''
        valid queries are queries that returns some coverage > 0
        '''
        for _ in range(self.max):

            if self.corpus and mut:
                node = self.mutate() # runs mutation in the second run
            else:
                node = self.get_random(table, tables)
            if not node: continue

            if self.commit or active:
                new_transact = random.choices([gen.TransactionControl.random(transaction_active=active, param_prob=self.prob).sql() + ";", ""], weights=[0.2, 0.8], k=1)[0]
                new_query = random.choices(["EXPLAIN " + node.sql() + ";", node.sql() + ";"], weights=[0.2, 0.8], k=1)[0]
                if active and new_transact:
                    new_query += " " + new_transact
                    active = False
                elif not active and new_transact:
                    new_query = new_transact + " " + new_query
                    active = True
            else:
                new_query = random.choices(["EXPLAIN " + node.sql() + ";", node.sql() + ";"], weights=[0.2, 0.8], k=1)[0]
            
            if LOCAL: db = "temp.db"
            lines_c, branch_c, taken_c, calls_c, msg = coverage_test(query + [new_query], db=db)
            cov_valid = coverage_score(lines_c, branch_c, taken_c, calls_c)

            save_error(msg, TEST_FOLDER + f"error/error_valid_{cov_valid}_{random.randint(0, 10000)}.txt")

            if cov_valid > 0:
                self.valid += 1
                return cov_valid, [new_query], node, active
            
            self.invalid += 1
            
        return 0, [], None, active

    def generate(self, cov: float, c, init_query: list, tables: list, nodes: list, 
                 find_best: bool = False, desc: str = "", mut: bool = False, active: bool = False):
        '''
        first test if the query is valid (with init_query) and then test it on the entire query (combined_query)
        '''
        pbar = tqdm(desc=f"{self.name:<15} (cov={cov:7.4f}) (query={len(init_query):03})")

        tries = 0
        best_cov = cov
        best_c = c # all coverages (lines, branches, taken, calls)
        best_msg = ""
        new_query = init_query
        updated_tables = list(tables) 
        self.corpus = nodes 
        active = active # transation active?

        while tries < self.threshold:

            #reset() # for local: resets the test.db and sqlite3.c.gcov

            if tables:
                table = random.choice(updated_tables)
                while ((self.mod_table and (isinstance(table, gen.View) or (isinstance(table, gen.VirtualTable) and table.vtype == "dbstat"))) or 
                       (self.no_virt and isinstance(table, gen.VirtualTable))):
                    table = random.choice(updated_tables)
                cov_valid, valid_query, node, val_active = self.gen_valid_query(init_query, table, updated_tables, mut, active)
                valid_query = valid_query + random.choices([[gen.Optimization.random(table).sql() + ";"], []], weights=[0.05, 0.95], k=1)[0]
            else:
                cov_valid, valid_query, node, val_active = self.gen_valid_query(init_query, None, updated_tables, mut, active)
            
            if cov_valid == 0:
                continue

            #reset() # for local: resets the test.db and sqlite3.c.gcov

            if LOCAL:
                test_query = valid_query
            else:
                test_query = (init_query if find_best else new_query) + valid_query
            lines_c, branch_c, taken_c, calls_c, msg = coverage_test(test_query)
            combined_cov = coverage_score(lines_c, branch_c, taken_c, calls_c)
            combined_query = (init_query if find_best else new_query) + valid_query

            if combined_cov > best_cov:
                best_cov = combined_cov
                best_c = (lines_c, branch_c, taken_c, calls_c)
                best_msg = msg
                new_query = combined_query
                self.corpus.append(node)
                active = val_active

                with open(TEST_FOLDER + f"query_test.sql", "w") as f:
                    f.write("\n".join(new_query))

                save_error(msg, TEST_FOLDER + f"error/error_{cov}_{random.randint(0, 10000)}.txt")
                
                if "EXPLAIN" not in valid_query[0] and not mut:
                    if self.rem_table: # alter table
                        updated_tables.remove(table)
                    if self.gen_table and node.columns: # view, table, alter table, virtual table
                        updated_tables.append(node)
                        init_query += valid_query

                tries = 0
                pbar.set_description(f"{self.name:<15} (cov={best_cov:7.4f}) (query={len(combined_query):03})")
            else:
                tries += 1

            pbar.update(1)

        pbar.close()
        return best_cov, best_c, new_query, updated_tables, self.corpus, best_msg, active
    
    def mutate(self) -> gen.SQLNode:
        if not self.corpus:
            return None
        
        base_node = random.choice(self.corpus)

        if hasattr(base_node, "mutate"):
            return base_node.mutate()

        return None

def run_pipeline(init_cov: int, init_query: list, init_tables: list, init_nodes: list, fuzz_pipeline: list[Fuzzing], 
                 repeat: int = 1, save: bool = True, mut_threshold: int = 15, desc: str = ""):
    cov = init_cov
    query = init_query
    tables = init_tables
    nodes = init_nodes
    c = (0, 0, 0, 0)
    total_valid = 0
    total_invalid = 0
    active = False

    reset()
    for i in range(repeat):
        for stage in fuzz_pipeline:
            cov, c, query, tables, nodes, msg, active = stage.generate(cov, c, query, tables, nodes, desc=desc, active=active)
            old_threshold = stage.threshold
            stage.threshold = mut_threshold
            cov, c, query, tables, nodes, msg, active = stage.generate(cov, c, query, tables, nodes, desc=desc, active=active, mut=True)
            stage.threshold = old_threshold
            total_valid += stage.valid
            total_invalid += stage.invalid
        random.shuffle(fuzz_pipeline)

        reset()
        lines_c, branch_c, taken_c, calls_c, msg = coverage_test(query)
        c = (lines_c, branch_c, taken_c, calls_c)
        cov = coverage_score(lines_c, branch_c, taken_c, calls_c)

        if save:
            with open(TEST_FOLDER + f"results/{desc}_save_{cov}.txt", "w") as f:
                f.write(f"Best Coverage: {cov}, {c}, Valid/Invalid{total_valid}/{total_invalid}\n")
                f.write(f"Best Query: {query}\n")
                f.write(f"Tables: {tables}\n")
            save_error(msg, TEST_FOLDER + f"results/{desc}_error_{cov}.txt")
            with open(TEST_FOLDER + f"results/{desc}_query_{cov}.sql", "w") as f:
                f.write("\n".join(query))

    return cov, c, query, tables, nodes

if __name__ == "__main__":
    #prob = {k: 0.5 for k in PROB_TABLE}
    pipeline = FUZZING_PIPELINE(PROB_TABLE)
    pipeline = [pipeline[0], pipeline[4], pipeline[5]]
    cov, c, query, tables, nodes = run_pipeline(0, [], [], [], pipeline, repeat=5, save=False)
    print(f"Final Coverage: {cov}")
