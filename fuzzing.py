from tqdm import tqdm
from client_local import coverage_test, reset, LOCAL
from metric import coverage_score, save_error
import generator as gen
import random, re, argparse
from config import TEST_FOLDER, SEED, PROB_TABLE, SQL_KEYWORDS, SQL_OPERATORS

#random.seed(SEED)

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
    Fuzzing("Delete", gen.Delete, mod_table=True, commit=True, prob=x), 
    Fuzzing("Replace", gen.Replace, mod_table=True, commit=True, prob=x),
    Fuzzing("DropTable", gen.DropTable, rem_table=True, needs_table=True, prob=x),
]

def mutate_query(query: str) -> str:
    mutations = [mutate_keyword, mutate_values, mutate_operator]
    mutation = random.choice(mutations)
    return mutation(query)

def mutate_keyword(query: str) -> str:
    if any(w in query for w in SQL_KEYWORDS):
        words = [w for w in SQL_KEYWORDS if w in query]
        if words:
            old_w = random.choice(words)
            new_w = random.choice(SQL_KEYWORDS)
            return re.sub(old_w, new_w, query)
    return query

def mutate_values(query: str) -> str:
    values = re.findall(r'\b(?:[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*|[a-z_][a-z0-9_]*|\d+)\b', query)
    if values:
        old_val = random.choice(values)
        new_val = random.choice(values)
        return re.sub(old_val, new_val, query)
    return query

def mutate_operator(sql: str) -> str:
    for op in SQL_OPERATORS:
        if op in sql:
            new_op = random.choice(SQL_OPERATORS)
            return sql.replace(op, new_op, 1)
    return sql

class Fuzzing:
    """
    Fuzzing pipeline 
    """
    def __init__(self, name: str, gen_fn: gen.SQLNode, threshold=10, max=100, needs_table=True, other_tables=False, 
                 gen_table=False, rem_table=False, need_prob=True, no_virt=False, mod_table=False, 
                 commit=False, prob=None, corpus: list[gen.SQLNode] = []):
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

    def get_random(self, table: gen.Table, tables: list[gen.Table]) -> gen.SQLNode:
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

    def gen_valid_query(self, table: gen.Table, tables: list, mut: bool, active: bool) -> tuple[list[str], gen.SQLNode, bool]:
        '''
        valid queries are queries that are not None
        '''
        for _ in range(self.max):
            if self.corpus and mut:
                node = self.mutate() # runs mutation in the second run
            else:
                node = self.get_random(table, tables)

            if not node: 
                self.invalid += 1
                continue

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
            
            if self.corpus and mut:
                new_query = mutate_query(new_query) # mutate on string

            self.valid += 1
            return [new_query], node, active

        return [], None, active

    def generate(self, cov: float, c: tuple[float], init_query: list[str], tables: list[gen.Table], corpus: list[gen.SQLNode], threshold_overwrite: int,
                 desc: str = "", mut: bool = False, active: bool = False) -> tuple[float, tuple[float], list[str], list[gen.Table], list[gen.SQLNode], str, bool]:
        '''
        mut == False: creates/takes random table and then call random function in generator.py with table 
        mut == True : randomly select a SQLNode from the corpus and mutate it 
            structural (on SQLNode): self.mutate()
            syntax     (on String) : mutate_query(new_query)
        '''
        self.threshold = threshold_overwrite
        name = "Mutate" if mut else self.name
        pbar = tqdm(desc=f"{(name):<12} (lines_cov={c[0]:5.4f}) (branch_cov={c[1]:5.4f}) (query={len(init_query):03}) {desc:<15}")

        tries = 0
        best_cov = cov
        best_c = c # all coverages (lines, branches, taken, calls)
        best_msg = ""
        new_query = init_query
        updated_tables = tables
        self.corpus = corpus # all the SQLNode in the query
        active = active # transation active

        while tries < self.threshold:

            if tables:
                table = random.choice(updated_tables)
                while ((self.mod_table and (isinstance(table, gen.View) or (isinstance(table, gen.VirtualTable) and table.vtype == "dbstat"))) or 
                       (self.no_virt and isinstance(table, gen.VirtualTable))):
                    table = random.choice(updated_tables)
                valid_query, node, val_active = self.gen_valid_query(table, updated_tables, mut, active)
                valid_query = valid_query + random.choices([[gen.Optimization.random(table).sql() + ";"], []], weights=[0.05, 0.95], k=1)[0]
            else:
                valid_query, node, val_active = self.gen_valid_query(None, updated_tables, mut, active)
            
            if not node:
                continue

            if LOCAL:
                test_query = valid_query
            else:
                test_query = new_query + valid_query
            lines_c, branch_c, taken_c, calls_c, msg = coverage_test(test_query)
            combined_cov = coverage_score(lines_c, branch_c, taken_c, calls_c)
            combined_query = new_query + valid_query

            if combined_cov > best_cov:
                best_cov = combined_cov
                best_c = (lines_c, branch_c, taken_c, calls_c)
                best_msg = msg
                new_query = combined_query
                self.corpus.append(node)
                active = val_active

                with open(TEST_FOLDER + f"query_test.sql", "w") as f:
                    f.write("\n".join(new_query))

                # save_error(msg, TEST_FOLDER + f"error/error_{cov}_{random.randint(0, 10000)}.txt")
                
                if "EXPLAIN" not in valid_query[0] and not mut:
                    if self.rem_table: # alter table
                        updated_tables.remove(table)
                    if self.gen_table and node.columns: # view, table, alter table, virtual table
                        updated_tables.append(node)
                        init_query += valid_query

                tries = 0
                pbar.set_description(f"{(name):<12} (lines_cov={lines_c:7.4f}) (branch_cov={branch_c:7.4f}) (query={len(combined_query):03}) {desc:<15}")
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
                 repeat: int = 1, save: bool = True, threshold: int = 10, desc: str = ""):
    cov = init_cov
    c = (0, 0, 0, 0) # all coverages (lines, branches, taken, calls)
    query = init_query
    tables = init_tables
    corpus = init_nodes
    active = False # transation active

    total_valid = 0
    total_invalid = 0

    reset() # for local: resets the test.db and sqlite3.c.gcov
    for i in range(repeat):
        print(f"Loop {i}")
        for stage in fuzz_pipeline:
            stage.threshold = threshold
            cov, c, query, tables, corpus, msg, active = stage.generate(cov, c, query, tables, corpus, threshold, desc=desc, active=active)
            
            # mutation
            cov, c, query, tables, corpus, msg, active = stage.generate(cov, c, query, tables, corpus, (i+2)*threshold, desc=desc, active=active, mut=True)
            stage.threshold = threshold

            total_valid += stage.valid
            total_invalid += stage.invalid

        random.shuffle(fuzz_pipeline)

        pragma = set()
        while len(pragma) < 10: # creates 10 unique pragma settings
            pragma.add(gen.Pragma.random().sql() + ";")
        query.extend(pragma)

        reset()
        lines_c, branch_c, taken_c, calls_c, msg = coverage_test(query, timeout=None)
        c = (lines_c, branch_c, taken_c, calls_c)
        cov = coverage_score(lines_c, branch_c, taken_c, calls_c)

    if save:
        with open(TEST_FOLDER + f"results/pipeline_{lines_c:5.4f}_save.txt", "w") as f:
            f.write(f"Best Coverage: {cov:5.4f}, {c}, Valid/Invalid: {total_valid}/{total_invalid}\n")
        save_error(msg, TEST_FOLDER + f"results/pipeline_{lines_c:5.4f}_error.txt")
        with open(TEST_FOLDER + f"pipeline_{lines_c:5.4f}_query.sql", "w") as f:
            f.write("\n".join(query))

    return cov, c, query, tables, corpus

def random_query(repeat: int = 3, save: bool = True):
    query = []
    tables = []

    reset() # for local: resets the test.db and sqlite3.c.gcov
    query, tables = gen.randomQueryGen(cycle=repeat)

    print(len(query))

    lines_c, branch_c, taken_c, calls_c, msg = coverage_test(query, timeout=len(query)/10.0)
    c = (lines_c, branch_c, taken_c, calls_c)
    cov = coverage_score(lines_c, branch_c, taken_c, calls_c)

    if save and cov != 0:
        with open(TEST_FOLDER + f"results/random_{lines_c:5.4f}_save.txt", "w") as f:
            f.write(f"Best Coverage: {cov:5.4f}, {c}\n")
        save_error(msg, TEST_FOLDER + f"results/random_{lines_c:5.4f}_error.txt")
        with open(TEST_FOLDER + f"random_{lines_c:5.4f}_query.sql", "w") as f:
            f.write("\n".join(query))

    return cov, c, query, tables

def main():
    parser = argparse.ArgumentParser(description="Fuzzing Script")
    parser.add_argument("type", help="Select hybrid type: 'PIPELINE', 'RANDOM'")
    parser.add_argument("repeat", help="Number of Loops")
    
    args = parser.parse_args()

    c = (0, 0, 0, 0)
    if str(args.type) == 'PIPELINE': 
        pipeline = FUZZING_PIPELINE(PROB_TABLE)
        cov, c, query, tables, corpus = run_pipeline(0, [], [], [], pipeline, repeat=int(args.repeat))
    elif str(args.type) == 'RANDOM': 
        cov, c, query, table = random_query(repeat=int(args.repeat))

    print(f"Final Coverage: {c[0]}")

if __name__ == "__main__":
    main()