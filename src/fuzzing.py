import random, re, time, argparse
from .config import QUERY_FOLDER, ERROR_FOLDER, STATS_FOLDER, SEED, PROB_TABLE, SQL_KEYWORDS, SQL_OPERATORS
from . import generator as gen
from .test import run_coverage, reset, LOCAL
from .helper.helper import coverage_score, save_error
from .helper.metric import extract_metric
from tqdm import tqdm


#random.seed(SEED)

FUZZING_PIPELINE = lambda x: [
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
    Fuzzing("DropTable", gen.DropTable, rem_table=True, prob=x),
]

def mutate_query(query: str) -> str:
    mutations = [mutate_keyword, mutate_values, mutate_operator]
    mutation = random.choice(mutations)
    return mutation(query)

def mutate_keyword(query: str) -> str:
    mutation_type = random.choice(["replace", "remove", "add"])

    if mutation_type == "replace":
        words = [w for w in SQL_KEYWORDS if w in query]
        if words:
            old_w = random.choice(words)
            new_w = random.choice(SQL_KEYWORDS)
            return re.sub(rf"\b{re.escape(old_w)}\b", new_w, query)

    elif mutation_type == "remove":
        words = [w for w in SQL_KEYWORDS if w in query]
        if words:
            remove = random.choice(words)
            return re.sub(rf"\b{re.escape(remove)}\b", '', query).strip()

    elif mutation_type == "add":
        word = random.choice(SQL_KEYWORDS)
        pos = random.randint(0, len(query))
        return query[:pos] + " " + word + " " + query[pos:]

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
            mut_q = False
            if self.corpus and mut and random.random() < 0.5:
                node = self.mutate() # runs mutation in the second run
            else:
                try:
                    node = self.get_random(table, tables)
                except:
                    node = self.mutate()
                mut_q = True

            if not node: 
                self.invalid += 1
                continue

            if self.commit or active:
                new_transact = random.choices([gen.TransactionControl.random(transaction_active=active, param_prob=self.prob).sql() + ";", ""], weights=[0.2, 0.8], k=1)[0]
                new_query = random.choices(["EXPLAIN " + node.sql() + ";", node.sql() + ";"], weights=[0.1, 0.9], k=1)[0]
                if active and new_transact:
                    new_query += " " + new_transact
                    active = False
                elif not active and new_transact:
                    new_query = new_transact + " " + new_query
                    active = True
            else:
                new_query = random.choices(["EXPLAIN " + node.sql() + ";", node.sql() + ";"], weights=[0.1, 0.9], k=1)[0]
            
            if self.corpus and mut and mut_q:
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
        start_time = time.time()
        self.threshold = threshold_overwrite
        name = "Mutate" if mut else self.name
        pbar = tqdm(desc=f"{(name):<12} (lines_cov={c[0]:5.4f}) (branch_cov={c[1]:5.4f}) (query={len(init_query):03})")

        tries = 0
        best_cov = cov
        best_c = c # all coverages (lines, branches, taken, calls)
        best_msg = ""
        new_query = init_query
        updated_tables = tables
        self.corpus = corpus # all the SQLNode in the query
        active = active # transation active

        if (self.needs_table or self.rem_table) and not updated_tables:
            runtime = end_time - start_time
            return best_cov, best_c, new_query, updated_tables, self.corpus, best_msg, active, runtime

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

            lines_c, branch_c, taken_c, calls_c, msg = run_coverage(test_query)
            combined_cov = coverage_score(lines_c, branch_c, taken_c, calls_c)
            combined_query = new_query + valid_query

            if combined_cov > best_cov:
                best_cov = combined_cov
                best_c = (lines_c, branch_c, taken_c, calls_c)
                best_msg = msg
                new_query = combined_query
                self.corpus.append(node)
                active = val_active

                if "EXPLAIN" not in valid_query[0] and not mut:
                    if self.rem_table: # alter table
                        updated_tables.remove(table)
                    if self.gen_table and node.columns: # view, table, alter table, virtual table
                        updated_tables.append(node)
                        init_query += valid_query

                tries = 0
                pbar.set_description(f"{(name):<12} (lines_cov={lines_c:7.4f}) (branch_cov={branch_c:7.4f}) (query={len(combined_query):03})")
            else:
                tries += 1

            pbar.update(1)

        pbar.close()
        end_time = time.time()

        runtime = end_time - start_time
        return best_cov, best_c, new_query, updated_tables, self.corpus, best_msg, active, runtime
    
    def mutate(self) -> gen.SQLNode:
        if not self.corpus:
            return None
        
        base_node = random.choice(self.corpus)

        if hasattr(base_node, "mutate"):
            return base_node.mutate()

        return None

def run_pipeline(init_cov: int, init_query: list, init_tables: list, init_nodes: list, fuzz_pipeline: list[Fuzzing], 
                 repeat: int = 1, save: bool = True, threshold: int = 10, desc: str = ""):
    '''
    Hybrid Pipeline Fuzzer with query generator and mutator
    '''
    total_runtime = 0
    
    cov = init_cov
    c = (0, 0, 0, 0) # all coverages (lines, branches, taken, calls)
    query = init_query
    tables = init_tables
    corpus = init_nodes
    active = False # transation active

    total_valid = 0
    total_invalid = 0

    init_pipeline = [Fuzzing("Table", gen.Table, gen_table=True, needs_table=False, need_prob=False)] 
    test_pipeline = init_pipeline + random.choices(fuzz_pipeline, k = random.randint(5, len(fuzz_pipeline)))

    reset() # for local: resets the test.db and coverage information
    for i in range(repeat):
        print(f"Loop {i}")
        for stage in test_pipeline:
            stage.threshold = threshold
            cov, c, query, tables, corpus, msg, active, runtime = stage.generate(cov, c, query, tables, corpus, threshold, desc=desc, active=active)
            total_runtime += runtime

            # mutation
            cov, c, query, tables, corpus, msg, active, runtime = stage.generate(cov, c, query, tables, corpus, (i+2)*threshold, desc=desc, active=active, mut=True)
            stage.threshold = threshold
            total_runtime += runtime

            total_valid += stage.valid
            total_invalid += stage.invalid
            
        test_pipeline = init_pipeline + random.choices(fuzz_pipeline, k = random.randint(5, len(fuzz_pipeline)))
        random.shuffle(test_pipeline)

        pragma = set()
        while len(pragma) < 10: # creates 10 unique pragma settings
            pragma.add(gen.Pragma.random().sql() + ";")
        query.extend(pragma)

        reset()

        queries = []
        for i in range(0, len(query), 250):
             queries.append(query[i:i+250])

        for q in queries:
            lines_c, branch_c, taken_c, calls_c, msg = run_coverage(q, timeout=30)
            c = (lines_c, branch_c, taken_c, calls_c)
            cov = coverage_score(lines_c, branch_c, taken_c, calls_c)

    if save and cov > 0:
        err = save_error(msg, f"{ERROR_FOLDER}pipeline_{lines_c:5.2f}.txt")
        with open(f"{STATS_FOLDER}pipeline_{lines_c:5.2f}.txt", "w") as f:
            f.write(f"Average Coverage: {cov:5.2f}\n") 
            f.write(f"Lines Coverage: {c[0]}\n")
            f.write(f"Branch Coverage: {c[1]}\n") 
            f.write(f"Taken Coverage: {c[2]}\n") 
            f.write(f"Calls Coverage: {c[3]}\n") 
            f.write(f"Valid/Invalid: {total_valid}/{total_invalid}\n")
            f.write(f"Errors: {err}\n")
            f.write(f"Runtime: {total_runtime}\n")
            counter = extract_metric(query)
            for k, v in counter.items():
                f.write(f"  {k}: {v}\n")
        with open(f"{QUERY_FOLDER}pipeline_{lines_c:5.2f}.sql", "w") as f:
            f.write("\n".join(query))

    return cov, c, query, tables, corpus

def random_query(repeat: int = 3, save: bool = True, param_prob: dict[str, float] = None, cov_test: bool = True):
    '''
    Fast query generator
    '''
    start = time.time()
    query = []
    tables = []
    cov = 0
    c = (0, 0, 0, 0)
    msg = ""

    reset() # for local: resets the test.db and coverage information
    query, tables = gen.randomQueryGen(param_prob=param_prob, cycle=repeat)

    if cov_test:
        queries = []
        for i in range(0, len(query), 250):
            queries.append(query[i:i+250])

        for q in queries:
            lines_c, branch_c, taken_c, calls_c, msg = run_coverage(q, timeout=len(q)/10.0)
            c = (lines_c, branch_c, taken_c, calls_c)
            cov = coverage_score(lines_c, branch_c, taken_c, calls_c)

        # print(f"Average Coverage: {cov:5.2f}, Lines Coverage: {c[0]}, Branch Coverage: {c[1]} ")
    stop = time.time()
    if save:
        if cov_test:
            filepath = f"random_{lines_c:5.2f}"
            err = save_error(msg, f"{ERROR_FOLDER}{filepath}.txt")
        else:
            filepath = f"random_{random.randint(1, 1000000)}"
        with open(f"{STATS_FOLDER}{filepath}.txt", "w") as f:
            if cov_test:
                f.write(f"Average Coverage: {cov:5.2f}\n") 
                f.write(f"Lines Coverage: {c[0]}\n")
                f.write(f"Branch Coverage: {c[1]}\n") 
                f.write(f"Taken Coverage: {c[2]}\n") 
                f.write(f"Calls Coverage: {c[3]}\n") 
                f.write(f"Errors: {err}\n")
                f.write(f"Runtime: {stop-start}\n")
            f.write(f"Metrics:\n")
            counter = extract_metric(query)
            for k, v in counter.items():
                f.write(f"  {k}: {v}\n")
        with open(f"{QUERY_FOLDER}{filepath}.sql", "w") as f:
            f.write("\n".join(query))

    return cov, c, query, tables

def main(args=None, remain_args=None):
    parser = argparse.ArgumentParser(description="Fuzzing")
    parser.add_argument("repeat", help="Number of fuzzing loops", nargs="?", default=1, type=int)
    parser.add_argument("sql", help="Number of .sql files", nargs="?", default=1, type=int)
    
    other_args = parser.parse_args(remain_args)

    times = other_args.sql

    c = (0, 0, 0, 0)
    for _ in range(times):
        if args.type == 'PIPELINE': 
            prob = {k: (0.05 if 0 <= v and v <= 0.01 else v) for k, v in PROB_TABLE.items()}
            prob = {k: ( 0.5 if v == 1 else v) for k, v in prob.items()}
            prob = {k: ( 0.9 if v >= 0.95 else v) for k, v in prob.items()}
            pipeline = FUZZING_PIPELINE(prob)
            cov, c, query, tables, corpus = run_pipeline(0, [], [], [], pipeline, repeat=other_args.repeat)
        elif args.type == 'RANDOM': 
            cov, c, query, table = random_query(repeat=other_args.repeat, param_prob=None, cov_test=True)

if __name__ == "__main__":
    main()