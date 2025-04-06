import random
import sys
import string

random.seed(42)

SQL_TYPES = ["INTEGER", "TEXT", "REAL"] #, "BLOB", "NULL"]
SQL_CONSTRAINTS = ["PRIMARY KEY", "FOREIGN KEY", "NOT NULL", "UNIQUE", "CHECK(%s > 0)", "DEFAULT NULL", "DEFAULT default"]

MAX_INT = 9223372036854775807
MIN_INT = -9223372036854775808

def random_string(length=10):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

class SQLiteTable:
    """
    Randomly generate table queries to create table and insert rows.
    """
    def __init__(self, name):
        self.name = name
        self.cols = {}
        self.script = []
    
    def create(self, rows=1, max_cols=1):
        self.cols = {}
        self.script = []
        columns_def = []
        
        for t in SQL_TYPES:
            num = max_cols
            col_names = []
            col_types = []
            
            for i in range(num):
                col_names.append(random_string(6))
                col_types.append(t)
                    
                column_def = f"{col_names[i]} {col_types[i]}" #{' '.join(random_constraints(col_names[i], col_types[i]))}"
                columns_def.append(column_def)
                
                self.cols[col_names[i]] = col_types[i]
                
        create_stmt = f"CREATE TABLE IF NOT EXISTS {self.name} ({', '.join(columns_def)});"
        self.script.append(create_stmt)

        for i in range(rows):
            self.insert()

    def insert(self):
        cols = ", ".join(self.cols.keys())
        values = ", ".join(self.generate_random_values())
        insert = f"INSERT INTO {self.name} ({cols}) VALUES ({values});"
        self.script.append(insert)
        return insert
        
    def get_script(self):
        return self.script
        
    def get_cols(self):
        return self.cols
    
    def generate_random_values(self):
        values = []
        for col, dtype in self.cols.items():
            dtype = dtype.upper()
            if "INTEGER" in dtype:
                values.append(random.choice(["0", "1", "-1", str(MAX_INT), str(MIN_INT), "NULL", str(random.randint(MIN_INT, MAX_INT))]))
            elif "TEXT" in dtype:
                values.append(random.choice([f"'{random_string(random.randint(2, 100))}'", f"'{random_string(1)}'", "''", "NULL"]))
            elif "REAL" in dtype:
                values.append(random.choice([str(random.uniform(-1000, 1000)), "0", "NULL"]))
            elif "BLOB" in dtype:
                values.append("NULL")
            else:
                values.append("NULL")
        return values
    
    def random_constraints(self, col_name, col_type):
        constraints = []
    
        if random.random() < 0:
            constraint = random.choice(SQL_CONSTRAINTS)
            if "CHECK" in constraint:
                constraint = constraint % col_name
            elif "DEFAULT" in constraint:
                default_value = "0" if col_type == "INTEGER" else "'default'"
                constraint = constraint % default_value
            constraints.append(constraint)
         
        return constraints

class SQLiteQuery:
    """
    Randomly generate queries given table names and table columns.
    """
    def __init__(self, tables, tables_cols):
        self.tables = tables
        self.tables_cols = tables_cols
        
    def select(self, table_name, size=1):
        col_names = list(self.tables_cols[table_name].keys())
        subset_size = random.randint(1, len(col_names))
        col_subset = random.sample(col_names, subset_size)
        
        where_size = random.randint(1, size)
        where_set = [self.generate_random_where(table_name) for i in range(where_size)]
        
        select = f"SELECT {', '.join(col_subset)} FROM {table_name} WHERE {' OR '.join(where_set)};"
        return select
        
    def join(self):
        #TODO add where clause
        
        if len(self.tables) < 2:
            return None  # Not enough tables

        table1, table2 = random.sample(self.tables, 2)
        
        join_type = random.choice(["INNER JOIN", "LEFT JOIN", "CROSS JOIN"]) # RIGHT and FULL not supported?

        col1_names = list(self.tables_cols[table1].keys()) #maybe helper function that creates subsets of cols of a table
        size1 = random.randint(1, len(col1_names))
        col1_subset = random.sample(col1_names, size1)
        
        col2_names = list(self.tables_cols[table2].keys())
        size2 = random.randint(1, len(col2_names))
        col2_subset = random.sample(col2_names, size2)
        
        col_subset = col1_subset + col2_subset
        col1 = random.choice(col1_names)
        col2 = random.choice(col2_names) 
        

        query = (
            f"SELECT {', '.join(col_subset)} "
            f"FROM {table1} {join_type} {table2} "
            f"ON {table1}.{col1} = {table2}.{col2};"
        )

        return query
        
    
    def index(self):
        # TODO add where clause in index
        # TODO use literals instead of col names
        
        table = random.choice(self.tables)
        col_names = list(self.tables_cols[table].keys())
        
        idx_name = "idx_"+random_string(6)
        
        num_cols = random.randint(1, min(2, len(col_names)))
        selected_cols = random.sample(col_names, num_cols)

        index_parts = []
        for col in selected_cols:
            if random.random() < 0.3:
                func = random.choice(["abs", "lower", "upper", "length"])
                index_parts.append(f"{func}({col})")
            else:
                index_parts.append(col)
        
        index_expr = ", ".join(index_parts)
        
        return f"CREATE INDEX {idx_name} ON {table}({index_expr});"
    
    def view(self):
        # TODO
        return 0
    
    def generate_random_where(self, table_name):
        col = random.choice(list(self.tables_cols[table_name].keys()))
        dtype = self.tables_cols[table_name][col]
        operators = ["IS", "IS NOT"]
        
        if "INT" in dtype or "REAL" in dtype or "FLOAT" in dtype:
            operators += ["=", "!=", "<", ">", "<=", ">="]
            op = random.choice(operators)
            val = random.choice([0, 1, -1, MAX_INT, MIN_INT, random.randint(-1000, 1000), "NULL"])
            return f"{col} {op} {val}"
        elif "TEXT" in dtype:
            op = random.choice(operators)
            val = f"'test'"
        elif "BLOB" in dtype:
            op = random.choice(operators)
            val = "NULL"
        else:
            op = random.choice(operators)
            val = "NULL"
            
        return f"{table_name}.{col} {op} {val}" 

    
    
