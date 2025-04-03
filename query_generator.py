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
        
    def join(self, tables):
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

    
    
