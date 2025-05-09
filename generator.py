import random
import string
from dataclasses import dataclass, field
from typing import List, Optional, Union, Dict
from config import SEED
import copy

random.seed(SEED)

VIRTUAL = {
    "types": ["rtree", "fts4", "dbstat"],
    "dbstat": [{"name": "TEXT"}, {"path": "TEXT"}, {"pageno": "INTEGER"}, {"pagetype": "TEXT"}, {"ncell": "INTEGER"}, {"payload": "INTEGER"}, {"unused": "INTEGER"}, {"mx_payload": "INTEGER"}, {"pgoffset": "INTEGER"}, {"pgsize": "INTEGER"}, {"schema": "TEXT"}],
    "rtree": [{"id": "INTEGER", "constraint": "PRIMARY KEY"}, {"minX": "REAL"}, {"maxX": "REAL"}, {"minY": "REAL"}, {"maxY": "REAL"}],
    "fts4": [{"title": "TEXT"}, {"body": "TEXT"}]
}
SQL_TYPES = ["INTEGER", "TEXT", "REAL"]
SQL_CONSTRAINTS = ["PRIMARY KEY", "UNIQUE", "NOT NULL", "CHECK", "DEFAULT"]
VALUES = { # put interesting values to test here
    "INTEGER": [0, 1, -1, 
                2**31-1, -2**31, 
                2**63-1, -2**63,
                999999999999999999999999999999999999999999999999999999999999999999999999,
                42, 1337,
                0x7FFFFFFF, 0x80000000,
                ],
    "TEXT": ["''", "' '", "'a'", "'abc'", "'" + " "*50 + "'", 
             #"' OR 1=1; --'", "'\x00\x01\x02'", 
             #"DROP TABLE test;", #"A"*10000,
             "'quoted'", "'NULL'",
             ],
    "REAL": [0.0, -0.0, 1.0, -1.0,
             3.14159, 2.71828,
             #float('inf'), float('-inf'), float('nan'),
             1e-10, 1e10, 1e308, -1e308,
             ],
}
TIME = {
    "TIMES": ["'now'",
            "'2025-01-01 12:00:00'",
            "'2020-06-15 08:45:00'",
            "'2000-01-01 00:00:00'",
            "'2030-12-31 23:59:59'",
            "'1700000000'",
    ],
    "TIME_MODS": [
            "'+1 day'", "'-2 days'", "'+3 hours'", "'-90 minutes'",
            "'start of month'", "'start of year'", "'weekday 0'",
            "'+1 month'", "'-1 year'",
            "'utc'", "'localtime'",
    ],
    "TIME_FORMATS": [
        "'%Y-%m-%d'", "'%H:%M:%S'", "'%s'", "'%w'", "'%Y %W'", "'%j'", "'%Y-%m-%d %H:%M'"
    ],
    "DATES": ['datetime', 'date', 'time', 'julianday', 'strftime']
}
CALLABLE_VALUES = {
    "INTEGER": lambda: random.randint(-10000, 10000),
    "TEXT": lambda: ("'" + random_name(prefix = "v", length=5) + "'"),
    "REAL": lambda: random.uniform(-1e5, 1e5),
}
OPS = {
    "INTEGER": ["=", "!=", ">", "<", ">=", "<="],
    "TEXT": ["=", "!="],
    "REAL": ["=", "!=", ">", "<", ">=", "<="],
    "TYPELESS": ["=", "!="],
}

def random_name(prefix: str = "x", length: int = 5) -> str:
    """
    Generate a random string, used for naming

    Args:
        prefix (str, optional): Prefix for naming purposes. Defaults to "x".
        length (int, optional): Length of string. Defaults to 5.

    Returns:
        str: the random string
    """
    suffix = ''.join(random.choices(string.ascii_lowercase, k=length)) #add string.digits?
    return f"{prefix}_{suffix}"

def random_type() -> str:
    """
    Returns a random data type

    Returns:
        str: the dtype
    """
    return random.choice(SQL_TYPES)

def random_value(dtype:str, null_chance:float = 0.05, callable_chance:float = 0.9) -> str:
    """
    Returns a ramdom value given the type, could be either interesting values or completely random.

    Args:
        dtype (str): The data type of the value

    Returns:
        str: the generated value
    """
    if flip(null_chance):
        return "NULL"
    
    dtype = random_type() if dtype == "TYPELESS" else dtype
    return str(CALLABLE_VALUES[dtype]()) if flip(callable_chance) else str(random.choice(VALUES[dtype]))
    
def flip(weight:float = 0.5) -> bool:  
    """
    Flipping weighted coin

    Args:
        weight (float, optional): Likelihood. Defaults to 0.5.

    Returns:
        bool: success
    """
    return random.random() < weight

class SQLNode:
    def sql(self) -> str:
        return ""
        #raise NotImplementedError

@dataclass
class Predicate(SQLNode):
    def sql(self) -> str:
        raise NotImplementedError
    
    @staticmethod
    def random(table: "Table", sub_allow: bool = True) -> "Predicate":
        col = random.choice(table.columns)
        
        predicate_classes = [NullCheck, Comparison, InList]
        if sub_allow:
            predicate_classes += [Exists]
        
        if col.dtype == "INTEGER" or col.dtype == "REAL":
            predicate_classes.append(Between)
        if col.dtype == "TEXT":
            predicate_classes.append(Like)
        
        cls = random.choice(predicate_classes)
        if cls == Exists:
            return cls.random(table)
        else:
            return cls.random(col, table_name=table.name)
    
@dataclass
class Comparison(Predicate):
    '''
    column OP value
    '''
    column: str
    operator: str
    value: str
    table_name: str
    
    def sql(self) -> str:
        ret = f"{self.table_name}." if self.table_name else ""
        return ret + f"{self.column} {self.operator} {self.value}"

    @staticmethod
    def random(col: "Column", param_prob: Dict[str, float] = None, table_name: str = "") -> "Comparison":
        prob = {"comp_nullc": 0.05, "comp_callc": 0.9}
        if param_prob is not None:
            prob.update(param_prob)
        dtype = col.dtype
        op = random.choice(OPS[dtype])
        val = random_value(dtype, prob["comp_nullc"], prob["comp_callc"])
        return Comparison(col.name, op, val, table_name)

@dataclass
class Between(Predicate):
    '''
    column BETWEEN low AND high
    '''
    column: str
    lower: str
    upper: str
    table_name: str

    def sql(self) -> str:
        ret = f"{self.table_name}." if self.table_name else ""
        return ret + f"{self.column} BETWEEN {self.lower} AND {self.upper}"

    @staticmethod
    def random(col: "Column", param_prob: Dict[str, float] = None, table_name: str = "") -> "Between":
        prob = {"bet_nullc" : 0, "bet_callc" : 0.9}
        if param_prob is not None:
            prob.update(param_prob)
        v1 = random_value(col.dtype, prob["bet_nullc"], prob["bet_callc"])
        v2 = random_value(col.dtype, prob["bet_nullc"], prob["bet_callc"])
        low, high = sorted([v1, v2], key=lambda x: float(x))
        return Between(col.name, low, high, table_name)

@dataclass
class Like(Predicate):
    '''
    column LIKE val
    '''
    column: str
    val: str
    table_name: str

    def sql(self) -> str:
        ret = f"{self.table_name}." if self.table_name else ""
        return ret + f"{self.column} LIKE {self.val}"

    @staticmethod
    def random(col: "Column", param_prob: Dict[str, float] = None, table_name: str = "") -> "Like":
        prob = {"like_nullc" : 0.05, "like_callc" : 0.9}
        if param_prob is not None:
            prob.update(param_prob)
        val = Like.generate_like_pattern(random_value("TEXT", prob["like_nullc"], prob["like_callc"]))
        return Like(col.name, val, table_name)
    
    @staticmethod
    def generate_like_pattern(base: str) -> str:
        '''
        Helper function to create patterns for LIKE, could be more complex if we wanted
        '''
        if not base:
            return random.choice(['%', '_', ''])
        
        base = base.replace("'", "")

        patterns = []
        patterns.append(f"{base}")
        patterns.append(f"{base}")
        
        patterns.append(f"%{base}")
        patterns.append(f"{base}%")
        patterns.append(f"%{base}%")
        patterns.append(f"_{base}")
        patterns.append(f"{base}_")
        
        if len(base) > 2:
            idx = random.randint(1, len(base) - 2)
            pattern = base[:idx] + '%' + base[idx:]
            patterns.append(pattern)

        if len(base) > 1:
            idx = random.randint(0, len(base) - 1)
            pattern = base[:idx] + '_' + base[idx + 1:]
            patterns.append(pattern)

        return "'" + random.choice(patterns) + "'"
    
@dataclass
class InList(Predicate):
    '''
    column in (v1, v2, v3, ...)
    '''
    column: str
    values: List[str]
    table_name: str = ""

    def sql(self) -> str:
        value_list = ', '.join(self.values)
        ret = f"{self.table_name}." if self.table_name else ""
        return ret + f"{self.column} IN ({value_list})"

    @staticmethod
    def random(col: "Column", param_prob: Dict[str, float] = None, table_name: str = "") -> "InList":
        prob = {"inli_nullc" : 0.05, "inli_callc" : 0.9}
        if param_prob is not None:
            prob.update(param_prob)
        count = random.randint(2, 5)
        values = [random_value(col.dtype, prob["inli_nullc"], prob["inli_callc"]) for _ in range(count)]
        return InList(col.name, values, table_name)
    
@dataclass
class Exists(Predicate):
    '''
    EXISTS (SELECT 1 FROM ... WHERE ...)
    apparently select 1 acts the same as any other column so we probably dont need to add the functionality
    '''
    select: "Select"

    def sql(self) -> str:
        return f"EXISTS ({self.select.sql()})"

    @staticmethod
    def random(table: "Table", param_prob: Dict[str, float] = None) -> "Exists":
        prob = {"where_p" : 1, "grp_p" : 0, "ord_p" : 0}
        if param_prob is not None:
            prob.update(param_prob)
        select = Select.random(table, sample=1, param_prob=prob)
        return Exists(select)

@dataclass
class NullCheck(Predicate):
    '''
    column IS/IS NOT NULL
    '''
    column: str
    check: bool
    table_name: str

    def sql(self) -> str:
        ret = f"{self.table_name}." if self.table_name else ""
        if self.check:
            return ret + f"{self.column} IS NULL"
        else:
            return ret + f"{self.column} IS NOT NULL"

    @staticmethod
    def random(col: "Column", param_prob: Dict[str, float] = None, table_name: str = "") -> "NullCheck":
        prob = {"nullc" : 0.5}
        if param_prob is not None:
            prob.update(param_prob)
        check = flip(prob["nullc"])
        return NullCheck(col.name, check, table_name)
    
@dataclass
class Where(SQLNode):
    '''
    WHERE Predicate/InSubquery/WHERE
    '''
    def sql(self) -> str:
        raise NotImplementedError 

    @staticmethod
    def random(table: "Table", max_depth: int = 1, other_tables: List["Table"] = None, param_prob: Dict[str, float] = None) -> "Where":
        prob = {"pred_p":1.0, "depth_p":0.7, "sub_p":0.05, "where_p":0.05}
        if param_prob is not None:
            prob.update(param_prob)
            
        other_tables = other_tables or []

        if max_depth <= 0 or flip(prob["depth_p"]):
            if other_tables and flip(prob["sub_p"]):
                return InSubquery.random(table, other_tables, param_prob=prob) #{"where_p":prob["where_p"]})
            elif flip(prob["pred_p"]):
                return Predicate.random(table)
            else:
                return Predicate.random(table, sub_allow=False) # Index does not accept subqueries
        else:
            left = Where.random(table, max_depth - 1, param_prob=prob)
            right = Where.random(table, max_depth - 1, param_prob=prob)
            op = random.choice(["AND", "OR"])
            return BooleanExpr(left, right, op)

@dataclass
class InSubquery(Where):
    '''
    WHERE col IN (SELECT other_col FROM other_table WHERE ...)
    '''
    column: "Column" 
    subquery: "Select"
    table_name: str

    def sql(self) -> str: 
        return f"{self.table_name}.{self.column.name} IN ({self.subquery.sql()})" #is this missing the WHERE at the beginning of the string?

    @staticmethod
    def random(table: "Table", other_tables: List["Table"],  param_prob: Dict[str, float] = None) -> "InSubquery":
        prob = {"where_p":0.05, }
        if param_prob is not None:
            prob.update(param_prob)
            
        column = random.choice(table.columns)
        other_table = random.choice(other_tables)

        matching_columns = [col for col in other_table.columns if col.dtype == column.dtype]
        if not matching_columns:
            sub_col = random.choice(other_table.columns)
        else:
            sub_col = random.choice(matching_columns)
            
        where_clause = Where.random(other_table, max_depth=1, param_prob=prob) if flip(prob["where_p"]) else None
        subquery = Select(
            columns=[sub_col],
            from_clause=other_table,
            where=where_clause
        )

        return InSubquery(column=column, subquery=subquery, table_name=table.name)

@dataclass
class BooleanExpr(Where):
    '''
    a < b
    '''
    left: Where
    right: Where
    operator: str 

    def sql(self) -> str:
        return f"({self.left.sql()} {self.operator} {self.right.sql()})"

@dataclass
class Column:
    '''
    name dtype primary_key nullable unique check default
    '''
    name: str
    dtype: str
    nullable: bool = True
    primary_key: bool = False
    notnull: bool = False
    unique: bool = False #unique and primary key are likely to cause an error bc of randomly generating same value, integer worst
    check: Optional[Predicate] = None #checking enforces a predicate that is basically never gonna be satisfied, causing crashes
    default: Optional[str] = None
    from_table: str = ""

    def sql(self) -> str:
        col = [self.name, ]
        if self.dtype != "TYPELESS":
            col.append(self.dtype)
        else:
            pass #apparently putting random strings as user defined types is allowed
        
        if self.primary_key:
            col.append("PRIMARY KEY")
        if self.notnull:
            col.append("NOT NULL")
        if self.unique:
            col.append("UNIQUE")
        if self.check:
            col.append(f"CHECK({self.check.sql()})")
        if self.default:
            col.append(f"DEFAULT {self.default}")
        return " ".join(col)

    @staticmethod
    def random(name: Optional[str] = None, param_prob:Dict[str, float] = None) -> "Column":
        prob = {"pk_p":0.0, "unq_p":0.05, "dft_p":0.2, "nnl_p":0.2, "cck_p":0.3, "typeless_p":0.1}
        if param_prob is not None:
            prob.update(param_prob)
        
        if flip(prob["typeless_p"]):
            dtype = "TYPELESS"
        else:
            dtype = random_type()
            
        name = name or random_name("col")
        name = dtype[0].lower() + name
        primary_key = flip(prob["pk_p"]) and dtype != "TYPELESS"
        unique = not primary_key and flip(prob["unq_p"])

        check = None 
        # if flip(prob["cck_p"]):
        #     temp_table = Table(name="fake", columns=[])
        #     fake_col = Column(name=name, dtype=dtype)
        #     temp_table.columns.append(fake_col)
        #     check = Predicate.random(temp_table, rand_tbl=0)

        default = random_value(dtype, null_chance=0) if flip(prob["dft_p"]) and not primary_key and not unique else None
        notnull = default and flip(prob["nnl_p"])
        nullable = not primary_key and not notnull
        
        return Column(name, dtype, nullable, primary_key, notnull, unique, check, default)

@dataclass
class Table(SQLNode):
    '''
    CREATE TABLE name (columns)
    '''
    name: str
    columns: List[Column]
    viewed = False

    def sql(self) -> str:
        column_defs = ", ".join([col.sql() for col in self.columns])
        return f"CREATE TABLE {self.name} ({column_defs})"

    def get_col_names(self) -> List[str]:
        return [col.name for col in self.columns]
    
    @staticmethod
    def random(name: Optional[str] = None, min_cols: int = 2, max_cols: int = 6) -> "Table":
        name = name or random_name("tbl")
        num_cols = random.randint(min_cols, max_cols)

        columns = []
        for i in range(num_cols):
            # primary key only for first column and try at least 1 non unique col
            prob = {"pk_p":0.1 if i==0 else 0, "unq_p": 0 if i==0 or i==1 else 0.05}
            col = Column.random(param_prob=prob)
            columns.append(col)

        return Table(name, columns)
    
@dataclass
class AlterTable(Table):
    '''
    ALTER TABLE name RENAME old_col TO new_col
    ALTER TABLE name ADD COLUMN new_col
    ALTER TABLE old_name RENAME TO name
    '''
    name: str
    table: Table
    old_name: str = ""
    new_col: Optional[Column] = None
    old_col_name: str = ""
    columns: List[Column]
    idx: Optional[int] = 0

    def sql(self) -> str:
        if self.old_col_name:
            return f"ALTER TABLE {self.name} RENAME {self.old_col_name} TO {self.new_col.name}"
        elif self.new_col:
            return f"ALTER TABLE {self.name} ADD COLUMN {self.new_col.sql()}"
        else:
            return f"ALTER TABLE {self.old_name} RENAME TO {self.name}"
        
    @staticmethod
    def random(table: "Table") -> "AlterTable":
        if table.viewed: #modifying tbales breaks views
            return None
        
        fn = random.choice([AlterTable.random_add, AlterTable.random_col_rename, AlterTable.random_tbl_rename])
        return fn(table)
        
    @staticmethod
    def random_add(table: "Table") -> "AlterTable":
        new_col = Column.random(param_prob={"unq_p":0.0})
        modified_cols = copy.deepcopy(table.columns)
        modified_cols.append(new_col)
        return AlterTable(name=table.name, table=table, new_col=new_col, columns=modified_cols)
    
    def confirm_add(self):
        self.table.columns = self.columns
    
    @staticmethod
    def random_col_rename(table: "Table") -> "AlterTable":
        if table.viewed:
            return None
        mod_cols = copy.deepcopy(table.columns)
        idx = random.randrange(len(mod_cols))
        mod_col = mod_cols[idx]
        old_col_name = mod_col.name
        mod_col.name = random_name("col")
        return AlterTable(name=table.name, table=table, new_col=mod_col, old_col_name=old_col_name, columns=mod_cols, idx=idx)
    
    def confirm_rename(self):
        self.table.columns[self.idx] = self.new_col

    
    @staticmethod
    def random_tbl_rename(table: "Table") -> "AlterTable":
        if table.viewed:
            return None
        return AlterTable(name=random_name("atbl"), table=table, old_name=table.name, columns=table.columns)

@dataclass
class Insert(SQLNode):
    table: str
    columns: List[Column]
    values: List[List[str]]
    default: bool
    full: bool
    conflict_action: Optional[str] = None

    def sql(self) -> str:
        query = "INSERT "
        if self.conflict_action:
            query+= f"OR {self.conflict_action} "
            
        cols = ", ".join([c.name for c in self.columns])
        vals_list = []
        for row in self.values:
            vals = ", ".join(row)
            vals_list.append(f"({vals})")
        all_vals = ", ".join(vals_list)
        
        query += f"INTO {self.table} "
        if not self.full:
            query += f"({cols}) "
            
        if self.default:
           query += f"DEFAULT VALUES"
        else:
            query += f"VALUES {all_vals}"
        
        return query

    @staticmethod
    def random(table: "Table", non_unique: bool = False, param_prob:Dict[str, float] = None) -> "Insert":
        prob = {"dft_p":0.2, "full_p":0.2, "conf_p":0.2, "null_p":0.05, "call_p":0.9}
        if param_prob is not None:
            prob.update(param_prob)
        
        candidate_cols = [col for col in table.columns if not non_unique or not (col.unique or col.primary_key)]
        if not candidate_cols and non_unique:
            return None
        
        default = flip(prob["dft_p"]) and all(col.nullable for col in table.columns)
        conflict_action = random.choice(["ROLLBACK", "ABORT", "FAIL", "IGNORE", "REPLACE"]) if flip(prob["conf_p"]) else None
        full = flip(prob["full_p"]) and not non_unique
        
        if default:
           return Insert(table=table.name, columns=[], values=[], conflict_action=conflict_action, default=True, full=True)
        
        cols = []
        num_rows = random.randint(1,5)
        vals = [[] for _ in range(num_rows)]
        
        if full:
            sample_cols = table.columns
        else:
            num_cols = random.randint(1, len(candidate_cols))
            sample_cols = random.sample(candidate_cols, num_cols)

        for col in sample_cols:
            cols.append(col)
            for i in range(num_rows):
                null_chance = 0 if not col.nullable else prob["null_p"]
                callable_chance = 1 if col.unique or col.primary_key else prob["call_p"]
                vals[i].append(random_value(col.dtype, null_chance=null_chance, callable_chance=callable_chance))

        return Insert(table=table.name, columns=cols, values=vals, conflict_action=conflict_action, default=default, full=full)
    
@dataclass
class Update(SQLNode):
    table: str
    columns: List[Column]
    values: List[str]
    where: Optional[Where] = None

    def sql(self) -> str:
        query = f"UPDATE {self.table} SET "
        assignments = []
        for column, value in zip(self.columns, self.values):
            assignments.append(f"{column.name} = {value}")
        query += ", ".join(assignments)
        if self.where:
            query += f" WHERE {self.where.sql()}"
        return query

    @staticmethod
    def random(table: "Table", param_prob:Dict[str, float] = None) -> "Update":
        prob = {"where_p":0.75}
        if param_prob is not None:
            prob.update(param_prob)
            
        cols = []
        vals = []
        
        candidate_cols = [col for col in table.columns if not (col.unique or col.primary_key)]
        if not candidate_cols: #no non-unique columns, update likely to crash
            #return None
            candidate_cols = col
            # where = Where.random(table)
            # vals = [random_value(table.columns[0].dtype,null_chance = 0, callable_chance=1),]
            # cols = [table.columns[0],]
            # return Update(table=table.name, columns=cols, values=vals, where = where)
        
        num_cols = random.randint(1, len(candidate_cols))
        sample_cols = random.sample(candidate_cols, num_cols)
        
        for col in sample_cols: 
            cols.append(col)
            null_chance = 0 if col.notnull else 0.05
            vals.append(random_value(col.dtype, null_chance=null_chance))

        where = Where.random(table, param_prob=prob) if flip(prob["where_p"]) else None
        return Update(table=table.name, columns=cols, values=vals, where=where)
    
@dataclass   
class Delete(SQLNode):
    table: str
    where: Optional[Where] = None

    def sql(self) -> str:
        query = f"DELETE FROM {self.table}"
        if self.where:
            query += f" WHERE {self.where.sql()}"
        return query

    @staticmethod
    def random(table: "Table", param_prob:Dict[str, float] = None) -> "Delete":
        prob = {"where_p":0.95}
        if param_prob is not None:
            prob.update(param_prob)
            
        where = Where.random(table, param_prob=prob) if flip(prob["where_p"]) else None
        return Delete(table=table.name, where=where)
    
@dataclass
class Replace(SQLNode):
    table: str
    columns: List[Column]
    values: List[List[str]]
    default: bool
    full: bool

    def sql(self) -> str:
        query = f"REPLACE INTO {self.table} "
            
        cols = ", ".join([c.name for c in self.columns])
        vals_list = []
        for row in self.values:
            vals = ", ".join(row)
            vals_list.append(f"({vals})")
        all_vals = ", ".join(vals_list)
        
        if not self.full:
            query += f"({cols}) "
            
        if self.default:
           query += f"DEFAULT VALUES"
        else:
            query += f"VALUES {all_vals}"
        
        return query

    @staticmethod
    def random(table: "Table", param_prob:Dict[str, float] = None) -> "Replace":
        prob = {"dft_p":0.2, "full_p":0.2, "null_p":0.05, "call_p":0.9}
        if param_prob is not None:
            prob.update(param_prob)
            
        default = flip(prob["dft_p"]) and all(col.nullable for col in table.columns)
        full = flip(prob["full_p"])
        
        if default:
           return Replace(table=table.name, columns=[], values=[], default=True, full=True)
        
        cols = []
        num_rows = random.randint(1,5)
        vals = [[] for _ in range(num_rows)]
        
        if full:
            sample_cols = table.columns
        else:
            num_cols = random.randint(1, len(table.columns))
            sample_cols = random.sample(table.columns, num_cols)

        for col in sample_cols:
            cols.append(col)
            for i in range(num_rows):
                null_chance = 0 if not col.nullable else prob["null_p"]
                callable_chance = 1 if col.unique or col.primary_key else prob["call_p"]
                vals[i].append(random_value(col.dtype, null_chance=null_chance, callable_chance=callable_chance))

        return Replace(table=table.name, columns=cols, values=vals, default=default, full=full)
    

@dataclass
class Join(SQLNode):
    '''
    table1 INNER/LEFT/CROSS JOIN table2 
    '''
    left_table: Table
    right_table: Table
    left_column: Column
    right_column: Column
    join_type: str
    alias: bool

    def sql(self) -> str:
        if self.alias:
            if self.join_type == "CROSS":
                return f"{self.left_table.name} AS a CROSS JOIN {self.right_table.name} AS b"
            
            return (
                f"{self.left_table.name} AS a {self.join_type} JOIN {self.right_table.name} AS b "
                f"ON a.{self.left_column.name} = b.{self.right_column.name}"
            )
        else :
            if self.join_type == "CROSS":
                return f"{self.left_table.name} CROSS JOIN {self.right_table.name}"
            
            return (
                f"{self.left_table.name} {self.join_type} JOIN {self.right_table.name} "
                f"ON {self.left_table.name}.{self.left_column.name} = {self.right_table.name}.{self.right_column.name}"
            )

    @staticmethod
    def random(left: "Table", right: "Table", join_type: str = None) -> "Join":
        left_cols = [c for c in left.columns if c.dtype in {"INTEGER", "TEXT"}]
        right_cols = [c for c in right.columns if c.dtype in {"INTEGER", "TEXT"}]

        alias = left.name == right.name
            
        if not left_cols or not right_cols:
            left_col = random.choice(left.columns)
            right_col = random.choice(right.columns)
        else:
            left_col = random.choice(left_cols)
            right_col = random.choice(right_cols)

        if not join_type:
            join_type = random.choice(["INNER", "LEFT", "CROSS"])
            
        return Join(
            left_table=left,
            right_table=right,
            left_column=left_col,
            right_column=right_col,
            join_type=join_type,
            alias=alias
        )

@dataclass
class Select(SQLNode):
    columns: List[Column]
    from_clause: Union[Table, Join]
    asterisk: bool = False
    one: bool = False
    omit:bool = False
    date:bool = False
    where: Optional[Where] = None
    group_by: Optional[List[Column]] = None
    order_by: Optional[List[Column]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    select_case: Optional["Case"] = None
    #having: #this one is confusing, its like a group level condition generally after a group by
    table_name: str = ""

    def sql(self) -> str:
        all_select = [f"{self.table_name}.{c.name}" if self.table_name else c.name for c in self.columns] 
        all_select += [] if not self.select_case else [self.select_case.sql()] 
        base = "SELECT "
        if self.asterisk:
            base += "*"
        elif self.one:
            #TODO: random expression "1" + "2", "a" + "1", "abc"
            base += "1" 
            if self.omit:
                return base
        else:
            base += f"{', '.join(all_select)}"

        if self.date:
            if flip():
                base += f", {random.choice(TIME['DATES'])}({random.choice(TIME['TIMES'])})"
            elif flip():
                base += f", {random.choice(TIME['DATES'])}({random.choice(TIME['TIMES'])}, {random.choice(TIME['TIME_MODS'])})"
            else:
                base += f", strftime({random.choice(TIME['TIME_FORMATS'])}, {random.choice(TIME['TIMES'])}, {random.choice(TIME['TIME_MODS'])})"
        
        base += f" FROM {self.from_clause.sql() if isinstance(self.from_clause, Join) else self.from_clause.name}"
        
        if self.where:
            base += f" WHERE {self.where.sql()}"
        if self.group_by:
            group_names = [c.name for c in self.group_by]
            base += f" GROUP BY {', '.join(group_names)}"
        if self.order_by:
            order_names = [c.name for c in self.order_by]
            base += f" ORDER BY {', '.join(order_names)}"
        if self.limit:
            base += f" LIMIT {self.limit}"
            if self.offset:
                base+= f" OFFSET {self.offset}"
                
        return base

    @staticmethod
    def random(table: Table, sample: int = None, other_tables: list[Table] = None, param_cols: List[Column] = None, param_prob:Dict[str, float] = None) -> "Select":
        prob = {"where_p":0.9, "grp_p":0.3, "ord_p":0.3, "join_p":0.3, "lmt_p":0.2, "case_p":0.2, "offst_p":0.5, "*_p":0.2, "omit_p":0.2, "one_p":0.05, "date_p":0.1}
        if param_prob is not None:
            prob.update(param_prob)
        
        asterisk = flip(prob["*_p"]) and not sample
        date = flip(prob["date_p"])
        one = flip(prob["one_p"]) and not (sample or asterisk or param_cols)
        omit = one and flip(prob["omit_p"])
        
        if param_cols:
            cols = param_cols
        else:
            cols = table.columns
            
        if asterisk:
            selected_cols = cols
        elif not sample:
            selected_cols = random.sample(cols, random.randint(1, len(cols)))
        else:
            selected_cols = random.sample(cols, sample)

        select_case = Case.random(table, random.choice(selected_cols), param_prob=prob) if flip(prob["case_p"]) else None
        where = Where.random(table, param_prob=prob, other_tables=other_tables) if flip(prob["where_p"]) else None
        group_by = random.sample(selected_cols, k=1) if flip(prob["grp_p"]) else None
        order_by = random.sample(selected_cols, k=1) if flip(prob["ord_p"]) else None
        limit = random.randint(1,20) if flip(prob["lmt_p"]) else None
        offset = random.randint(1,20) if flip(prob["offst_p"]) and limit else None
        
        if other_tables and flip(prob["join_p"]):
            left = table
            right = random.choice(other_tables)
            from_clause = Join.random(left, right)
            if left.name == right.name:
                asterisk = True
                group_by = None
                order_by = None
                select_case = None
                where = None
            
        else:
            from_clause = table

        return Select(
            columns=selected_cols,
            asterisk=asterisk,
            one=one,
            omit=omit,
            date=date,
            from_clause=from_clause,
            where=where,
            group_by=group_by,
            order_by=order_by,
            limit=limit,
            offset=offset,
            select_case=select_case,
            table_name=table.name
        )
        
@dataclass
class With(SQLNode):
    '''
    WITH name AS (
        SELECT ... FROM ... WHERE ...
    ) 
    SELECT ... FROM ... WHERE ...
    '''
    names: List[str]
    querys: List[Select]
    main_query: SQLNode
    recursive: bool = False #TODO: recursive

    def sql(self) -> str:
        full_query = f"WITH {'RECURSIVE' if self.recursive else ''} "
        
        temp = []
        for i in range(len(self.names)):
            temp.append(f"{self.names[i]} AS ({self.querys[i].sql()})")
        full_query += ", ".join(temp)
        if self.main_query:
            return f"{full_query} {self.main_query.sql()}"
        return full_query
    
    @staticmethod
    def random(table: Table, param_prob: Dict[str,float] = None) -> "With":
        prob = {"rec_p":0.5, "one_p":0, "*_p":1, "select_p":0.95, }
        if param_prob is not None:
            prob.update(param_prob)
        
        recursive = flip(prob["rec_p"])
        num = random.randint(1,3)
        with_names = [random_name("with") for _ in range(num)]
        with_tables = [Table(with_name, table.columns) for with_name in with_names]
        inner_selects = [Select.random(random.choice([table] + with_tables[:i]), param_prob=prob) for i in range(num)]
        
        idx = random.randint(0, num-1)
        cols = inner_selects[idx].columns
        w_table = with_tables[idx]
        if flip(prob["select_p"]) or isinstance(table, View):
            main_query = Select.random(w_table, param_cols=cols, param_prob=prob)
        else:
            fn = random.choice([Delete.random, Insert.random, Replace.random, Update.random])
            main_query = fn(table, param_prob=prob)#using the w_table inside these is pretty complicated, so the with here is useless but correct syntax
        
        return With(names=with_names, querys=inner_selects, main_query=main_query, recursive=recursive)

@dataclass
class View(Table):
    '''
    CREATE VIEW name AS SELECT ... 
    '''
    name: str
    select: Select
    columns: List[Column]
    temp: bool

    def sql(self) -> str:
        return f"CREATE {'TEMP' if self.temp else ''} VIEW {self.name} AS {self.select.sql()}"
    
    @staticmethod
    def random(table: Table, other_tables: List[Table] = None, param_prob: Dict[str,float] = None) -> "View":
        prob = {"tmp_p":0.1, "one_p":0}
        if param_prob is not None:
            prob.update(param_prob)
            
        temp = flip(prob["tmp_p"])
        view_name = random_name("view")
        select = Select.random(table, other_tables=other_tables, param_prob=prob)
        
        return View(name=view_name, columns=select.columns, select=select, temp=temp)

@dataclass
class VirtualTable(Table):
    name: str
    columns: List[Column]
    vtype: str

    def sql(self) -> str:
        if self.vtype == "dbstat":
            return f"CREATE VIRTUAL TABLE {self.name} USING {self.vtype}"
        else:
            return f"CREATE VIRTUAL TABLE {self.name} USING {self.vtype}({', '.join([c.name for c in self.columns])})"
    
    '''
    TODO: NO ALTER TABLE for VIRTUAL TABLES
    fts4 supports: no foreign keys, otherwise everything is supported
    rtree supports: no foreign keys, otherwise everything is supported
    dbstat supports: read_only (no insert, update, delete)
    '''
    def random(param_prob: Dict[str,float] = None) -> "VirtualTable":
        vtype = random.choice(VIRTUAL["types"])
        col_names = VIRTUAL[vtype]
        columns = []
        for c in col_names:
            keys = list(c)
            if len(keys) == 1:
                columns.append(Column(name=keys[0], dtype=c[keys[0]]))
            else:
                columns.append(Column(name=keys[0], dtype=c[keys[0]], primary_key=True))
        return VirtualTable(name=random_name(vtype), columns=columns, vtype=vtype)


@dataclass
class Index(SQLNode):
    '''
    CREATE (UNIQUE) INDEX name ON table (idx_columns) WHERE ...
    '''
    name: str
    table: str
    columns: List[str]
    unique: bool
    where: Optional[Where] = None

    def sql(self) -> str:
        query = ["CREATE"]
        if self.unique:
            query.append("UNIQUE")
        query.append(f"INDEX {self.name} ON {self.table} ({', '.join(self.columns)})")
        if self.where:
            query.append(f"WHERE {self.where.sql()}")
        return " ".join(query)

    @staticmethod
    def random(table: "Table", param_prob:Dict[str, float] = None) -> "Index":
        if isinstance(table, View):
            return None # cannot index on View
        
        prob = {"uniq_p":0.01, "where_p": 0.4, "pred_p":0.0, "sub_p":0.0}
        if param_prob is not None:
            prob.update(param_prob)
            
        name = random_name("idx")
        col_names = table.get_col_names()
        num_cols = random.randint(1, min(len(col_names), 3))
        cols = random.sample(col_names, num_cols)
        unique = flip(prob["uniq_p"])
        where = Where.random(table, param_prob=prob) if flip(prob["where_p"]) else None
        return Index(name=name, table=table.name, columns=cols, unique=unique, where=where)
    
@dataclass
class Trigger(SQLNode):
    '''
    CREATE TRIGGER name BEFORE/AFTER INSERT/UPDATE/DELETE ON table
    BEGIN
        INSERT/UPDATE/DELETE ...
    END
    '''
    name: str
    temp: bool
    nexists: bool
    timing: str
    event: str
    table: Table
    foreach: bool
    statements: List[str] 
    cols: Optional[List[str]] = None
    when: Optional[Where] = None #when inside a trigger needs to reference columns with NEW and OLD

    def sql(self) -> str:
        base = "CREATE "
        if self.temp:
            base += "TEMP "
        base += "TRIGGER "
        if self.nexists:
            base+= "IF NOT EXISTS "
        base += f"{self.name} {self.timing} {self.event} " 
        if self.cols:
           base += f"OF {', '.join(self.cols)} " 
        base += f"ON {self.table.name} "
        if self.foreach:
            base+= "FOR EACH ROW "
        if self.when:
            base += f"WHEN {self.when.sql()}"
            
        body = " ".join(self.statements)
        return f"{base} BEGIN {body} END"

    @staticmethod
    def random(table: "Table", param_prob:Dict[str, float] = None) -> "Trigger":
        prob = {"temp_p":0.2, "nex_p":0.2, "upcol_p":0.2, "where_p": 0.0, "feac_p":0.2, "dft_p":0, "conf_p":0.9}
        if param_prob is not None:
            prob.update(param_prob)
            
        name = random_name("trg")
        temp = flip(prob["temp_p"])
        nexists = flip(prob["nex_p"])
        timing = random.choice(["BEFORE", "AFTER"])
        event = random.choice(["INSERT", "UPDATE", "DELETE"])
        cols = None
        if event == "UPDATE" and flip(prob["upcol_p"]):
            num_cols = random.randint(1, len(table.columns))
            sample_cols = random.sample(table.columns, num_cols)
            cols = [col.name for col in sample_cols]
        when = Where.random(table, param_prob=prob) if flip(prob["where_p"]) else None
        foreach = flip(prob["feac_p"])

        statements = []
        for _ in range(random.randint(1, 3)):
            if flip(0.25):
                statements.append(Select.random(table, param_prob=prob).sql()+";")
                continue
            insert = Insert.random(table, non_unique=True, param_prob=prob) #param_prob={"dft_p":0, "conf_p":0.9})
            if not insert or flip(0.33):
                statements.append(Delete.random(table, param_prob=prob).sql()+";")
                continue
            else:
                update = Update.random(table, param_prob=prob)
                if not update or flip(0.5):
                    statements.append(insert.sql()+";") 
                else:
                    statements.append(update.sql()+";")

        return Trigger(name=name, temp=temp, nexists=nexists, timing=timing, event=event, cols=cols, table=table, foreach=foreach, when=when, statements=statements)

@dataclass
class Pragma(SQLNode):
    '''
    PRAGMA ...
    basically SQLite configuration
    '''
    name: str
    value: str

    def sql(self) -> str:
        return f"PRAGMA {self.name} = {self.value}"

    @staticmethod
    def random() -> "Pragma":
        pragmas = [
            ("foreign_keys", random.choice(["ON", "OFF"])),
            ("cache_size", str(random.randint(1000, 5000))),
            ("journal_mode", random.choice(["DELETE", "TRUNCATE", "PERSIST", "WAL", "MEMORY"])),
            ("synchronous", random.choice(["0", "1", "2"])),  # OFF, NORMAL, FULL
            ("temp_store", random.choice(["DEFAULT", "FILE", "MEMORY"])),
            ("locking_mode", random.choice(["NORMAL", "EXCLUSIVE"])),
        ]

        name, value = random.choice(pragmas)
        return Pragma(name=name, value=value)

@dataclass
class Expression(SQLNode):
    """
    Expression is anything that computes a value, including:
    Column - col
    Literal - 'USA', 42, 3.14, NULL	Constant values
    Math/Logic - col * 1.1, age + 5, x > 10
    String Ops - LOWER(name), SUBSTR(city, 1, 3)	
    CASE - CASE WHEN x > 0 THEN 'Yes' ...
    Aggregate Funcs	- SUM(sales), COUNT(*) - Only in SELECT, HAVING, etc.
    Cast/Conversion - CAST(age AS TEXT)	
    Nested SELECT - (SELECT MAX(age) FROM users) - Scalar subquery — returns 1 value
    
    Put it inside select, where and update
    """
    def sql(self) -> str:
        return NotImplementedError
    
    @staticmethod
    def random():
        return
    
@dataclass
class Case(SQLNode):
    """
    A Expression, cant be used alone
    Put it inside select, where, order by, and update
    """
    conditions:List[str]
    values:List[str]
    col:str
    else_:str
    dtype: str
    
    def sql(self) -> str:
        query = f"CASE "
        if self.col:
            query += f"{self.col} "
        
        for conditon, value in zip(self.conditions, self.values):
            query += f"WHEN {conditon} THEN {value} "
            
        if self.else_:
            query += f"ELSE {self.else_} "
            
        query += "END"    
        return query

    @staticmethod
    def random(table: "Table", column:Optional["Column"]=None, case_dtype:Optional[str]=None, param_prob: Dict[str, float] = None) -> "Case":
        prob = {"case_col" : 0.5}
        if param_prob is not None:
            prob.update(param_prob)
            
        if column:
            col = f"{table.name}.{column.name}"
        else:
            column = random.choice(table.columns)
            col = "" if flip(prob["case_col"]) else f"{table.name}.{column.name}"
        col_dtype = column.dtype
        
        if not case_dtype:
            case_dtype = random_type()
            
        num_cases = random.randint(1,5)
        conditions = []
        values = []
        for _ in range(num_cases):
            if col:
                conditions.append(random_value(col_dtype))
            else:
                conditions.append(Comparison.random(table, param_prob=prob))
                
            values.append(random_value(case_dtype))
        
        else_ = "" if flip() else random_value(case_dtype)
        return Case(conditions, values, col, else_, col_dtype)
    

def randomQueryGen(param_prob: Dict[str, float] = None, debug: bool = False, cycle: int = 3, context: Table = None) -> str:
    """
    Randomly generates the entire query, keeping track of the tables to pass as arguments.
    
    Args:
        prob (Dict[str, float]): Dictionary with probabilities of generating each query type
        debug (bool, optional): helps debugging. Defaults to False.
        cycle (int, optional): number of iterations. Defaults to 3.
        context (Table): table context to add to query. Defaults to None

    Returns:
        str: the string of the entire query generated
        
    """
    prob = {   
        "table":   0.2,
        "insert":  0.2,
        "update":  0.2,
        "replace": 0.2,
        "delete":  0.2,
        "alt_ren": 0.2, 
        "alt_add": 0.2,
        "alt_col": 0.2,
        "select1": 0.2,
        "select2": 0.2,
        "with":    0.2,
        "view":    0.2,
        "index":   0.2,
        "trigger": 0.2,
        "pragma":  0.2,
    }
    if param_prob is not None:
        prob.update(param_prob)
    
    query = ""
    if not context:
        tables = [Table.random()]
        query = tables[0].sql() + ";\n"
        for i in range(1):
            insert = Insert.random(tables[0])
            query += insert.sql() + ";\n"
    views = []
    for i in range(cycle):
        try:
            if flip(prob["table"]) or debug:
                new_table = Table.random()
                tables.append(new_table)
                query+= new_table.sql() + ";\n"
                for i in range(1):
                    insert = Insert.random(new_table)
                    query += insert.sql() + ";\n"
                    
            table = random.choice(tables)
                
            if flip(prob["insert"]) or debug:
                insert = Insert.random(table)
                query += insert.sql() + ";\n"
            if flip(prob["replace"]) or debug:
                replace = Replace.random(table)
                query += replace.sql() + ";\n"
            if flip(prob["update"]) or debug:
                update = Update.random(table)
                if update:
                    query += update.sql() + ";\n"
            if flip(prob["delete"]) or debug:
                delete = Delete.random(table)
                query += delete.sql() + ";\n"
                
            if flip(prob["alt_ren"]) or debug and not table.viewed:
                new_table = AlterTable.random_tbl_rename(table)
                if new_table:
                    tables.remove(table) # renamed name of table, table does not exist anymore
                    tables.append(new_table)
                    query += new_table.sql() + ";\n"
                    table = random.choice(tables)
            if flip(prob["alt_add"]) or debug and not table.viewed:
                new_table = AlterTable.random_add(table)
                if new_table:
                    new_table.confirm_add()
                    query += new_table.sql() + ";\n"
            if flip(prob["alt_col"]) or debug and not table.viewed:
                new_table = AlterTable.random_col_rename(table)
                if new_table:
                    new_table.confirm_rename()
                    query += new_table.sql() + ";\n"
            
            if flip(prob["view"]) or debug:
                table.viewed = True #flags table so that we dont modify it
                view = View.random(table)
                views.append(view)
                query += view.sql() + ";\n"
            if flip(prob["index"]) or debug:
                index = Index.random(table)
                if index:
                    query += index.sql() + ";\n"
            if flip(prob["trigger"]) or debug:
                trigger = Trigger.random(table)
                query += trigger.sql() + ";\n"
            
            table = random.choice(tables + views)
            if flip(prob["with"]) or debug:
                with_ = With.random(table)
                query += with_.sql() + ";\n"
            
            if flip(prob["select1"]) or debug:
                query += Select.random(table).sql() + ";\n"
            if (flip(prob["select2"]) and len(tables) > 1) or debug:
                query += Select.random(table, other_tables=tables).sql() + ";\n"
                
            if flip(prob["pragma"]) or debug:
                query += Pragma.random().sql() + ";\n"
        
        except Exception as e:
            print(e)
            i-=1

    return query, tables, views
        
# if __name__ == "__main__":
#     print(randomQueryGen(prob, debug=False, cycle=1))

